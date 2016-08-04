#
# Copyright 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import rados
from hashlib import md5
import subprocess
import os
import msgpack
import json
import logging
import utils

log = logging.getLogger('calamari.remote.mon')

SRC_DIR = "/etc/ceph"
SOCKET_DIR = "/var/run/ceph"
LOG_DIR = None

if SRC_DIR and not SOCKET_DIR:
    SOCKET_DIR = os.path.join(SRC_DIR, "out")

if SRC_DIR and not LOG_DIR:
    LOG_DIR = os.path.join(SRC_DIR, "out")

SOCKET_PREFIX = "{cluster_name}-"

RADOS_TIMEOUT = 20
RADOS_NAME = 'client.admin'
EVENT_TAG_PREFIX = 'calamari/'

# Severity codes for Calamari events
CRITICAL = 1
ERROR = 2
WARNING = 3
RECOVERY = 4
INFO = 5

SEVERITIES = {
    CRITICAL: "CRITICAL",
    ERROR: "ERROR",
    WARNING: "WARNING",
    RECOVERY: "RECOVERY",
    INFO: "INFO"
}


class Unavailable(Exception):
    pass


class ClusterHandle():

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name

    def __enter__(self):
        if SRC_DIR:
            conf_file = os.path.join(SRC_DIR, self.cluster_name + ".conf")
        else:
            conf_file = ''

        log.debug('rados_connect getting handle for: %s' % str(conf_file))

        self.cluster_handle = rados.Rados(
            name=RADOS_NAME,
            clustername=self.cluster_name,
            conffile=conf_file)
        self.cluster_handle.connect(timeout=RADOS_TIMEOUT)

        return self.cluster_handle

    def __exit__(self, *args):
        self.cluster_handle.shutdown()


def rbd_listing(cluster_handle, clustername='ceph'):
    """
    For each pool list the rbd images
    return a mapping of pool name to rbd images
    """
    listing = {}
    pools = rados_command(cluster_handle, "osd lspools")
    for pool in pools:
        name = pool['poolname']
        if clustername == 'ceph':
            args = ['ls', '-l', '--format', 'json']
        else:
            args = ['ls', '-l', '--cluster', clustername, '--format', 'json']

        result = rbd_command(args, name)
        if result['status'] == 0:
            listing[name] = json.loads(result['out'])
        else:
            listing[name] = {}

    return listing


def rados_command(cluster_handle, prefix, args=None, decode=True):
    """
    Safer wrapper for ceph_argparse.json_command, which raises
    Error exception instead of relying on caller to check return
    codes.

    Error exception can result from:
    * Timeout
    * Actual legitimate errors
    * Malformed JSON output

    return: Decoded object from ceph, or None if empty string returned.
            If decode is False, return a string (the data returned by
            ceph command)
    """
    from ceph_argparse import json_command
    if args is None:
        args = {}

    argdict = args.copy()
    argdict['format'] = 'json'

    ret, outbuf, outs = json_command(cluster_handle,
                                     prefix=prefix,
                                     argdict=argdict,
                                     timeout=RADOS_TIMEOUT)
    if ret != 0:
        raise rados.Error(outs)
    else:
        if decode:
            if outbuf:
                try:
                    return json.loads(outbuf)
                except (ValueError, TypeError):
                    raise rados.Error("Invalid JSON output for command {0}".format(argdict))
            else:
                return None
        else:
            return outbuf


def rbd_command(command_args, pool_name=None):
    """
    Run a rbd CLI operation directly.  This is a fallback to allow
    manual execution of arbitrary commands in case the user wants to
    do something that is absent or broken in Calamari proper.

    :param pool_name: Ceph pool name, or None to run without --pool argument
    :param command_args: Command line, excluding the leading 'rbd' part.
    """

    if pool_name:
        args = ["rbd", "--pool", pool_name] + command_args
    else:
        args = ["rbd"] + command_args

    log.info('rbd_command {0}'.format(str(args)))

    rc, out, err = utils.execCmd(args)

    log.info('rbd_command {0} {1} {2}'.format(str(rc), out, err))

    return {
        'out': out,
        'err': err,
        'status': rc
    }


def _emit_to_salt_bus(severity, message, tag, **tags):
    """
    This function emits events to salt event bus, if the config
    value "emit_events_to_salt_event_bus" is set to true.
    """
    res = {}
    res["message"] = message
    res["severity"] = severity
    res["tags"] = tags
    tag = EVENT_TAG_PREFIX + tag

    log.debug("Eventer._emit_to_salt_bus: Tag:%s | Data: %s" % (str(tag), str(res)))

    __salt__['event.send'](tag, res)  # noqa


def rbd_eventer(cluster_name='ceph'):
    try:
        old = _get_data()
    except:
        log.error('clearing data from get')
        __salt__['data.clear']
        old = (None, {})
    fsid = _set_data(cluster_name)
    new = _get_data()
    _on_rbd_listing(fsid, new, old)


def _set_data(cluster_name='ceph'):
    from ceph_argparse import json_command

    # Open a RADOS session
    with ClusterHandle(cluster_name) as cluster_handle:
        ret, outbuf, outs = json_command(cluster_handle,
                                         prefix='status',
                                         argdict={'format': 'json'},
                                         timeout=RADOS_TIMEOUT)
        status = json.loads(outbuf)
        fsid = status['fsid']

        data = rbd_listing(cluster_handle, cluster_name)
        version = md5(msgpack.packb(data)).hexdigest()
        __salt__['data.update']('rbd_list', (version,data))

    return fsid


def _get_data():
    return __salt__['data.getval']('rbd_list')  # noqa


def _on_rbd_listing(fsid, new, old):
    if old is None:
        old = (None, {})
    old_version, old_data = old
    new_version, new_data = new
    if old_version == new_version:
        return

    def _transform(listing, key):
        # We know that we're only dealing with a mapping of pool name to list of rbdimages
        # where an rbdimage is a dict. containing attributes of rbd ls -l i.e. format, image, size
        # returns a set of tuples (pool_name, image, size)
        rbds = []
        for k, v in listing.iteritems():
            for rbd in v:
                if key == 'size':
                    rbds.append((k, rbd['image'], rbd[key]))
                else:
                    rbds.append((k, rbd[key]))
        return set(rbds)

    def _rbd_event(severity, msg, tag, rbdName, poolName):
        log.error('_rbd_event' + msg)
        _emit_to_salt_bus(
            SEVERITIES[severity],
            msg,
            tag,
            fsid=fsid,
            fqdn=None,
            rbdName=rbdName,
            poolName=poolName
        )

    old_rbds = _transform(old_data, 'image')
    new_rbds = _transform(new_data, 'image')
    deleted_rbds = old_rbds - new_rbds
    created_rbds = new_rbds - old_rbds
    resized_rbds = _transform(new_data, 'size') - _transform(old_data, 'size')

    for r in deleted_rbds:
        _rbd_event(INFO, "RBD {0} was removed from pool {1}".format(r[1], r[0]), 'ceph/rbd/deleted', r[1], r[0])

    for r in created_rbds:
        _rbd_event(INFO, "RBD {0} was added to pool {1}".format(r[1], r[0]), 'ceph/rbd/created', r[1], r[0])

    for r in resized_rbds:
        if (r[0], r[1]) not in created_rbds:
            _rbd_event(INFO, "RBD {0} in pool {1} was resized to {2}".format(r[1], r[0], r[2]), 'ceph/rbd/resized', r[1], r[0])
