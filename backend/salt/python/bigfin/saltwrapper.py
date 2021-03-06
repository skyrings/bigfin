# Copyright 2015 Red Hat, Inc.
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

from netaddr import IPNetwork, IPAddress
import os
import json
import logging
from functools import wraps
import ConfigParser
import string
import ast
import time
import uuid

import salt
import salt.client
import salt.config

import utils


log = logging.getLogger(__name__).root
opts = salt.config.master_config('/etc/salt/master')

_CEPH_CLUSTER_CONF_DIR = '/srv/salt/skyring/conf/ceph'
_MON_ID_LIST = list(string.ascii_lowercase)
_DEFAULT_MON_PORT = 6789

_ceph_authtool = utils.CommandPath("ceph-authtool",
                                   "/usr/bin/ceph-authtool",)
_monmaptool = utils.CommandPath("monmaptool",
                                "/usr/bin/monmaptool",)


def enableLogger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        log.info('args=%s, kwargs=%s' % (args, kwargs))
        rv = func(*args, **kwargs)
        log.info('rv=%s' % rv)
        return rv
    return wrapper


setattr(salt.client.LocalClient, 'cmd',
        enableLogger(salt.client.LocalClient.cmd))


def _get_short_hostname(hostname):
    return hostname.split('.')[0]


def _get_state_result(out):
    failed_minions = {}
    for minion, v in out.iteritems():
        failed_results = {}
        for id, res in v.iteritems():
            if not res['result']:
                failed_results.update({id: res})
        if not v:
            failed_minions[minion] = {}
        if failed_results:
            failed_minions[minion] = failed_results

    return failed_minions


def run_state(local, tgt, state, *args, **kwargs):
    out = local.cmd(tgt, 'state.sls', [state], *args, **kwargs)
    return _get_state_result(out)


def pull_minion_file(local, minion, minion_path, path):
    out = local.cmd(minion, 'file.grep', [minion_path, '.'])

    result = out.get(minion, {})
    if result and result['retcode'] == 0:
        with open(path, 'wb') as f:
            f.write(result['stdout'])
            f.write('\n')
        return True
    else:
        return False


def _get_minion_network_info(minions):
    local = salt.client.LocalClient()
    out = local.cmd(minions, ['grains.item', 'network.subnets'],
                    [['ipv4', 'ipv6'], []], expr_form='list')
    netinfo = {}
    for minion in minions:
        info = out.get(minion)
        if info:
            netinfo[minion] = {'ipv4': info['grains.item']['ipv4'],
                               'ipv6': info['grains.item']['ipv6'],
                               'subnet': info['network.subnets']}
        else:
            netinfo[minion] = {'ipv4': [], 'ipv6': [], 'subnet': []}

    return netinfo


def _check_minion_networks(minions, public_network=None, cluster_network=None,
                           check_cluster_network=False):
    '''
    :: minions = {MINION_ID: {'public_ip': IP_ADDRESS,
                              'cluster_ip': IP_ADDRESS}, ...}
    '''

    def _get_ip_network(ip, subnets):
        for subnet in subnets:
            network = IPNetwork(subnet)
            if ip in network:
                return network

    def _check_ip_network(minion, ip, ip_list, network, subnets,
                          label='public'):
        if ip not in ip_list:
            raise ValueError('%s ip %s not found in minion %s' %
                             (label, ip, minion))
        ip = IPAddress(ip)
        if not network:
            network = _get_ip_network(ip, subnets)
        if network and ip not in network:
            raise ValueError('minion %s %s ip %s not in network %s' %
                             (m, ip, label, network))
        return network

    netinfo = _get_minion_network_info(minions)
    for m, v in minions.iteritems():
        public_network = _check_ip_network(m, v['public_ip'],
                                           netinfo[m]['ipv4'],
                                           public_network,
                                           netinfo[m]['subnet'])
        if not check_cluster_network:
            continue
        cluster_network = _check_ip_network(m, v['cluster_ip'],
                                            netinfo[m]['ipv4'],
                                            cluster_network,
                                            netinfo[m]['subnet'])

    return public_network, cluster_network


def sync_ceph_conf(cluster_name, minions):
    local = salt.client.LocalClient()
    out = local.cmd(minions,
                    'state.single',
                    ['file.managed', '/etc/ceph/%s.conf' % cluster_name,
                     'source=salt://skyring/conf/ceph/%s/%s.conf' % (
                         cluster_name, cluster_name),
                     'show_diff=False'], expr_form='list')
    return _get_state_result(out)


def _config_add_monitors(config, monitors):
    for m, v in monitors.iteritems():
        section = 'mon.' + m
        config.add_section(section)
        config.set(section, 'host', v['name'])
        config.set(section, 'mon addr',
                   '%s:%s' % (v['address'], v.get('port', _DEFAULT_MON_PORT)))


def _gen_ceph_cluster_conf(conf_file, cluster_name, fsid, monitors,
                           public_network,
                           osd_journal_size=1024,
                           osd_pool_default_size=2,
                           osd_pool_default_min_size=1,
                           osd_pool_default_pg_num=128,
                           osd_pool_default_pgp_num=128,
                           osd_crush_chooseleaf_type=1):
    '''
    :: monitors = {ID: {'name': SHORT_HOSTNAME, 'address': IP_ADDR,
                        'port': INT}, ...}
    '''
    config = ConfigParser.RawConfigParser()

    config.add_section('global')
    config.set('global', 'fsid', fsid)
    config.set('global', 'public network', public_network)
    config.set('global', 'auth cluster required', 'cephx')
    config.set('global', 'auth service required', 'cephx')
    config.set('global', 'auth client required', 'cephx')
    config.set('global', 'osd journal size', osd_journal_size)
    config.set('global', 'filestore xattr use omap', 'true')
    config.set('global', 'osd pool default size', osd_pool_default_size)
    config.set('global', 'osd pool default min size',
               osd_pool_default_min_size)
    config.set('global', 'osd pool default pg num', osd_pool_default_pg_num)
    config.set('global', 'osd pool default pgp num', osd_pool_default_pgp_num)
    config.set('global', 'osd crush chooseleaf type',
               osd_crush_chooseleaf_type)

    config.add_section('mon')
    config.set('mon', 'mon initial members', ', '.join(monitors))
    _config_add_monitors(config, monitors)

    with open(conf_file, 'wb') as f:
        config.write(f)
    return True


def _gen_keys(cluster_name, fsid, monitors, cluster_dir):
    '''
    :: monitors = {ID: {'name': SHORT_HOSTNAME, 'address': IP_ADDR,
                        'port': INT}, ...}
    '''
    mon_key_path = cluster_dir + '/mon.key'
    admin_key_path = cluster_dir + '/client.admin.keyring'
    mon_map_path = cluster_dir + '/mon.map'

    utils.execCmd([_ceph_authtool.cmd, '--create-keyring', mon_key_path,
                   '--gen-key', '-n', 'mon.', '--cap', 'mon', 'allow *'])

    utils.execCmd([_ceph_authtool.cmd, '--create-keyring', admin_key_path,
                   '--gen-key', '-n', 'client.admin', '--set-uid=0', '--cap',
                   'mon', 'allow *', '--cap', 'osd', 'allow *', '--cap',
                   'mds', 'allow'])

    utils.execCmd([_ceph_authtool.cmd, mon_key_path, '--import-keyring',
                   admin_key_path])

    cmd = [_monmaptool.cmd, '--create', '--clobber']
    for m, v in monitors.iteritems():
        cmd += ['--add', 'mon.' + m, v['address']]
    cmd += ['--fsid', fsid, mon_map_path]
    utils.execCmd(cmd)

    return True


def _get_mon_id_map(unused_mon_ids, minions):
    mon_id_map = dict(zip(unused_mon_ids, minions))
    monitors = {}
    for id, minion in mon_id_map.iteritems():
        monitors[id] = {'name': _get_short_hostname(minion),
                        'address': minions[minion]['public_ip']}
    return mon_id_map, monitors


def _add_ceph_mon_pillar_data(mon_id_map, cluster_name, monitors):
    pillar_data = {}
    for id, minion in mon_id_map.iteritems():
        pillar_data[minion] = {'cluster_name': cluster_name, 'mon_id': id,
                               'mon_name': monitors[id]['name'],
                               'public_ip': monitors[id]['address']}
    return pillar_data


def CreateCluster(cluster_name, fsid, minions, ctxt=""):
    # convert list of minions to below dict
    # {MINION_ID: {'public_ip': IP_ADDRESS,
    #              'cluster_ip': IP_ADDRESS}, ...}
    d = {}
    for m in minions:
        d.update({m['Node']: {'public_ip': m['PublicIP4'],
                              'cluster_ip': m['ClusterIP4']}})
    minion_set = minions
    minions = d

    public_network, cluster_network = _check_minion_networks(minions)
    mon_id_map, monitors = _get_mon_id_map(_MON_ID_LIST, minions)

    cluster_dir = _CEPH_CLUSTER_CONF_DIR + '/' + cluster_name
    if not os.path.exists(cluster_dir):
        os.makedirs(cluster_dir)

    conf_file = cluster_dir + '/' + cluster_name + '.conf'
    _gen_ceph_cluster_conf(conf_file, cluster_name, fsid, monitors,
                           public_network)

    _gen_keys(cluster_name, fsid, monitors, cluster_dir)

    pillar_data = _add_ceph_mon_pillar_data(mon_id_map, cluster_name, monitors)
    pillar = {'skyring': pillar_data}

    bootstrapped_minion = None
    local = salt.client.LocalClient()
    for id, minion in mon_id_map.iteritems():
        out = run_state(local, minion, 'add_ceph_mon',
                        kwarg={'pillar':
                               {'skyring': {'mon_bootstrap': True,
                                            minion: pillar_data[minion]}}})
        if out:
            log.error("%s-mon_bootstrap failed. %s" % (ctxt, out))
        else:
            bootstrapped_minion = minion
            break

    if not bootstrapped_minion:
        log.error("%s-mon_bootstrap failed" % ctxt)
        raise Exception("mon_bootstrap failed")

    cluster_key_file = cluster_name + '.keyring'
    bootstrap_osd_key_file = '/var/lib/ceph/bootstrap-osd/' + cluster_key_file
    cluster_key_path = cluster_dir + '/' + cluster_key_file

    if not pull_minion_file(local, bootstrapped_minion, bootstrap_osd_key_file,
                            cluster_key_path):
        log.error("%s-failed to pull %s file from %s" %
                  (ctxt, bootstrap_osd_key_file, bootstrapped_minion))
        raise Exception("failed to pull %s file from %s" %
                        (bootstrap_osd_key_file, bootstrapped_minion))

    minion_set = set(minions)
    minion_set.remove(bootstrapped_minion)
    if minion_set:
        rv = run_state(local, minion_set, 'add_ceph_mon', expr_form='list',
                       kwarg={'pillar': pillar})
        if rv:
            log.error('%s-add_mon failed for %s. error=%s' %
                      (ctxt, minion_set, rv))
            raise Exception('add_mon failed for %s. error=%s' %
                            (minion_set, rv))
    return True


def AddMon(cluster_name, minions, ctxt=""):
    # convert list of minions to below dict
    # {MINION_ID: {'public_ip': IP_ADDRESS,
    #              'cluster_ip': IP_ADDRESS}, ...}
    d = {}
    for m in minions:
        d.update({m['Node']: {'public_ip': m['PublicIP4'],
                              'cluster_ip': m['ClusterIP4']}})
    minion_set = minions
    minions = d

    conf_file = (_CEPH_CLUSTER_CONF_DIR + '/' + cluster_name + '/' +
                 cluster_name + '.conf')
    config = ConfigParser.RawConfigParser()
    config.read(conf_file)

    public_network = IPNetwork(config.get('global', 'public network'))
    _check_minion_networks(minions, public_network)

    used_mon_ids = set([id.strip() for id in config.get(
        'mon', 'mon initial members').split(',')])
    unused_mon_ids = list(set(_MON_ID_LIST) - used_mon_ids)
    unused_mon_ids.sort()

    mon_id_map, monitors = _get_mon_id_map(unused_mon_ids, minions)

    mon_initial_members = list(used_mon_ids) + list(monitors)
    mon_initial_members.sort()
    config.set('mon', 'mon initial members', ', '.join(mon_initial_members))

    _config_add_monitors(config, monitors)

    with open(conf_file, 'wb') as f:
        config.write(f)

    pillar_data = _add_ceph_mon_pillar_data(mon_id_map, cluster_name, monitors)
    pillar = {'skyring': pillar_data}

    local = salt.client.LocalClient()
    out = run_state(local, minions, 'add_ceph_mon', expr_form='list',
                    kwarg={'pillar': pillar})
    if out:
        log.error('%s-add_mon failed for %s. error=%s' %
                  (ctxt, minion_set, out))
        raise Exception('add_mon failed for %s. error=%s' %
                        (minion_set, out))

    out = sync_ceph_conf(cluster_name, minions)
    if out:
        log.error("%s-sync_ceph_conf failed to %s. error=%s" %
                  (ctxt, minions, out))
        raise Exception("sync_ceph_conf failed to %s. error=%s" %
                        (minions, out))

    return True


def StartMon(monitors, ctxt=""):
    local = salt.client.LocalClient()
    out = run_state(local, monitors, 'start_ceph_mon', expr_form='list')
    if out:
        log.error("%s-start_mon failed to %s. error=%s" %
                  (ctxt, monitors, out))
        raise Exception("start_mon failed to %s. error=%s" %
                        (monitors, out))
    return True


def AddOSD(cluster_name, minions, ctxt=""):
    # convert minions dict to below dict
    # {MINION_ID: {'public_ip': IP_ADDRESS,
    #              'cluster_ip': IP_ADDRESS,
    #              'devices': {DEVICE: FSTYPE, ...}}, ...}
    d = {minions['Node']: {'public_ip': minions['PublicIP4'],
                           'cluster_ip': minions['ClusterIP4'],
                           'devices': {
                               minions['Device']: minions['FSType'],
                           }}}
    minions = d

    conf_file = (_CEPH_CLUSTER_CONF_DIR + '/' + cluster_name + '/' +
                 cluster_name + '.conf')
    config = ConfigParser.RawConfigParser()
    config.read(conf_file)

    public_network = IPNetwork(config.get('global', 'public network'))
    if config.has_option('global', 'cluster network'):
        cluster_network = IPNetwork(config.get('global', 'cluster network'))
    else:
        cluster_network = None
    public_network, cluster_network = _check_minion_networks(
        minions, public_network, cluster_network, check_cluster_network=True)

    pillar_data = {}
    for minion, v in minions.iteritems():
        pillar_data[minion] = {'cluster_name': cluster_name,
                               'cluster_id': config.get('global', 'fsid'),
                               'devices': v['devices']}
    pillar = {'skyring': pillar_data}

    local = salt.client.LocalClient()
    out = run_state(local, minions, 'prepare_ceph_osd', expr_form='list',
                    kwarg={'pillar': pillar})
    if out:
        log.error("%s-prepare_osd failed for %s. error=%s" %
                  (ctxt, minions, out))
        raise Exception("prepare_osd failed for %s. error=%s" %
                        (minions, out))

    for minion, v in minions.iteritems():
        count = 0
        found = False
        failed_devices = []
        while count < 6:
            out = local.cmd(minion, 'cmd.run_all', ['ls -l /dev/disk/by-parttypeuuid'])
            time.sleep(15)
            for key, value in v['devices'].iteritems():
                val_to_check = key.split('/')[-1]
                found = False
                for line in out[minion]["stdout"].splitlines():
                    if val_to_check in line:
                        found = True
                        if key in failed_devices:
                            failed_devices.remove(key)
                        break
                if not found:
                    if key not in failed_devices:
                        failed_devices.append(key)
                    break
            if found:
                break
            count += 1
        if len(failed_devices) != 0:
            log.error("%s-prepare_osd failed for %s" % (ctxt, failed_devices))
            raise Exception("prepare_osd failed for %s" % failed_devices)

    out = local.cmd(minions, 'cmd.run_all', ['ceph-disk activate-all'],
                    expr_form='list')

    osd_map = {}
    failed_minions = {}
    for minion, v in out.iteritems():
        osds = []

        if v.get('retcode') != 0:
            failed_minions[minion] = v
            continue

        for line in v['stdout'].splitlines():
            if line.startswith('=== '):
                osds.append(line.split('=== ')[1].strip())
                break
        osd_map[minion] = osds

    config.set('global', 'cluster network', cluster_network)
    for minion, osds in osd_map.iteritems():
        name = _get_short_hostname(minion)
        for osd in osds:
            config.add_section(osd)
            config.set(osd, 'host', name)
            config.set(osd, 'public addr', minions[minion]['public_ip'])
            config.set(osd, 'cluster addr', minions[minion]['cluster_ip'])

    with open(conf_file, 'wb') as f:
        config.write(f)

    out = sync_ceph_conf(cluster_name, minions)
    if out:
        log.error("%s-sync_cepH-conf failed for %s. error=%s" %
                  (ctxt, minions, out))
        #raise Exception("sync_ceph_conf failed for %s. error=%s" %
        #                (minions, out))

    if failed_minions:
        log.error('%s-add_osd failed. error=%s' % (ctxt, failed_minions))
        raise Exception('add_osd failed. error=%s' % failed_minions)

    return osd_map


def CreatePool(pool_name, monitor, cluster_name, pg_num=0, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.createPool',
                    [cluster_name, pool_name, pg_num])

    if out.get(monitor, {}).get('retcode') == 0:
        return True

    log.error("%s-create_pool failed. error=%s" % (ctxt, out))
    raise Exception("create_pool failed. error=%s" % out)


def ListPool(monitor, cluster_name, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.getPoolList', [cluster_name])

    if out.get(monitor, {}).get('retcode') == 0:
        stdout = out.get(monitor, {}).get('stdout')
        return [pool['poolname'] for pool in json.loads(stdout)]

    log.error("%s-list_pool failed. error=%s" % (ctxt, out))
    raise Exception("list_pool failed. error=%s" % out)


def RemovePool(monitor, cluster, pool, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.removePool', [cluster, pool])
    if out.get(monitor, 'Failed') == '':
        return True

    log.error("%s - Remove pool failed. error=%s" % (ctxt, out))
    raise Exception("Remove pool failed. error=%s", out)


def GetClusterStatus(monitor, cluster_name, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.getClusterStatus', [cluster_name])
    if out[monitor] != '':
        return out[monitor]

    log.error("%s-ceph cluster status failed. error=%s" % (ctxt, out))
    raise Exception("ceph cluster status failed. error=%s" % out)


def ParticipatesInCluster(node, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(node, 'cmd.run_all', ['ls -l /var/lib/ceph/mon/*'])
    if out[node]['retcode'] == 0:
        return True
    out = local.cmd(node, 'cmd.run_all', ['ls -l /var/lib/ceph/osd/*'])
    if out[node]['retcode'] == 0:
        return True
    return False


def GetRBDStats(monitor, pool_name, cluster_name, ctxt=""):
    ret_val = []
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.getRBDStats', [pool_name, cluster_name])
    if not out[monitor]:
        log.error("%s- failed to get rbd stats from pool %s of cluster %s. error=%s" % (ctxt, pool_name, cluster_name, out))
        raise Exception("failed to get rbd stats from pool %s of cluster %s. error=%s" % (ctxt, pool_name, cluster_name, out))
    try:
        stats = ast.literal_eval(out[monitor])
        if stats.has_key('images'):
            for rbd in stats["images"]:
                stat = {}
                stat["Name"] = rbd["name"]
                stat["Used"] = rbd["used_size"]
                stat["Total"] = rbd["provisioned_size"]
                ret_val.append(stat)
        return ret_val
    except SyntaxError as err:
        log.error("%s-Failed to get RBD utilization details from mon %s for pool %s of cluster %s, error=%s" % (ctxt, monitor, pool_name, cluster_name, out[monitor]))
        raise Exception("Failed to get RBD utilization details from mon %s for pool %s of cluster %s, error=%s" % (monitor, pool_name, cluster_name, out[monitor]))


def GetClusterStats(monitor, cluster_name, ctxt=""):
    ret_val = {}
    local = salt.client.LocalClient()
    out = local.cmd(monitor, "ceph.getClusterStats", [cluster_name])
    if not out:
        log.error("%s-Failed to get cluster statistics from %s" % (ctxt, monitor))
        raise Exception("Failed to get cluster statistics from %s" % monitor)
    try:
        stats = ast.literal_eval(out[monitor])
        ret_val["Used"] = stats["stats"]["total_used_bytes"]
        ret_val["Available"] = stats["stats"]["total_avail_bytes"]
        ret_val["Total"] = stats["stats"]["total_bytes"]
        pools = []
        poolStat = {}
        if "pools" in stats:
            for pool in stats["pools"]:
                poolStat["Name"] = pool["name"]
                poolStat["Id"] = pool["id"]
                poolStat["Used"] = pool["stats"]["bytes_used"]
                poolStat["Available"] = pool["stats"]["max_avail"]
                pools.append(poolStat)
            ret_val["Pools"] = pools
            return ret_val
    except SyntaxError as err:
        log.error("%s-Failed to get cluster stats from mon %s, error=%s" % (ctxt, monitor, out[monitor]))
        raise Exception("Failed to get cluster stats from mon %s, error:%s" % (monitor, err))
    log.error("%s-Failed to get cluster stats from mon %s, error=%s" % (ctxt, monitor, out[monitor]))
    raise Exception("Failed to get cluster stats from mon %s, error:%s" % (monitor, err))


def GetObjectCount(monitor, cluster_name, ctxt=""):
    local = salt.client.LocalClient()
    res = local.cmd(monitor, "ceph.getObjectCount", [cluster_name])
    object_cnt = {}
    if res:
        num_objects = 0
        num_objects_degraded = 0
        try:
            res = ast.literal_eval(res[monitor])
            if "pools" in res:
                for pool in res["pools"]:
                    num_objects = num_objects + int(pool["num_objects"])
                    num_objects_degraded = num_objects_degraded + int(pool["num_objects_degraded"])
                object_cnt["num_objects"] = num_objects
                object_cnt["num_objects_degraded"] = num_objects_degraded
                return object_cnt
        except SyntaxError as err:
            log.error("%s-Failed to get object count from mon %s, error=%s" % (ctxt, monitor, out[monitor]))
            raise Exception("Failed to get boject count from mon %s, error:%s" % (monitor, err))
    log.error("%s-Object Count failed. error=%s" % (ctxt, out))
    raise Exception("Object Count failed. error=%s" % out)


def GetOSDDetails(monitor, cluster_name, ctxt=""):
    '''
    returns a list of osd details in a dictonary
    [{'Name' : 'Name of the osd device',
      'Id': 'OSD id',
      'Available': 'available space in GB',
      'Used': 'used data in kb',
      'UsagePercent': 'usage percentage'}, ...]
    '''

    rv = []
    local = salt.client.LocalClient()
    out = local.cmd('%s' % monitor, 'ceph.getOSDDetails', [cluster_name])
    if not out:
        log.error("%s-Failed to get cluster statistics from %s" % (ctxt, monitor))
        raise Exception("Failed to get cluster statistics from %s" % monitor)
    try:
        dist = ast.literal_eval(out[monitor])
    except SyntaxError as err:
        log.error("%s-Failed to get OSD utilization details from mon %s, error=%s" % (ctxt, monitor, out[monitor]))
        raise Exception("Failed to get OSD utilization details from mon %s, error:%s" % (monitor, err))

    if dist.has_key('nodes'):
        for osd in dist['nodes']:
            stat = {}
	    if osd.has_key('name'):
	        stat['Name'] = osd['name']
            if osd.has_key('id'):
                stat['Id'] = int(osd['id'])
            if osd.has_key('utilization'):
                stat['UsagePercent'] = int(osd['utilization'])
            if osd.has_key('kb_avail'):
                stat['Available'] = int(osd['kb_avail'])
            if osd.has_key('kb_used'):
                stat['Used'] = int(osd['kb_used'])
            rv.append(stat)
    return rv


def GetPartDeviceDetails(node, partPath, ctxt=""):
    '''
    returns structure
    {"devname":  "devname",
     "uuid": "uuid"
     "partname": "partname",
     "fstype":   "fstype",
     "size":     uint64,
    }
    '''

    columes = 'NAME,KNAME,FSTYPE,MOUNTPOINT,UUID,PARTUUID,MODEL,SIZE,TYPE,' \
              'PKNAME,VENDOR'
    keys = columes.split(',')
    lsblk = ("lsblk --all --bytes --noheadings --output='%s' --path --raw" %
             columes)
    local = salt.client.LocalClient()
    out = local.cmd([node], 'cmd.run', [lsblk], expr_form='list')

    if not out[node]:
        return {}

    devlist = map(lambda line: dict(zip(keys, line.split(' '))),
                  out[node].splitlines())

    dev_info = {}
    for dev in devlist:
        if dev['TYPE'] == 'part' and dev['MOUNTPOINT'] == partPath:
            try:
                u = list(bytearray(uuid.UUID(dev["UUID"]).get_bytes()))
            except ValueError:
                log.warn("%s-Unable to parse device uuid for %s" % (ctxt, dev["PKNAME"]))
                u = [0] * 16
            dev_info["DevName"] = dev["PKNAME"]
            dev_info["Uuid"] = u
            dev_info["PartName"] = dev["KNAME"]
            dev_info["FSType"] = dev["FSTYPE"]
            dev_info["Size"] = long(dev["SIZE"])

    return dev_info


def GetJournalDeviceDetails(node, journalPath, ctxt=""):
    '''
    returns structure
    {"devname":  "devname",
     "uuid": "uuid"
     "partname": "partname",
     "fstype":   "fstype",
     "size":     uint64,
    }
    '''

    columes = 'NAME,KNAME,FSTYPE,MOUNTPOINT,UUID,PARTUUID,MODEL,SIZE,TYPE,' \
              'PKNAME,VENDOR'
    keys = columes.split(',')
    lsblk = ("lsblk --all --bytes --noheadings --output='%s' --path --raw %s" %
             (columes, journalPath))
    local = salt.client.LocalClient()
    out = local.cmd([node], 'cmd.run', [lsblk], expr_form='list')

    if not out[node]:
        return {}

    devlist = map(lambda line: dict(zip(keys, line.split(' '))),
                  out[node].splitlines())

    dev_info = {}
    try:
	u = list(bytearray(uuid.UUID(devlist[0]["UUID"]).get_bytes()))
    except ValueError:
	log.warn("%s-Unable to parse device uuid for %s" % (ctxt, devlist[0]["PKNAME"]))
	u = [0] * 16
    dev_info["DevName"] = devlist[0]["PKNAME"]
    dev_info["Uuid"] = u
    dev_info["PartName"] = devlist[0]["KNAME"]
    dev_info["FSType"] = devlist[0]["FSTYPE"]
    dev_info["Size"] = long(devlist[0]["SIZE"])
    return dev_info


def GetServiceCount(node, ctxt=""):
    service_count = {'SluServiceCount':0,'MonServiceCount':0}
    local = salt.client.LocalClient()
    out = local.cmd(node, 'status.procs')
    for key, value in out[node].iteritems():
        for key, val in value.iteritems():
	    if val.startswith('/usr/bin/ceph-osd'):
                service_count['SluServiceCount'] += 1
            if val.startswith('/usr/bin/ceph-mon'):
                service_count['MonServiceCount'] += 1
    return service_count


def StartCalamari(node, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(
        [node],
        'cmd.run',
        ['calamari-ctl initialize --admin-username admin --admin-password admin --admin-email skyring@redhat.com'],
        expr_form='list')

    return True


def StopCalamari(node, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(
        [node],
        'cmd.run',
        ['supervisorctl stop all'],
        expr_form='list')

    return True


def EmitRbdEvents(node, cluster, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(node, 'mon_remote.rbd_eventer', [cluster])
    return True


def AddOsdToCrush(monitor, clusterName, osdName, weight, host, ctxt=""):
    local = salt.client.LocalClient()
    out = local.cmd(monitor, 'ceph.addOsdToCrush',
                    [clusterName, osdName, weight, host])

    if out.get(monitor, 1) == 1:
        log.error("%s-addOsdToCrush failed." % ctxt)
        raise Exception("addOsdToCrush failed.")

    return True
