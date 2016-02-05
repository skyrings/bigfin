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

import salt.modules.cmdmod as cmdmod

DEFAULT_PG_NUM = 128


def getDevicePartUuid(devices):
    arr = []
    path = "/dev/disk/by-parttypeuuid"
    status = {'failedList': [], 'error': ""}
    out = cmdmod.run_all("ls -l %s" % path)
    if out['retcode'] == 0:
        arr = out['stdout'].splitlines()
    else:
        status['error'] = out['stderr']
        return status
    for dev in devices.split(" "):
        found = False
        for i in arr:
            if dev.split('/')[-1] in i:
                found = True
                break
        if not found:
            status['failedList'].append(dev)
    return status


def activateAllDisk():
    return cmdmod.run_all(['ceph-disk activate-all'])


def createPool(clusterName, poolName, pgNum=0):
    cmd = "ceph --cluster %s osd pool create %s" % (clusterName, poolName)
    if pgNum:
        cmd += " %s" % pgNum
    else:
        cmd += " %s" % DEFAULT_PG_NUM
    return cmdmod.run_all(cmd)


def getPoolList(clusterName):
    cmd = "ceph --cluster %s -f json osd lspools" % clusterName
    return cmdmod.run_all(cmd)


def GetClusterStatus(clusterName):
    cmd = "ceph -s --cluster %s" % clusterName
    out = cmdmod.run_all(cmd)
    if out['retcode'] == 0:
        arr = out['stdout'].rstrip.splitlines()
        for entry in arr:
            if entry.strip().startswith('health'):
                return entry.strip().split(' ')[1]
    return ''


def getClusterStats(clusterName):
    cmd = "ceph df --cluster=%s -f json" % clusterName
    return cmdmod.run(cmd)


def getOSDDetails(clusterName):
    cmd = "ceph osd df --cluster %s -f json" % clusterName
    return cmdmod.run(cmd)
