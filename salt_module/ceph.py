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

import utils

DEFAULT_PG_NUM = 128


def createPool(clusterName, poolName, pgNum=0):
    cmd = ["ceph", "--cluster", clusterName, "osd", "pool", "create", poolName]
    if pgNum:
        cmd += [pgNum]
    else:
        cmd += [DEFAULT_PG_NUM]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def getPoolList(clusterName):
    cmd = ["ceph", "--cluster", clusterName, "-f", "json", "osd", "lspools"]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def removePool(clusterName, poolName):
    cmd = [
            "ceph",
            "osd",
            "pool",
            "delete",
            poolName,
            poolName,
            "--yes-i-really-really-mean-it",
            "--cluster",
            clusterName
          ]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def getClusterStatus(clusterName):
    cmd = ["ceph", "-s", "--cluster", clusterName]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        for line in out.splitlines():
            if line.strip().startswith("health"):
                return line.strip().split(' ')[1]
    return ''


def getClusterStatsFromJsonOp(clusterName):
    cmd = ["ceph", "df", "--cluster", clusterName, "-f", "json"]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def getClusterStats(clusterName):
    cmd = ["ceph", "df", "--cluster", clusterName]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def getRBDStats(pool_name, cluster_name):
    cmd = ["rbd", "du", "--cluster", cluster_name, "-p", pool_name, "--format=json"]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err


def getObjectCount(clusterName):
    cmd = ["rados", "df", "--cluster", clusterName, "--format", "json"]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    return {}


def getOSDDetails(clusterName):
    cmd = ["ceph", "osd", "df", "--cluster", clusterName, "-f", "json"]
    rc, out, err = utils.execCmd(cmd)
    if not rc:
        return out
    else:
        return err

def addOsdToCrush(clusterName, osdName, weight, host):
    hostStr = 'host='+host
    cmd = ["ceph", "--cluster", clusterName, "osd", "crush", "create-or-move", osdName, str(round(weight,9)), hostStr, "root=default"]
    try:
        rc, out, err = utils.execCmd(cmd)
        return rc
    except Exception as e:
        return 1
