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

import cephutils

DEFAULT_PG_NUM = 128


def createPool(clusterName, poolName, pgNum=0):
    cmd = ["ceph", "--cluster", clusterName, "osd", "pool", "create", poolName]
    if pgNum:
        cmd += [pgNum]
    else:
        cmd += [DEFAULT_PG_NUM]
    rc, out, err = cephutils.execCmd(cmd)
    return out


def getPoolList(clusterName):
    cmd = ["ceph", "--cluster", clusterName, "-f", "json", "osd", "lspools"]
    rc, out, err = cephutils.execCmd(cmd)
    return out


def getClusterStatus(clusterName):
    cmd = ["ceph", "-s", "--cluster", clusterName]
    rc, out, err = cephutils.execCmd(cmd)
    for line in out.splitlines():
        if line.strip().startswith("health"):
            return line.strip().split(' ')[1]
    return ''


def getClusterStats(clusterName):
    cmd = ["ceph", "df", "--cluster", clusterName, "-f", "json"]
    rc, out, err = cephutils.execCmd(cmd)
    return out


def getObjectCount(clusterName):
    cmd = ["ceph", "-s", "--cluster", clusterName]
    rc, out, err = cephutils.execCmd(cmd)
    for line in out.splitlines():
        if line.strip().startswith("pgmap"):
            return line.split(",")[3].split(' ')[1]
    return ''

def getOSDDetails(clusterName):
    cmd = ["ceph", "osd", "df", "--cluster", clusterName, "-f", "json"]
    rc, out, err = cephutils.execCmd(cmd)
    return out
