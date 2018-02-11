#########################################################################
# File Name: startup_test.sh
# Author: Lei Tian
# mail: taylor840326@gmail.com
# Created Time: 2018-02-06 17:20
#########################################################################
#!/bin/bash


export ETCD_ADDR="172.18.10.136:2379"
export ETCD_PREFIX="database"
export ETCD_SVC="parauser"

go test -timeout 30m 
