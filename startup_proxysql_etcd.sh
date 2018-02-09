#!/bin/bash

export ETCD_ADDR="172.18.10.136:2379"
export ETCD_PREFIX="database"
export ETCD_SVC="parauser"
export PROXYSQL_ADDR="172.18.10.136"
export PROXYSQL_PORT="13306"
export PROXYSQL_USER="admin"
export PROXYSQL_PASS="admin"

go build

./proxysql_etcd

