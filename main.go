package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/imSQL/proxysql_etcd/petcd"
)

func main() {

	endpoints := []string{"172.18.10.136:2379"}
	etcdcli := petcd.NewEtcdCli(endpoints)

	etcdcli.SetPrefix("database")
	etcdcli.SetService("parauser")

	etcdcli.OpenEtcd()

	// see https://github.com/coreos/etcd/blob/master/clientv3/example_watch_test.go
	log.Println("Running proxysql_etcd as watch mode. the watching path is /database/parauser")
	rch := etcdcli.etcdv3.Watch(context.Background(), "/database/parauser", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {

			node := strings.Split(string(ev.Kv.Key), "/")

			switch node[3] {
			/*
				/database/parauser/users/user01 '{"username":"parauser_v2","password":"123456"}'
				node[1] -> database
				node[2] -> parauser
				node[3] -> users
				node[4] -> user01
			*/
			case "users":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						petcd.CreateOneUser(ev)
					default:
						petcd.UpdateOneUser(ev)
					}
				case mvccpb.DELETE:
					petcd.DeleteOneUser(ev, node[4])
				default:

				}

			case "servers":
				fmt.Println("servers")
			case "queryrules":
				fmt.Println("queryrules")
			case "schedulers":
				fmt.Println("schedulers")
			case "variables":
				fmt.Println("variables")
			default:
				fmt.Println("node[3] " + node[3])
			}
		}
	}
}
