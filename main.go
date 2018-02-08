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

	etcdcli.SetProxyAddr("172.18.10.136")
	etcdcli.SetProxyPort(13306)
	etcdcli.SetProxyAdmin("admin")
	etcdcli.SetProxyPass("admin")

	fmt.Println(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)

	etcdcli.MakeWatchRoot()

	// see https://github.com/coreos/etcd/blob/master/clientv3/example_watch_test.go
	log.Println("Running proxysql_etcd as watch mode. the watching path is ", etcdcli.Root)

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		fmt.Println(err)
	}

	err = petcd.SyncUserToProxy(etcdcli, cli)
	if err != nil {
		fmt.Println(err)
	}

	rch := cli.Watch(context.Background(), etcdcli.Root, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {

			node := strings.Split(string(ev.Kv.Key), "/")
			etcdcli.SetEtcdKey(node[4])
			etcdcli.SetEtcdValue(string(ev.Kv.Value))

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
						log.Println("CreateOneUser ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneUser(etcdcli)
					default:
						log.Println("UpdateOneUser ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneUser(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneUser ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneUser(etcdcli)
				default:

				}

			case "servers":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneServer ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneServer(etcdcli)
					default:
						log.Println("UpdateOneServer ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneServer(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneServer ", etcdcli.Root+"/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneServer(etcdcli)
				default:

				}
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

	err = etcdcli.CloseEtcd(cli)
	if err != nil {
		fmt.Println(err)
	}
}
