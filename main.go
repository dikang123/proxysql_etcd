package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/imSQL/proxysql_etcd/petcd"
)

func main() {

	// get value from env
	// etcd_endpoints like "192.168.100.10:2379,192.168.100.11:2379,192.168.100.12:2379"
	// etcd_prefix like "database"
	// etcd_service like "users"
	// proxysql_addr like "user:password@addr?dbname"

	etcd_endpoints := os.Getenv("ETCD_ADDR")
	etcd_prefix := os.Getenv("ETCD_PREFIX")
	etcd_service := os.Getenv("ETCD_SVC")
	proxysql_addr := os.Getenv("PROXYSQL_ADDR")
	proxysql_port := os.Getenv("PROXYSQL_PORT")
	proxysql_user := os.Getenv("PROXYSQL_USER")
	proxysql_pass := os.Getenv("PROXYSQL_PASS")

	endpoints := strings.Split(etcd_endpoints, ",")
	etcdcli := petcd.NewEtcdCli(endpoints)

	etcdcli.SetPrefix(etcd_prefix)
	etcdcli.SetService(etcd_service)

	etcdcli.SetProxyAddr(proxysql_addr)
	pport, _ := strconv.Atoi(proxysql_port)
	etcdcli.SetProxyPort(uint64(pport))
	etcdcli.SetProxyAdmin(proxysql_user)
	etcdcli.SetProxyPass(proxysql_pass)

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
						log.Println("CreateOneUser ", etcdcli.Root+"/users/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneUser(etcdcli)
					default:
						log.Println("UpdateOneUser ", etcdcli.Root+"/users/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneUser(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneUser ", etcdcli.Root+"/users/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneUser(etcdcli)
				default:

				}

			case "servers":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneServer(etcdcli)
					default:
						log.Println("UpdateOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneServer(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneServer(etcdcli)
				default:

				}
			case "rhgs":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneRhg(etcdcli)
					default:
						log.Println("UpdateOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneRhg(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneRhg(etcdcli)
				default:

				}
			case "queryrules":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneQr ", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneQr(etcdcli)
					default:
						log.Println("UpdateOneQr", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneQr(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneQr", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneQr(etcdcli)
				default:

				}
			case "schedulers":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
						petcd.CreateOneSchld(etcdcli)
					default:
						log.Println("UpdateOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
						petcd.UpdateOneSchld(etcdcli)
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
					petcd.DeleteOneSchld(etcdcli)
				default:

				}
			case "variables":
				switch ev.Type {
				case mvccpb.PUT:
					log.Println("UpdateOneVariable", etcdcli.Root+"/variables/"+etcdcli.Key, etcdcli.Value)
					petcd.UpdateOneVars(etcdcli)
				default:

				}
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
