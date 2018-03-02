package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/imSQL/etcd"
	"github.com/imSQL/proxysql_etcd/petcd"
	"github.com/juju/errors"
)

var etcd_points = os.Getenv("ETCD_ADDR")
var etcd_prefix = os.Getenv("ETCD_PREFIX")
var etcd_service = os.Getenv("ETCD_SVC")
var proxysql_addr = os.Getenv("PROXYSQL_ADDR")
var proxysql_port = os.Getenv("PROXYSQL_PORT")
var proxysql_user = os.Getenv("PROXYSQL_USER")
var proxysql_pass = os.Getenv("PROXYSQL_PASS")

func main() {
	//print logs to stdout.
	log.SetOutput(os.Stdout)

	// get value from env
	// etcd_endpoints like "192.168.100.10:2379,192.168.100.11:2379,192.168.100.12:2379"
	// etcd_prefix like "database"
	// etcd_service like "users"
	// proxysql_addr like "user:password@addr?dbname"

	endpoints := strings.Split(etcd_points, ",")
	etcdcli := etcd.NewEtcdCli(endpoints)

	etcdcli.SetPrefix(etcd_prefix)
	etcdcli.SetService(etcd_service)

	etcdcli.SetProxyAddr(proxysql_addr)
	pport, _ := strconv.Atoi(proxysql_port)
	etcdcli.SetProxyPort(uint64(pport))
	etcdcli.SetProxyAdmin(proxysql_user)
	etcdcli.SetProxyPass(proxysql_pass)

	etcdcli.MakeWatchRoot()

	// see https://github.com/coreos/etcd/blob/master/clientv3/example_watch_test.go
	log.Println("Running proxysql_etcd as watch mode. the watching path is ", etcdcli.Root)

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		log.Println(errors.Details(err))
	}

	// sync user informations to proxysql
	err = petcd.SyncUserToProxy(etcdcli, cli)
	if err != nil {
		log.Println(errors.Details(err))
	}
	// sync servers informations to proxysql
	err = petcd.SyncServerToProxy(etcdcli, cli)
	if err != nil {
		log.Println(errors.Details(err))
	}
	// sync qr informations to proxysql
	err = petcd.SyncQrToProxy(etcdcli, cli)
	if err != nil {
		log.Println(errors.Details(err))
	}
	// sync rhg informations to proxysql
	err = petcd.SyncRhgToProxy(etcdcli, cli)
	if err != nil {
		log.Println(errors.Details(err))
	}
	// sync scheduler informations to proxysql
	err = petcd.SyncSchldToProxy(etcdcli, cli)
	if err != nil {
		log.Println(errors.Details(err))
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
						err = petcd.CreateOneUser(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					default:
						log.Println("UpdateOneUser ", etcdcli.Root+"/users/"+etcdcli.Key, etcdcli.Value)
						err = petcd.UpdateOneUser(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneUser ", etcdcli.Root+"/users/"+etcdcli.Key, etcdcli.Value)
					err = petcd.DeleteOneUser(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}

			case "servers":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
						err = petcd.CreateOneServer(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					default:
						log.Println("UpdateOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
						err = petcd.UpdateOneServer(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneServer ", etcdcli.Root+"/servers/"+etcdcli.Key, etcdcli.Value)
					err = petcd.DeleteOneServer(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}
			case "rhgs":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
						err = petcd.CreateOneRhg(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					default:
						log.Println("UpdateOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
						err = petcd.UpdateOneRhg(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneRhg", etcdcli.Root+"/rhgs/"+etcdcli.Key, etcdcli.Value)
					err = petcd.DeleteOneRhg(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}
			case "queryrules":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneQr ", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
						err = petcd.CreateOneQr(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					default:
						log.Println("UpdateOneQr", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
						err = petcd.UpdateOneQr(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneQr", etcdcli.Root+"/queryrules/"+etcdcli.Key, etcdcli.Value)
					err = petcd.DeleteOneQr(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}
			case "schedulers":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						log.Println("CreateOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
						err = petcd.CreateOneSchld(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					default:
						log.Println("UpdateOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
						err = petcd.UpdateOneSchld(etcdcli)
						if err != nil {
							log.Println(errors.Details(err))
						}
					}
				case mvccpb.DELETE:
					log.Println("DeleteOneSchld", etcdcli.Root+"/schedulers/"+etcdcli.Key, etcdcli.Value)
					err = petcd.DeleteOneSchld(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}
			case "variables":
				switch ev.Type {
				case mvccpb.PUT:
					log.Println("UpdateOneVariable", etcdcli.Root+"/variables/"+etcdcli.Key, etcdcli.Value)
					err = petcd.UpdateOneVars(etcdcli)
					if err != nil {
						log.Println(errors.Details(err))
					}
				default:

				}
			default:
			}
		}
	}

	err = etcdcli.CloseEtcd(cli)
	if err != nil {
		log.Println(err)
	}
}
