package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/imSQL/proxysql"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	dialTimeout    = 5 * time.Second
	requestTimeout = 2 * time.Second
	endpoints      = []string{"172.18.10.111:2379"}
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	log.Println("存储值")
	if _, err := cli.Put(context.TODO(), "sensors", `{sensor01:{topic:"w_sensor01"}}`); err != nil {
		log.Fatal(err)
	}
	log.Println("获取值")
	if resp, err := cli.Get(context.TODO(), "sensors"); err != nil {
		log.Fatal(err)
	} else {
		log.Println("resp: ", resp)
	}
	// see https://github.com/coreos/etcd/blob/master/clientv3/example_kv_test.go#L220
	log.Println("事务&超时")
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	_, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("key"), ">", "abc")). // txn value comparisons are lexical
		Then(clientv3.OpPut("key", "XYZ")).                      // this runs, since 'xyz' > 'abc'
		Else(clientv3.OpPut("key", "ABC")).
		Commit()
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	// see https://github.com/coreos/etcd/blob/master/clientv3/example_watch_test.go
	log.Println("监视")
	rch := cli.Watch(context.Background(), "/database/parauser", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {

			node := strings.Split(string(ev.Kv.Key), "/")

			fmt.Println(node[4])
			switch node[3] {
			/*
				/database/parauser/users/user1/name
				/database/parauser/users/user1/pass
			*/
			case "users":
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case ev.IsCreate():
						fmt.Printf("%s Create %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
						conn, err := proxysql.NewConn("172.18.10.111", 13306, "admin", "admin")
						if err != nil {
							fmt.Println(err)
						}
						conn.SetCharset("utf8")
						conn.SetCollation("utf8_general_ci")
						conn.MakeDBI()

						db, err := conn.OpenConn()
						if err != nil {
							fmt.Println(err)
						}

						fmt.Println(ev.Kv.Value)
						var tmpusr proxysql.Users
						if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
							fmt.Println(err)
						}
						fmt.Println(tmpusr)
						newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
						if err != nil {
							fmt.Println(err)
						}

						newuser.SetUserActive(1)

						err = newuser.AddOneUser(db)
						if err != nil {
							fmt.Println(err)
						}
					default:
						fmt.Printf("%s Update %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
						fmt.Printf("%s Create %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
						conn, err := proxysql.NewConn("172.18.10.111", 13306, "admin", "admin")
						if err != nil {
							fmt.Println(err)
						}
						conn.SetCharset("utf8")
						conn.SetCollation("utf8_general_ci")
						conn.MakeDBI()

						db, err := conn.OpenConn()
						if err != nil {
							fmt.Println(err)
						}

						fmt.Println(ev.Kv.Value)
						var tmpusr proxysql.Users
						if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
							fmt.Println(err)
						}
						fmt.Println(tmpusr)
						newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
						if err != nil {
							fmt.Println(err)
						}

						newuser.SetUserActive(1)

						err = newuser.UpdateOneUserInfo(db)
						if err != nil {
							fmt.Println(err)
						}
					}
				case mvccpb.DELETE:
					fmt.Printf("%s Default %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					fmt.Printf("%s Create %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					conn, err := proxysql.NewConn("172.18.10.111", 13306, "admin", "admin")
					if err != nil {
						fmt.Println(err)
					}
					conn.SetCharset("utf8")
					conn.SetCollation("utf8_general_ci")
					conn.MakeDBI()

					db, err := conn.OpenConn()
					if err != nil {
						fmt.Println(err)
					}

					fmt.Println(ev.Kv.Value)
					var tmpusr proxysql.Users
					if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
						fmt.Println(err)
					}
					fmt.Println(tmpusr)
					newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
					if err != nil {
						fmt.Println(err)
					}

					newuser.SetUserActive(1)

					err = newuser.DeleteOneUser(db)
					if err != nil {
						fmt.Println(err)
					}
				default:

				}

			case "servers":

			default:
				fmt.Println("node[3] " + node[3])
			}
		}
	}
}
