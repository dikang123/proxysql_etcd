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

	// see https://github.com/coreos/etcd/blob/master/clientv3/example_watch_test.go
	log.Println("Running proxysql_etcd as watch mode. the watching path is /database/parauser")
	rch := cli.Watch(context.Background(), "/database/parauser", clientv3.WithPrefix())
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
						fmt.Printf("Create %q : %q\n", ev.Kv.Key, ev.Kv.Value)
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

						var tmpusr proxysql.Users
						if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
							fmt.Println(err)
						}
						//tmpusr.Username = node[4]

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
						fmt.Printf("Update %q : %q\n", ev.Kv.Key, ev.Kv.Value)
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

						var tmpusr proxysql.Users
						if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
							fmt.Println(err)
						}

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

					fmt.Printf("Delete %q \n", ev.Kv.Key)

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

					var tmpusr proxysql.Users
					tmpusr.Username = node[4]

					newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
					if err != nil {
						fmt.Println(err)
					}

					//newuser.SetUserActive(1)

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
