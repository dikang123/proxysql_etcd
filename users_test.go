package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/imSQL/proxysql"
	"github.com/imSQL/proxysql_etcd/petcd"
)

func TestUser(t *testing.T) {
	// set etcd dbi
	endpoints := []string{"172.18.10.136:2379"}
	etcdcli := petcd.NewEtcdCli(endpoints)

	etcdcli.SetPrefix("database")
	etcdcli.SetService("parauser")
	etcdcli.SetEtcdKey("users")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 1000; i++ {

		fmt.Println("add user devtest" + strconv.Itoa(i))
		// new users handler
		user01, err := proxysql.NewUser("devtest"+strconv.Itoa(i), "devtest"+strconv.Itoa(i), 0, "devtest"+strconv.Itoa(i))
		if err != nil {
			t.Error(err)
		}

		user01.SetBackend(1)
		user01.SetFrontend(1)
		user01.SetFastForward(1)
		user01.SetMaxConnections(10000)
		user01.SetSchemaLocked(0)
		user01.SetTransactionPersistent(0)
		user01.SetUserActive(1)
		user01.SetUseSSL(0)

		fmt.Println("user01.Username=" + user01.Username)

		key := []byte(user01.Username)
		if err != nil {
			t.Error(err)
		}

		value, err := json.Marshal(user01)
		if err != nil {
			t.Error(err)
		}

		fmt.Println("key=", key, " value=", value)

		// base64
		encodeKey := base64.StdEncoding.EncodeToString(key)
		encodeValue := base64.StdEncoding.EncodeToString(value)

		fmt.Println("encodKey=", encodeKey, " encodeValue=", encodeValue)

		ctx, cancel := context.WithTimeout(context.Background(), etcdcli.RequestTimeout)
		//create user
		_, err = cli.Put(ctx, etcdcli.Root+"/"+encodeKey, encodeValue)
		cancel()
		if err != nil {
			t.Error(err)
		}

		fmt.Println("Put success")

		// update user
		ctx, cancel = context.WithTimeout(context.Background(), etcdcli.RequestTimeout)
		_, err = cli.Put(ctx, etcdcli.Root+"/"+encodeKey, encodeValue)
		cancel()
		if err != nil {
			t.Error(err)
		}

		fmt.Println("Put success")

		// delete user
		fmt.Println(etcdcli.Root + "/" + encodeKey)
		ctx, cancel = context.WithTimeout(context.Background(), etcdcli.RequestTimeout)
		_, err = cli.Delete(ctx, etcdcli.Root+"/"+encodeKey, clientv3.WithPrefix())
		if err != nil {
			t.Error(err)
		}

		fmt.Println("Del success")

		if i%100 == 0 {
			time.Sleep(time.Second * 30)
		}
	}

}
