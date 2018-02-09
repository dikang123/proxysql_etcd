package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/imSQL/proxysql"
	"github.com/imSQL/proxysql_etcd/petcd"
)

func TestQr(t *testing.T) {

	flag.Parse()
	// set etcd dbi
	etcdcli := petcd.NewEtcdCli([]string{*etcd_points})

	etcdcli.SetPrefix(*etcd_prefix)
	etcdcli.SetService(*etcd_service)
	etcdcli.SetEtcdType("queryrules")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 100; i++ {

		// new users handler
		user_name := fmt.Sprintf("user%d", i)
		qr01, err := proxysql.NewQr(user_name, uint64(i))
		if err != nil {
			t.Error(err)
		}

		qr01.SetQrRuleid(uint64(i))

		rule_id := fmt.Sprintf("%d", qr01.Rule_id)
		key := []byte(rule_id)
		if err != nil {
			t.Error(err)
		}

		value, err := json.Marshal(qr01)
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

		if i%10 == 0 {
			time.Sleep(time.Second * 3)
		}
	}

}
