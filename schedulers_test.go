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

func TestScheduler(t *testing.T) {

	flag.Parse()
	// set etcd dbi
	etcdcli := petcd.NewEtcdCli([]string{*etcd_points})

	etcdcli.SetPrefix(*etcd_prefix)
	etcdcli.SetService(*etcd_service)
	etcdcli.SetEtcdType("schedulers")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		t.Error(err)
	}

	for i := 1; i < 100; i++ {

		// new users handler
		file_name := fmt.Sprintf("file%d", i)
		schld01, err := proxysql.NewSch(file_name, int64(i)*100)
		if err != nil {
			t.Error(err)
		}

		schld01.SetSchedulerId(int64(i))

		schld_id := fmt.Sprintf("%d", schld01.Id)
		key := []byte(schld_id)
		if err != nil {
			t.Error(err)
		}

		value, err := json.Marshal(schld01)
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
