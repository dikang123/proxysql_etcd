package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"testing"

	"github.com/imSQL/proxysql"

	"github.com/coreos/etcd/clientv3"

	"github.com/imSQL/proxysql_etcd/petcd"
)

func TestVariables(t *testing.T) {

	vars := new(proxysql.Variables)
	vars.VariablesName = "mysql-wait_timeout"
	vars.Value = "9898"

	flag.Parse()
	// set etcd dbi
	etcdcli := petcd.NewEtcdCli([]string{*etcd_points})

	etcdcli.SetPrefix(*etcd_prefix)
	etcdcli.SetService(*etcd_service)
	etcdcli.SetEtcdType("servers")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		fmt.Println(err)
	}

	key := []byte(vars.VariablesName)
	if err != nil {
		t.Error(err)
	}

	value, err := json.Marshal(vars)
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
}
