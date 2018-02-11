package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"testing"

	"github.com/imSQL/proxysql"

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
	etcdcli.SetEtcdType("variables")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		t.Error(err)
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

}
