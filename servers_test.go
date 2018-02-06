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

func TestServer(t *testing.T) {
	// set etcd dbi
	endpoints := []string{"172.18.10.136:2379"}
	etcdcli := petcd.NewEtcdCli(endpoints)

	etcdcli.SetPrefix("database")
	etcdcli.SetService("parauser")
	etcdcli.SetEtcdType("servers")
	etcdcli.MakeWatchRoot()

	cli, err := etcdcli.OpenEtcd()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < 1000; i++ {

		fmt.Println("add user devtest" + strconv.Itoa(i))
		// new users handler
		srv01, err := proxysql.NewServer(uint64(i), "127.0.0.1", uint64(3301+i))
		if err != nil {
			t.Error(err)
		}

		srv01.SetServerStatus("ONLINE")
		srv01.SetServerWeight(1000)
		srv01.SetServerCompression(0)
		srv01.SetServerMaxConnection(10000)
		srv01.SetServerMaxReplicationLag(0)
		srv01.SetServerUseSSL(0)
		srv01.SetServerMaxLatencyMs(0)
		srv01.SetServersComment("test hostgroup")

		fmt.Println("srv >>>", srv01.HostGroupId, srv01.HostName, srv01.Port)

		key := []byte(strconv.Itoa(int(srv01.HostGroupId)) + "|" + srv01.HostName + "|" + strconv.Itoa(int(srv01.Port)))
		if err != nil {
			t.Error(err)
		}

		value, err := json.Marshal(srv01)
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
