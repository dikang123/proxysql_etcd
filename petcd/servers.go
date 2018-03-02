package petcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/etcd"
	"github.com/imSQL/proxysql"
	"github.com/juju/errors"
)

// sync etcd users informations to proxysql_users
func SyncServerToProxy(etcdcli *etcd.EtcdCli, cli *clientv3.Client) error {

	// get value from etcd
	ctx, cancel := context.WithTimeout(context.Background(), etcdcli.RequestTimeout)
	resp, err := cli.Get(ctx, etcdcli.Root+"/users", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	//new proxysql connection.
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	fmt.Println(resp.Kvs)

	for _, evs := range resp.Kvs {
		// get servers information.
		var tmpsrv proxysql.Servers
		// key is username ,like user01
		// value is proxysql.Users []byte type.
		//key, _ := base64.StdEncoding.DecodeString(string(evs.Key))
		value, _ := base64.StdEncoding.DecodeString(string(evs.Value))

		// []byte to proxysql.Users struct.
		if err := json.Unmarshal(value, &tmpsrv); err != nil {
			return errors.Trace(err)
		}

		// new user handler
		newsrv, err := proxysql.NewServer(tmpsrv.HostGroupId, tmpsrv.HostName, tmpsrv.Port)
		if err != nil {
			return errors.Trace(err)
		}

		newsrv.SetServerStatus(tmpsrv.Status)
		newsrv.SetServerWeight(tmpsrv.Weight)
		newsrv.SetServerCompression(tmpsrv.Compression)
		newsrv.SetServerMaxConnection(tmpsrv.MaxConnections)
		newsrv.SetServerMaxReplicationLag(tmpsrv.MaxReplicationLag)
		newsrv.SetServerUseSSL(tmpsrv.UseSsl)
		newsrv.SetServerMaxLatencyMs(tmpsrv.MaxLatencyMs)
		newsrv.SetServersComment(tmpsrv.Comment)

		err = newsrv.AddOneServers(db)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

//create a new mysql_users in proxysql.
//create success return nil,else return error
func CreateOneServer(etcdcli *etcd.EtcdCli) error {

	//new proxysql connection.
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	// get servers information.
	var tmpsrv proxysql.Servers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpsrv); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newsrv, err := proxysql.NewServer(tmpsrv.HostGroupId, tmpsrv.HostName, tmpsrv.Port)
	if err != nil {
		return errors.Trace(err)
	}

	newsrv.SetServerStatus(tmpsrv.Status)
	newsrv.SetServerWeight(tmpsrv.Weight)
	newsrv.SetServerCompression(tmpsrv.Compression)
	newsrv.SetServerMaxConnection(tmpsrv.MaxConnections)
	newsrv.SetServerMaxReplicationLag(tmpsrv.MaxReplicationLag)
	newsrv.SetServerUseSSL(tmpsrv.UseSsl)
	newsrv.SetServerMaxLatencyMs(tmpsrv.MaxLatencyMs)
	newsrv.SetServersComment(tmpsrv.Comment)

	err = newsrv.AddOneServers(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// update a proxysql mysql_users information.
// update successed return nil,else return error
func UpdateOneServer(etcdcli *etcd.EtcdCli) error {

	//new proxysql connection.
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	// get servers information.
	var tmpsrv proxysql.Servers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpsrv); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newsrv, err := proxysql.NewServer(tmpsrv.HostGroupId, tmpsrv.HostName, tmpsrv.Port)
	if err != nil {
		return errors.Trace(err)
	}

	newsrv.SetServerStatus(tmpsrv.Status)
	newsrv.SetServerWeight(tmpsrv.Weight)
	newsrv.SetServerCompression(tmpsrv.Compression)
	newsrv.SetServerMaxConnection(tmpsrv.MaxConnections)
	newsrv.SetServerMaxReplicationLag(tmpsrv.MaxReplicationLag)
	newsrv.SetServerUseSSL(tmpsrv.UseSsl)
	newsrv.SetServerMaxLatencyMs(tmpsrv.MaxLatencyMs)
	newsrv.SetServersComment(tmpsrv.Comment)

	err = newsrv.UpdateOneServerInfo(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// delete a proxysql mysql_users.
func DeleteOneServer(etcdcli *etcd.EtcdCli) error {

	//new proxysql connection.
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	// get servers information.
	//var tmpsrv proxysql.Servers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	//value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	server_hostgroup_id, _ := strconv.Atoi(strings.Split(string(key), "|")[0])
	server_hostname := strings.Split(string(key), "|")[1]
	server_port, _ := strconv.Atoi(strings.Split(string(key), "|")[2])

	// new user handler
	newsrv, err := proxysql.NewServer(uint64(server_hostgroup_id), server_hostname, uint64(server_port))
	if err != nil {
		return errors.Trace(err)
	}

	err = newsrv.DeleteOneServers(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
