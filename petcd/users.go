package petcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/proxysql"
	"github.com/juju/errors"
)

// sync etcd users informations to proxysql_users
func SyncUserToProxy(etcdcli *EtcdCli, cli *clientv3.Client) error {

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
		// get users information.
		var tmpusr proxysql.Users
		// key is username ,like user01
		// value is proxysql.Users []byte type.
		key, _ := base64.StdEncoding.DecodeString(string(evs.Key))
		value, _ := base64.StdEncoding.DecodeString(string(evs.Value))

		// []byte to proxysql.Users struct.
		if err := json.Unmarshal(value, &tmpusr); err != nil {
			return errors.Trace(err)
		}

		log.Printf("Syncing %s into proxysql", tmpusr.Username)
		// new user handler
		newuser, err := proxysql.NewUser(string(key), tmpusr.Password, tmpusr.DefaultHostgroup, tmpusr.Username)
		if err != nil {
			return errors.Trace(err)
		}

		newuser.SetUserActive(tmpusr.Active)
		newuser.SetFastForward(tmpusr.FastForward)
		newuser.SetBackend(tmpusr.Backend)
		newuser.SetFrontend(tmpusr.Frontend)
		newuser.SetMaxConnections(tmpusr.MaxConnections)
		newuser.SetSchemaLocked(tmpusr.SchemaLocked)
		newuser.SetTransactionPersistent(tmpusr.TransactionPersistent)
		newuser.SetUseSSL(tmpusr.UseSsl)

		err = newuser.AddOneUser(db)
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
func CreateOneUser(etcdcli *EtcdCli) error {

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

	// get users information.
	var tmpusr proxysql.Users
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpusr); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newuser, err := proxysql.NewUser(string(key), tmpusr.Password, tmpusr.DefaultHostgroup, tmpusr.Username)
	if err != nil {
		return errors.Trace(err)
	}

	newuser.SetUserActive(tmpusr.Active)
	newuser.SetFastForward(tmpusr.FastForward)
	newuser.SetBackend(tmpusr.Backend)
	newuser.SetFrontend(tmpusr.Frontend)
	newuser.SetMaxConnections(tmpusr.MaxConnections)
	newuser.SetSchemaLocked(tmpusr.SchemaLocked)
	newuser.SetTransactionPersistent(tmpusr.TransactionPersistent)
	newuser.SetUseSSL(tmpusr.UseSsl)

	err = newuser.AddOneUser(db)
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
func UpdateOneUser(etcdcli *EtcdCli) error {

	// new proxysql conenction
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection.
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	// new proxysql mysql_users instance.
	var tmpusr proxysql.Users
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// convert []byte to json
	if err := json.Unmarshal(value, &tmpusr); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newuser, err := proxysql.NewUser(string(key), tmpusr.Password, tmpusr.DefaultHostgroup, tmpusr.Username)
	if err != nil {
		return errors.Trace(err)
	}
	newuser.SetUserActive(tmpusr.Active)
	newuser.SetFastForward(tmpusr.FastForward)
	newuser.SetBackend(tmpusr.Backend)
	newuser.SetFrontend(tmpusr.Frontend)
	newuser.SetMaxConnections(tmpusr.MaxConnections)
	newuser.SetSchemaLocked(tmpusr.SchemaLocked)
	newuser.SetTransactionPersistent(tmpusr.TransactionPersistent)
	newuser.SetUseSSL(tmpusr.UseSsl)

	err = newuser.UpdateOneUserInfo(db)
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
func DeleteOneUser(etcdcli *EtcdCli) error {

	// new proxysql connection.
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		return errors.Trace(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	// open proxysql connection.
	db, err := conn.OpenConn()
	if err != nil {
		return errors.Trace(err)
	}

	//var tmpusr proxysql.Users
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	//value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// convert []byte to json
	//if err := json.Unmarshal(value, &tmpusr); err != nil {
	//	return errors.Trace(err)
	//}

	newuser, err := proxysql.NewUser(string(key), "111111", 0, string(key))
	if err != nil {
		return errors.Trace(err)
	}

	err = newuser.DeleteOneUser(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
