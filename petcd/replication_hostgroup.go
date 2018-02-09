package petcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/proxysql"
	"github.com/juju/errors"
)

// sync etcd users informations to proxysql_users
func SyncRhgToProxy(etcdcli *EtcdCli, cli *clientv3.Client) error {

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
		var tmprhg proxysql.ReplicationHostgroup
		// key is username ,like user01
		// value is proxysql.Users []byte type.
		//key, _ := base64.StdEncoding.DecodeString(string(evs.Key))
		value, _ := base64.StdEncoding.DecodeString(string(evs.Value))

		// []byte to proxysql.Users struct.
		if err := json.Unmarshal(value, &tmprhg); err != nil {
			return errors.Trace(err)
		}

		// new user handler
		newrhg, err := proxysql.NewRHG(tmprhg.WriterHostgroup, tmprhg.ReaderHostgroup)
		if err != nil {
			return errors.Trace(err)
		}

		newrhg.SetWriterHostGroup(tmprhg.WriterHostgroup)
		newrhg.SetReaderHostGroup(tmprhg.ReaderHostgroup)
		newrhg.SetComment(tmprhg.Comment)

		err = newrhg.AddOneRHG(db)
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
func CreateOneRhg(etcdcli *EtcdCli) error {

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
	var tmprhg proxysql.ReplicationHostgroup
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmprhg); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newrhg, err := proxysql.NewRHG(tmprhg.WriterHostgroup, tmprhg.ReaderHostgroup)
	if err != nil {
		return errors.Trace(err)
	}

	newrhg.SetWriterHostGroup(tmprhg.WriterHostgroup)
	newrhg.SetReaderHostGroup(tmprhg.ReaderHostgroup)
	newrhg.SetComment(tmprhg.Comment)

	err = newrhg.AddOneRHG(db)
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
func UpdateOneRhg(etcdcli *EtcdCli) error {

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
	var tmprhg proxysql.ReplicationHostgroup
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmprhg); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newrhg, err := proxysql.NewRHG(tmprhg.WriterHostgroup, tmprhg.ReaderHostgroup)
	if err != nil {
		return errors.Trace(err)
	}

	newrhg.SetWriterHostGroup(tmprhg.WriterHostgroup)
	newrhg.SetReaderHostGroup(tmprhg.ReaderHostgroup)
	newrhg.SetComment(tmprhg.Comment)

	err = newrhg.UpdateOneRHG(db)
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
func DeleteOneRhg(etcdcli *EtcdCli) error {

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
	var tmprhg proxysql.ReplicationHostgroup
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmprhg); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newrhg, err := proxysql.NewRHG(tmprhg.WriterHostgroup, tmprhg.ReaderHostgroup)
	if err != nil {
		return errors.Trace(err)
	}

	newrhg.SetWriterHostGroup(tmprhg.WriterHostgroup)
	newrhg.SetReaderHostGroup(tmprhg.ReaderHostgroup)
	newrhg.SetComment(tmprhg.Comment)

	err = newrhg.DeleteOneRHG(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
