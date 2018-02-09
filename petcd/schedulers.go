package petcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/proxysql"
	"github.com/juju/errors"
)

// sync etcd users informations to proxysql_users
func SyncSchldToProxy(etcdcli *EtcdCli, cli *clientv3.Client) error {

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
		var tmpschld proxysql.Schedulers
		// key is username ,like user01
		// value is proxysql.Users []byte type.
		//key, _ := base64.StdEncoding.DecodeString(string(evs.Key))
		value, _ := base64.StdEncoding.DecodeString(string(evs.Value))

		// []byte to proxysql.Users struct.
		if err := json.Unmarshal(value, &tmpschld); err != nil {
			return errors.Trace(err)
		}

		// new user handler
		newschld, err := proxysql.NewSch(tmpschld.FileName, tmpschld.IntervalMs)
		if err != nil {
			return errors.Trace(err)
		}

		newschld.SetSchedulerId(tmpschld.Id)
		newschld.SetSchedulerIntervalMs(tmpschld.IntervalMs)
		newschld.SetSchedulerActive(tmpschld.Active)
		newschld.SetSchedulerArg1(tmpschld.Arg1)
		newschld.SetSchedulerArg2(tmpschld.Arg2)
		newschld.SetSchedulerArg3(tmpschld.Arg3)
		newschld.SetSchedulerArg4(tmpschld.Arg4)
		newschld.SetSchedulerArg5(tmpschld.Arg5)

		err = newschld.AddOneScheduler(db)
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
func CreateOneSchld(etcdcli *EtcdCli) error {

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
	var tmpschld proxysql.Schedulers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpschld); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newschld, err := proxysql.NewSch(tmpschld.FileName, tmpschld.IntervalMs)
	if err != nil {
		return errors.Trace(err)
	}

	newschld.SetSchedulerId(tmpschld.Id)
	newschld.SetSchedulerIntervalMs(tmpschld.IntervalMs)
	newschld.SetSchedulerActive(tmpschld.Active)
	newschld.SetSchedulerArg1(tmpschld.Arg1)
	newschld.SetSchedulerArg2(tmpschld.Arg2)
	newschld.SetSchedulerArg3(tmpschld.Arg3)
	newschld.SetSchedulerArg4(tmpschld.Arg4)
	newschld.SetSchedulerArg5(tmpschld.Arg5)

	fmt.Println(newschld)

	err = newschld.AddOneScheduler(db)
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
func UpdateOneSchld(etcdcli *EtcdCli) error {

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
	var tmpschld proxysql.Schedulers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpschld); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newschld, err := proxysql.NewSch(tmpschld.FileName, tmpschld.IntervalMs)
	if err != nil {
		return errors.Trace(err)
	}

	newschld.SetSchedulerId(tmpschld.Id)
	newschld.SetSchedulerIntervalMs(tmpschld.IntervalMs)
	newschld.SetSchedulerActive(tmpschld.Active)
	newschld.SetSchedulerArg1(tmpschld.Arg1)
	newschld.SetSchedulerArg2(tmpschld.Arg2)
	newschld.SetSchedulerArg3(tmpschld.Arg3)
	newschld.SetSchedulerArg4(tmpschld.Arg4)
	newschld.SetSchedulerArg5(tmpschld.Arg5)

	err = newschld.UpdateOneSchedulerInfo(db)
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
func DeleteOneSchld(etcdcli *EtcdCli) error {

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
	//var tmpschld proxysql.Schedulers
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	//value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	//if err := json.Unmarshal(value, &tmpschld); err != nil {
	//	return errors.Trace(err)
	//}

	schld_id, _ := strconv.Atoi(string(key))

	// new user handler
	newschld, err := proxysql.NewSch("ls", 1)
	if err != nil {
		return errors.Trace(err)
	}

	newschld.SetSchedulerId(int64(schld_id))

	err = newschld.DeleteOneScheduler(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
