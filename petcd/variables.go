package petcd

import (
	"encoding/base64"

	"github.com/imSQL/proxysql"
	"github.com/juju/errors"
)

// update a proxysql mysql_users information.
// update successed return nil,else return error
func UpdateOneVars(etcdcli *EtcdCli) error {

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
	var tmpvrs proxysql.Variables
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	//update on variable.
	err = proxysql.UpdateOneConfig(db, string(key), string(value))
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
