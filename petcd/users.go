package petcd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/proxysql"
)

func CreateOneUser(ev *clientv3.Event, etcdcli *EtcdCli) {
	fmt.Printf("Create %q : %q\n", ev.Kv.Key, ev.Kv.Value)
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		fmt.Println(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	db, err := conn.OpenConn()
	if err != nil {
		fmt.Println(err)
	}

	var tmpusr proxysql.Users
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	err = json.Unmarshal(value, &tmpusr)

	if err := json.Unmarshal(value, &tmpusr); err != nil {
		fmt.Println(err)
	}
	//tmpusr.Username = node[4]

	newuser, err := proxysql.NewUser(string(key), tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	newuser.SetUserActive(1)

	err = newuser.AddOneUser(db)
	if err != nil {
		fmt.Println(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		fmt.Println(err)
	}
}

func UpdateOneUser(ev *clientv3.Event, etcdcli *EtcdCli) {
	fmt.Printf("Update %q : %q\n", ev.Kv.Key, ev.Kv.Value)
	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		fmt.Println(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	db, err := conn.OpenConn()
	if err != nil {
		fmt.Println(err)
	}

	var tmpusr proxysql.Users
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	err = json.Unmarshal(value, &tmpusr)

	if err := json.Unmarshal(value, &tmpusr); err != nil {
		fmt.Println("err->:", err)
	}

	newuser, err := proxysql.NewUser(string(key), tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	newuser.SetUserActive(1)

	err = newuser.UpdateOneUserInfo(db)
	if err != nil {
		fmt.Println(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		fmt.Println(err)
	}
}

func DeleteOneUser(ev *clientv3.Event, etcdcli *EtcdCli) {
	fmt.Printf("Delete %q \n", ev.Kv.Key)

	conn, err := proxysql.NewConn(etcdcli.ProxySQLAddr, etcdcli.ProxySQLPort, etcdcli.ProxySQLAdmin, etcdcli.ProxySQLPass)
	if err != nil {
		fmt.Println(err)
	}
	conn.SetCharset("utf8")
	conn.SetCollation("utf8_general_ci")
	conn.MakeDBI()

	db, err := conn.OpenConn()
	if err != nil {
		fmt.Println(err)
	}

	var tmpusr proxysql.Users
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	err = json.Unmarshal(value, &tmpusr)

	newuser, err := proxysql.NewUser(string(key), tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	err = newuser.DeleteOneUser(db)
	if err != nil {
		fmt.Println(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		fmt.Println(err)
	}
}
