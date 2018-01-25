package prxetcd

import (
	"encoding/json"
	"fmt"

	"github.com/imSQL/proxysql"
)

func CreateOneUser() {
	fmt.Printf("Create %q : %q\n", ev.Kv.Key, ev.Kv.Value)
	conn, err := proxysql.NewConn("172.18.10.136", 13306, "admin", "admin")
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
	if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
		fmt.Println(err)
	}
	//tmpusr.Username = node[4]

	newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	newuser.SetUserActive(1)

	err = newuser.AddOneUser(db)
	if err != nil {
		fmt.Println(err)
	}
}

func UpdateOneUser() {
	fmt.Printf("Update %q : %q\n", ev.Kv.Key, ev.Kv.Value)
	conn, err := proxysql.NewConn("172.18.10.136", 13306, "admin", "admin")
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
	if err := json.Unmarshal(ev.Kv.Value, &tmpusr); err != nil {
		fmt.Println(err)
	}

	newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	newuser.SetUserActive(1)

	err = newuser.UpdateOneUserInfo(db)
	if err != nil {
		fmt.Println(err)
	}
}

func DeleteOneUser() {
	fmt.Printf("Delete %q \n", ev.Kv.Key)

	conn, err := proxysql.NewConn("172.18.10.136", 13306, "admin", "admin")
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
	tmpusr.Username = node[4]

	newuser, err := proxysql.NewUser(tmpusr.Username, tmpusr.Password, 0, tmpusr.Username)
	if err != nil {
		fmt.Println(err)
	}

	err = newuser.DeleteOneUser(db)
	if err != nil {
		fmt.Println(err)
	}
}
