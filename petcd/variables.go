package petcd

import (
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/imSQL/proxysql"
)

func UpdateOneVar(ev *clientv3.Event) {
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

	var tmpsrv proxysql.Servers
	if err := json.Unmarshal(ev.Kv.Value, &tmpsrv); err != nil {
		fmt.Println(err)
	}

	newsrv, err := proxysql.NewServer(tmpsrv.HostGroupId, tmpsrv.HostName, tmpsrv.Port)
	if err != nil {
		fmt.Println(err)
	}

	newsrv.SetServerMaxConnection(tmpsrv.MaxConnections)

	err = newsrv.UpdateOneServerInfo(db)
	if err != nil {
		fmt.Println(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		fmt.Println(err)
	}
}
