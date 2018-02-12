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
func SyncQrToProxy(etcdcli *EtcdCli, cli *clientv3.Client) error {

	// get value from etcd
	ctx, cancel := context.WithTimeout(context.Background(), etcdcli.RequestTimeout)
	resp, err := cli.Get(ctx, etcdcli.Root+"/queryrules", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
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
		var tmpqr proxysql.QueryRules
		// key is username ,like user01
		// value is proxysql.Users []byte type.
		//key, _ := base64.StdEncoding.DecodeString(string(evs.Key))
		value, _ := base64.StdEncoding.DecodeString(string(evs.Value))

		// []byte to proxysql.Users struct.
		if err := json.Unmarshal(value, &tmpqr); err != nil {
			return errors.Trace(err)
		}

		// new user handler
		newqr, err := proxysql.NewQr(tmpqr.Username)
		if err != nil {
			return errors.Trace(err)
		}

		newqr.SetQrRuleid(tmpqr.Rule_id)
		newqr.SetQrProxyAddr(tmpqr.Proxy_addr)
		newqr.SetProxyPort(tmpqr.Proxy_port)
		newqr.SetQrActive(tmpqr.Active)
		newqr.SetQrApply(tmpqr.Apply)
		newqr.SetQrCacheTTL(tmpqr.Cache_ttl)
		newqr.SetQrClientAddr(tmpqr.Client_addr)
		newqr.SetQrDelay(tmpqr.Delay)
		newqr.SetQrDestHostGroup(tmpqr.Destination_hostgroup)
		newqr.SetQrDigest(tmpqr.Digest)
		newqr.SetQrErrorMsg(tmpqr.Error_msg)
		newqr.SetQrFlagIN(tmpqr.FlagIN)
		newqr.SetQrFlagOut(tmpqr.FlagOUT)
		newqr.SetQrLog(tmpqr.Log)
		newqr.SetQrMatchDigest(tmpqr.Match_digest)
		newqr.SetQrMatchPattern(tmpqr.Match_pattern)
		newqr.SetQrMirrorFlagOUT(tmpqr.Mirror_flagOUT)
		newqr.SetQrMirrorHostgroup(tmpqr.Mirror_hostgroup)
		newqr.SetQrNegateMatchPattern(tmpqr.Negate_match_pattern)
		newqr.SetQrReconnect(tmpqr.Reconnect)
		newqr.SetQrReplacePattern(tmpqr.Replace_pattern)
		newqr.SetQrRetries(tmpqr.Retries)
		newqr.SetQrSchemaname(tmpqr.Schemaname)
		newqr.SetQrTimeOut(tmpqr.Timeout)

		err = newqr.AddOneQr(db)
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
func CreateOneQr(etcdcli *EtcdCli) error {

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
	var tmpqr proxysql.QueryRules
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpqr); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newqr, err := proxysql.NewQr(tmpqr.Username)
	if err != nil {
		return errors.Trace(err)
	}

	newqr.SetQrRuleid(tmpqr.Rule_id)
	newqr.SetQrProxyAddr(tmpqr.Proxy_addr)
	newqr.SetProxyPort(tmpqr.Proxy_port)
	newqr.SetQrActive(tmpqr.Active)
	newqr.SetQrApply(tmpqr.Apply)
	newqr.SetQrCacheTTL(tmpqr.Cache_ttl)
	newqr.SetQrClientAddr(tmpqr.Client_addr)
	newqr.SetQrDelay(tmpqr.Delay)
	newqr.SetQrDestHostGroup(tmpqr.Destination_hostgroup)
	newqr.SetQrDigest(tmpqr.Digest)
	newqr.SetQrErrorMsg(tmpqr.Error_msg)
	newqr.SetQrFlagIN(tmpqr.FlagIN)
	newqr.SetQrFlagOut(tmpqr.FlagOUT)
	newqr.SetQrLog(tmpqr.Log)
	newqr.SetQrMatchDigest(tmpqr.Match_digest)
	newqr.SetQrMatchPattern(tmpqr.Match_pattern)
	newqr.SetQrMirrorFlagOUT(tmpqr.Mirror_flagOUT)
	newqr.SetQrMirrorHostgroup(tmpqr.Mirror_hostgroup)
	newqr.SetQrNegateMatchPattern(tmpqr.Negate_match_pattern)
	newqr.SetQrReconnect(tmpqr.Reconnect)
	newqr.SetQrReplacePattern(tmpqr.Replace_pattern)
	newqr.SetQrRetries(tmpqr.Retries)
	newqr.SetQrSchemaname(tmpqr.Schemaname)
	newqr.SetQrTimeOut(tmpqr.Timeout)

	err = newqr.AddOneQr(db)
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
func UpdateOneQr(etcdcli *EtcdCli) error {

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
	var tmpqr proxysql.QueryRules
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	//key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	// []byte to proxysql.Users struct.
	if err := json.Unmarshal(value, &tmpqr); err != nil {
		return errors.Trace(err)
	}

	// new user handler
	newqr, err := proxysql.NewQr(tmpqr.Username)
	if err != nil {
		return errors.Trace(err)
	}

	newqr.SetQrRuleid(tmpqr.Rule_id)
	newqr.SetQrProxyAddr(tmpqr.Proxy_addr)
	newqr.SetProxyPort(tmpqr.Proxy_port)
	newqr.SetQrActive(tmpqr.Active)
	newqr.SetQrApply(tmpqr.Apply)
	newqr.SetQrCacheTTL(tmpqr.Cache_ttl)
	newqr.SetQrClientAddr(tmpqr.Client_addr)
	newqr.SetQrDelay(tmpqr.Delay)
	newqr.SetQrDestHostGroup(tmpqr.Destination_hostgroup)
	newqr.SetQrDigest(tmpqr.Digest)
	newqr.SetQrErrorMsg(tmpqr.Error_msg)
	newqr.SetQrFlagIN(tmpqr.FlagIN)
	newqr.SetQrFlagOut(tmpqr.FlagOUT)
	newqr.SetQrLog(tmpqr.Log)
	newqr.SetQrMatchDigest(tmpqr.Match_digest)
	newqr.SetQrMatchPattern(tmpqr.Match_pattern)
	newqr.SetQrMirrorFlagOUT(tmpqr.Mirror_flagOUT)
	newqr.SetQrMirrorHostgroup(tmpqr.Mirror_hostgroup)
	newqr.SetQrNegateMatchPattern(tmpqr.Negate_match_pattern)
	newqr.SetQrReconnect(tmpqr.Reconnect)
	newqr.SetQrReplacePattern(tmpqr.Replace_pattern)
	newqr.SetQrRetries(tmpqr.Retries)
	newqr.SetQrSchemaname(tmpqr.Schemaname)
	newqr.SetQrTimeOut(tmpqr.Timeout)

	err = newqr.UpdateOneQrInfo(db)
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
func DeleteOneQr(etcdcli *EtcdCli) error {

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
	//var tmpqr proxysql.QueryRules
	// key is username ,like user01
	// value is proxysql.Users []byte type.
	key, _ := base64.StdEncoding.DecodeString(etcdcli.Key)
	//value, _ := base64.StdEncoding.DecodeString(etcdcli.Value)

	newqr, err := proxysql.NewQr("test")
	if err != nil {
		return errors.Trace(err)
	}

	rule_id, _ := strconv.Atoi(string(key))
	newqr.SetQrRuleid(uint64(rule_id))

	err = newqr.DeleteOneQr(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = conn.CloseConn(db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
