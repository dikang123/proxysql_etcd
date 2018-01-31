package petcd

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
)

type (
	EtcdCli struct {
		//etcd connection informations
		DialTimeout    time.Duration
		RequestTimeout time.Duration
		EndPoints      []string

		//clientv3.Client
		etcdv3 *clientv3.Client

		//watch path.
		// sucs as :  /database/parauser
		// database is prefix
		// parauser is service
		Prefix  string
		Service string

		//proxysql connection informations.
		ProxySQLAddr  string
		ProxySQLPort  uint64
		ProxySQLAdmin string
		ProxySQLPass  string

		//error
		Err error
	}
)

func NewEtcdCli(endpoints []string) *EtcdCli {
	var etcdcli *EtcdCli

	etcdcli.DialTimeout = 5 * time.Second
	etcdcli.RequestTimeout = 3 * time.Second

	etcdcli.EndPoints = endpoints

	etcdcli.Prefix = "/database"
	etcdcli.Service = "users"

	etcdcli.ProxySQLAddr = "172.18.10.136"
	etcdcli.ProxySQLPort = 13306
	etcdcli.ProxySQLAdmin = "admin"
	etcdcli.ProxySQLPass = "admin"

	return etcdcli
}

func (cli *EtcdCli) SetDilTimeout(num uint64) {

	cli.DialTimeout = 5 * time.Second
}

func (cli *EtcdCli) SetRequestTimeout(num uint64) {
	cli.RequestTimeout = 5 * time.Second
}

func (cli *EtcdCli) SetPrefix(prefix string) {
	cli.Prefix = prefix
}

func (cli *EtcdCli) SetService(service string) {
	cli.Service = service
}

func (cli *EtcdCli) OpenEtcd() error {
	cli.etcdv3, cli.Err = clientv3.New(clientv3.Config{
		Endpoints:   cli.EndPoints,
		DialTimeout: cli.DialTimeout,
	})
	if cli.Err != nil {
		return errors.Trace(cli.Err)
	}

	return nil
}

func (cli *EtcdCli) CloseEtcd() {
	cli.etcdv3.Close()
}
