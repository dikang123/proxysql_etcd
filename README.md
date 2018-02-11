# proxysql_etcd
-----

#### 1.Introduce
-----

A proxysql distributed tools.

#### 2.Requirements
-----

1. Go 1.7 +
1. ProxySQL 1.3.x
1. github.com/coreos/etcd/clientv3
1. github.com/coreos/etcd/mvcc/mvccpb
1. github.com/imSQL/proxysql

#### 3.Installation
-----

Simple install the package to you $GOPATH with the go tool from shell:

    # go get -u github.com/coreos/etcd/clientv3
    # go get -u github.com/coreos/etcd/mvcc/mvccpb
    # go get -u github.com/imSQL/proxysql
    # go get -u github.com/imSQL/proxysql_etcd
    

Make sure git command is installed on your OS.

#### 4. Startup proxysql_etcd
-----
    # ./startup_proxysql_etcd.sh
    
#### 5.Execute Test
-----

Execute follow command:

    # ./startup_test.sh



### Donate

-----

If you like the project and want to buy me a cola, you can through:

| PayPal                                                                                                               | 微信                                                                 |
| -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| [![](https://www.paypalobjects.com/webstatic/paypalme/images/pp_logo_small.png)](https://www.paypal.me/taylor840326) | ![](https://github.com/taylor840326/blog/raw/master/imgs/weixin.png) |

