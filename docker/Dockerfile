FROM golang:1.9
MAINTAINER Lei Tian

WORKDIR /go/src

RUN \
    mkdir -p github.com/imSQL && \
    go get -u github.com/coreos/etcd/clientv3 && \
    go get -u github.com/coreos/etcd/mvcc/mvccpb && \
    go get -u github.com/imSQL/proxysql && \
    git clone https://github.com/imSQL/proxysql_etcd.git github.com/imSQL/proxysql_etcd && \
    cd /go/src/github.com/imSQL && \
    go install ./proxysql_etcd && \
    rm -fr /go/src/github.com/imSQL/proxysql_etcd

#EXPOSE 3334


ENTRYPOINT ["/go/bin/proxysql_etcd"]
