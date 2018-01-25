#!/bin/bash

set -xeou pipefail

readonly ENDPOINT="172.18.10.136:2379"

export ETCDCTL_API=3

function cmd() {
	# put/get/del
	local METHOD=$1
	# username
	local USERNAME=$2
	etcdctl "$METHOD" --endpoints="$ENDPOINT" /database/parauser/users/$USERNAME $3
}


COUNT=0
while [ 1 ];
do
	BASEUSER="parauser"
	cmd put "$BASEUSER"_"$COUNT" "{\"username\":\"$BASEUSER"_"$COUNT\",\"password\":\"111111\"}"
	cmd put "$BASEUSER"_"$COUNT" "{\"username\":\"$BASEUSER"_"$COUNT\",\"password\":\"111111\"}"
	cmd del "$BASEUSER"_"$COUNT" ""
	COUNT=$(( $COUNT+1 ))
done

