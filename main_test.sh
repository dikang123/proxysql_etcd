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

function delcmd() {
	# put/get/del
	local METHOD=$1
	# username
	local USERNAME=$2
	etcdctl "$METHOD" --endpoints="$ENDPOINT" /database/parauser/users/$USERNAME
}

COUNT=0
while [ 1 ];
do
	BASEUSER="parauser"
	cmd put "cmVsYXRpb25fc2VydmljZQ==" "eyJ1c2VybmFtZSI6InJlbGF0aW9uX3NlcnZpY2UiLCJwYXNzd29yZCI6IjExMTExMSIsImFjdGl2ZSI6MSwidXNlX3NzbCI6MCwiZGVmYXVsdF9ob3N0Z3JvdXAiOjAsImRlZmF1bHRfc2NoZW1hIjoicmVsYXRpb25kYiIsInNjaGVtYV9sb2NrZWQiOjAsInRyYW5zYWN0aW9uX3BlcnNpc3RlbnQiOjAsImZhc3RfZm9yd2FyZCI6MCwiYmFja2VuZCI6MSwiZnJvbnRlbmQiOjEsIm1heF9jb25uZWN0aW9ucyI6MTAwMDB9"
	cmd put "cmVsYXRpb25fc2VydmljZQ==" "eyJ1c2VybmFtZSI6InJlbGF0aW9uX3NlcnZpY2UiLCJwYXNzd29yZCI6IjExMTExMSIsImFjdGl2ZSI6MSwidXNlX3NzbCI6MCwiZGVmYXVsdF9ob3N0Z3JvdXAiOjAsImRlZmF1bHRfc2NoZW1hIjoicmVsYXRpb25kYiIsInNjaGVtYV9sb2NrZWQiOjAsInRyYW5zYWN0aW9uX3BlcnNpc3RlbnQiOjAsImZhc3RfZm9yd2FyZCI6MCwiYmFja2VuZCI6MSwiZnJvbnRlbmQiOjEsIm1heF9jb25uZWN0aW9ucyI6MTAwMDB9"
	delcmd del "cmVsYXRpb25fc2VydmljZQ==" 
	COUNT=$(( $COUNT+1 ))
	SLEEP_TIMEOUT=` echo "$COUNT % 10" |bc`
	if [ $SLEEP_TIMEOUT -eq 0 ];then
		sleep 10
	fi
done

