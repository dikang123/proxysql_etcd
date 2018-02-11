#########################################################################
# File Name: startup_test.sh
# Author: Lei Tian
# mail: taylor840326@gmail.com
# Created Time: 2018-02-06 17:20
#########################################################################
#!/bin/bash

go test -timeout 30m variables_test.go --args -addr 172.18.10.136:2379 -prefix database -service parauser 

