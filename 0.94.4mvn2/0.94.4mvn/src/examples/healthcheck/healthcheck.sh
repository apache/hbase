#!/bin/bash 
 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 #
 #     http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.

 # This is an example script for checking health of a node ( master or region server). 
 # The health chore script should essentially output an message containing "ERROR" at an undesirable
 # outcome of the checks in the script. 

err=0;

function check_disks {

for m in `awk '$3~/ext3/ {printf" %s ",$2}' /etc/fstab` ; do
    fsdev=""
    fsdev=`awk -v m=$m '$2==m {print $1}' /proc/mounts`;
    if [ -z "$fsdev" ] ; then
      msg_="$msg_ $m(u)"
    else
      msg_="$msg_`awk -v m=$m '$2==m { if ( $4 ~ /^ro,/ ) {printf"%s(ro)",$2 } ; }' /proc/mounts`"
    fi
  done

  if [ -z "$msg_" ] ; then
    echo "disks ok" ; exit 0
  else
    echo "$msg_" ; exit 2
  fi

}

function check_link {
  /usr/bin/snmpwalk -t 5 -Oe  -Oq  -Os -v 1 -c public localhost if | \
        awk ' { 
          split($1,a,".") ;
          if ( a[1] == "ifIndex" ) { ifIndex[a[2]] = $2 }
          if ( a[1] == "ifDescr" ) { ifDescr[a[2]] = $2 }
          if ( a[1] == "ifType" ) { ifType[a[2]] = $2 }
          if ( a[1] == "ifSpeed" ) { ifSpeed[a[2]] = $2 }
          if ( a[1] == "ifAdminStatus" ) { ifAdminStatus[a[2]] = $2 }
          if ( a[1] == "ifOperStatus" ) { ifOperStatus[a[2]] = $2 }
        }
        END {
        up=0;
        for (i in ifIndex ) {
          if ( ifType[i] == 6 && ifAdminStatus[i] == 1 && ifOperStatus[i] == 1 && ifSpeed[i] == 1000000000 ) {
            up=i;
          }
        }
        if ( up == 0 ) { print "check link" ; exit 2 }
        else { print ifDescr[up],"ok" }
        }'
  exit $? ;
}

for check in disks link ; do
  msg=`check_${check}` ;
  if [ $? -eq 0 ] ; then
    ok_msg="$ok_msg$msg,"
  else
    err_msg="$err_msg$msg,"
  fi
done

if [ ! -z "$err_msg" ] ; then
  echo -n "ERROR $err_msg " 
fi
if [ ! -z "$ok_msg" ] ; then
  echo -n "OK: $ok_msg" 
fi
echo
exit 0
