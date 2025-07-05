##
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
##

# Shared function to wait for a process end. Take the pid and the command name as parameters
waitForProcessEnd() {
  local pid_killed=$1
  local command=$2
  local proc_keyword="proc_$command"
  local processed_at=`date +%s`
  while is_process_alive $pid_killed $proc_keyword;
   do
     echo -n "."
     sleep 1;
     # if process persists more than $HBASE_STOP_TIMEOUT (default 1200 sec) no mercy
     if [ $(( `date +%s` - $processed_at )) -gt ${HBASE_STOP_TIMEOUT:-1200} ]; then
       break;
     fi
   done
  # process still there : kill -9
  if is_process_alive $pid_killed $proc_keyword; then
    echo -n force stopping $command with kill -9 $pid_killed
    $JAVA_HOME/bin/jstack -l $pid_killed > "$logout" 2>&1
    kill -9 $pid_killed > /dev/null 2>&1
  fi
  # Add a CR after we're done w/ dots.
  echo
}

# shared function to check whether a process is still alive
is_process_alive() {
  local pid=$1
  local keyword=$2
  # check whether /proc/$pid exists
  if [[ ! -d "/proc/$pid" ]]; then
    return 1
  fi

  # get the command line of the process
  local cmdline
  cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)

  # check whether the command line contains the given keyword
  if [[ "$cmdline" == *"$keyword"* ]]; then
    return 0
  else
    return 1
  fi
}
