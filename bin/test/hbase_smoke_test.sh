#!/usr/bin/env bash

# This check includes - hbck, ltt, itbll, canary. Can be improved further.

usage="Usage: hbase_smoke_test.sh (--help|--testAll|--testReadWrite|--testRegions|--testItbll|--testCanaryRs|--testCanaryZk)
        \n\nKerberos Authentication is required for this script to run.\n"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HBASE_HOME, etc.
. "$bin"/../hbase-config.sh

case $1 in
  --testAll|--testReadWrite|--testRegions|--testItbll|--testCanaryRs|--testCanaryZk|--help)
    accept="yes" ;;
  *) ;;
esac

if [ $# -ne 1 -o "$accept" = "" ]; then
  echo -e $usage
  exit 1;
fi

test_ltt(){
ltt_write_o=`hbase ltt -write 3:1024 -num_keys 100000 -tn script1 2>&1`
ltt_write_exit_code=$?
if [ $ltt_write_exit_code -eq 0 ]; then
  echo "Write Successful"
else
  echo $ltt_write_o
  echo "Write Failed"
  exit 1;
fi

ltt_read_o=`hbase ltt -read 95 -num_keys 100000 -tn script1 2>&1`
ltt_read_exit_code=$?
if [ $ltt_read_exit_code -eq 0 ]; then
  echo "Read Successful"
else
  echo $ltt_read_o
  echo "Read Failed"
  exit 1;
fi
}


test_hbck(){
hbck_output=`hbase hbck 2>&1`
hbck_exit_code=$?
if [ $hbck_exit_code -eq 0 ]; then
  declare -a hbck_expected_output=("Status: OK", "0 inconsistencies detected", "hbase:meta is okay", "hbase:namespace is okay")
  IFS=','
  for txt in ${hbck_expected_output[@]}
  do
    if ! (grep -q "$txt" <<< "$hbck_output"); then
      echo "Hbck failed at $txt"
      exit 2;
    fi
  done
else
  echo $hbck_output
  echo "HBCK Failed"
  exit 1;
fi
echo "Regions consistent and hbase:meta okay"
}

test_itbll(){
itbll_output=`hbase org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList loop 5 5 5000 . 5 1000 5 2>&1`
itbll_exit_code=$?
if [ $itbll_exit_code -eq 0 ]; then
  cnt=`grep -c "Verify finished with success" <<< "$itbll_output"`
  if [ "$cnt" = 5 ]; then
    echo "ITBLL successful"
  else
    echo "$itbll_output"
    echo "ITBLL failed"
    exit 2;
  fi
else
  echo "$itbll_output"
  echo "ITBLL failed"
  exit 1;
fi
itbll_clean=`hbase org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList clean . 2>&1`
}

test_canary_rs(){
canary_rs_output=`hbase canary -regionserver -allRegions 2>&1`
canary_rs_exit_code=$?
if [ $canary_rs_exit_code -eq 0 ]; then
  echo "HBase Canary - regionserver mode successful."
else
  echo $canary_rs_output
  echo "HBase canary - regionserver mode failed"
  exit 1;
fi
}

test_canary_zk(){
canary_zk_output=`hbase canary -zookeeper 2>&1`
canary_zk_exit_code=$?
if [ $canary_zk_exit_code -eq 0 ]; then
  echo "HBase Canary - zookeeper mode successful."
else
  echo $canary_zk_output
  echo "HBase canary - zookeeper mode failed"
  exit 1;
fi
}

run_health_check(){
  case $1 in
  --help)
    echo -e $usage;
    ;;
  --testRegions)
    test_hbck;
    ;;
  --testReadWrite)
    test_ltt;
    ;;
  --testItbll)
    test_itbll;
    ;;
  --testCanaryRs)
    test_canary_rs;
    ;;
  --testCanaryZk)
    test_canary_zk;
    ;;
  --testAll)
    test_hbck;
    test_ltt;
    test_canary_rs;
    test_canary_zk;
    test_itbll;
    echo "HBASE IS HEALTHY"
    ;;
  *)
    ;;
  esac
}

run_health_check $1
