# Generate Thrift IDL files for Swift annotated classes
if [ $# -eq 0 ]
then
  echo "Usage"
  echo "-----"
  echo "./gen_thrift_from_swift <space separated list of HBase, Hadoop and swift2thrift jar paths>"
  exit
fi
export CLASSPATH=$CLASSPATH:$1:$2:$3


dirpath=`dirname $0`

$JAVA_HOME/bin/java com.facebook.swift.generator.swift2thrift.Main -allow_multiple_packages org.apache.hadoop.hbase org.apache.hadoop.hbase.KeyValue org.apache.hadoop.hbase.client.Put org.apache.hadoop.hbase.io.TimeRange org.apache.hadoop.hbase.filter.TFilter org.apache.hadoop.hbase.client.Get org.apache.hadoop.hbase.client.MultiPut org.apache.hadoop.hbase.client.Delete org.apache.hadoop.hbase.client.Scan org.apache.hadoop.hbase.HColumnDescriptor org.apache.hadoop.hbase.HTableDescriptor org.apache.hadoop.hbase.HRegionInfo org.apache.hadoop.hbase.client.MultiPutResponse org.apache.hadoop.hbase.client.Result org.apache.hadoop.hbase.HServerAddress 'org.apache.hadoop.hbase.HServerLoad$RegionLoad' org.apache.hadoop.hbase.HServerLoad org.apache.hadoop.hbase.HServerInfo org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException org.apache.hadoop.hbase.client.MultiAction 'org.apache.hadoop.hbase.client.IntegerOrResultOrException$Type' org.apache.hadoop.hbase.client.IntegerOrResultOrException org.apache.hadoop.hbase.client.TMultiResponse org.apache.hadoop.hbase.client.TRowMutations org.apache.hadoop.hbase.master.AssignmentPlan org.apache.hadoop.hbase.client.RowLock 'org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram$HFileStat' 'org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram$Bucket' org.apache.hadoop.hbase.HRegionLocation org.apache.hadoop.hbase.ipc.ScannerResult org.apache.hadoop.hbase.ipc.ThriftHRegionInterface -out "$dirpath/HBase.thrift" -namespace cpp facebook.hbase.hbcpp -namespace php hbase
