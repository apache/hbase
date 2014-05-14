package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestTableInputFormatWithHFileOutputFormat {
  static final Log LOG = LogFactory.getLog(TestTableInputFormatScan.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static final String TABLENAME = "testMultipleHLogs";
  static final byte[] CF1 = Bytes.toBytes("cf1");
  static final byte[] CF2 = Bytes.toBytes("cf2");
  static final byte[] CF3 = Bytes.toBytes("cf3");
  static final byte[][] FAMILIES = new byte[][]{CF1, CF2, CF3};
  static final int REGION_NUM = 20;
  static final byte[] QUALIFIER = Bytes.toBytes("q");
  static final byte[] VALUE = Bytes.toBytes("v");
  static final Path OUTPUTPATH = new Path("TEST-OUTPUT");
  static HTable htable;
  static Configuration conf = TEST_UTIL.getConfiguration();


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
    
  }
  
  @Test
  public void testTableInputFormatWithHFileOutputFormat() throws IOException,
  InterruptedException, ClassNotFoundException {
    // Create table and load some data into CF1
    loadTable();
    
    // Run a MapReduce with TableInputFormat and HFileOutputFormat in order to 
    // put all the kv from CF1 to CF2
    launchMRJob();
    
    // Bulk upload the MR output file into HBase
    new LoadIncrementalHFiles(conf).doBulkLoad(OUTPUTPATH, htable);
    
    // Verify the table that all the rows have the same data for both CF1 and CF2
    verifyTable();
  }
  
  private void loadTable() throws IOException{
    final int actualStartKey = 0;
    final int actualEndKey = Integer.MAX_VALUE;
    final int keysPerRegion = (actualEndKey - actualStartKey) / REGION_NUM;
    final int splitStartKey = actualStartKey + keysPerRegion;
    final int splitEndKey = actualEndKey - keysPerRegion;
    final String keyFormat = "%08x";
    htable = TEST_UTIL.createTable(Bytes.toBytes(TABLENAME),
        FAMILIES,
        1,
        Bytes.toBytes(String.format(keyFormat, splitStartKey)),
        Bytes.toBytes(String.format(keyFormat, splitEndKey)),
        REGION_NUM);
    
    // Put some data for each Region
    for (byte[] row : htable.getStartKeys()) {
      Put p = new Put(row);
      p.add(CF1, QUALIFIER, VALUE);
      // We would delete the kv for CF3, to verify that Deletes work with
      // HFileOutputFormat
      p.add(CF3, QUALIFIER, VALUE);
      htable.put(p);
      htable.flushCommits();
    }
  }

  private void launchMRJob() throws IOException, InterruptedException, ClassNotFoundException {
    // Create the scan object
    Scan scan = new Scan();
    scan.addFamily(CF1);
    scan.addFamily(CF3);

    // Create and initialize the MR job
    Job job = new Job(conf, "process column contents");
    FileOutputFormat.setOutputPath(job, OUTPUTPATH);
    
    TableMapReduceUtil.initHTableInputAndHFileOutputMapperJob(
        TABLENAME, 
        scan, 
        TestTableInputFormatWithHFileOutputFormat.DummyMapper.class, 
        job);

    // Wait for job completion
    job.waitForCompletion(true);
    
    // Sanity check the output file
    FileStatus[] files = TEST_UTIL.getDFSCluster().getFileSystem().listStatus(
        new Path(OUTPUTPATH, Bytes.toString(CF2)));
    for(FileStatus file : files) {
      System.out.println(file.getPath());
    }
    Assert.assertEquals(REGION_NUM, files.length);
    LOG.info("After map/reduce completion");
  }
  
  private void verifyTable() throws IOException {
    Scan scan = new Scan();
    scan.addFamily(CF1);
    scan.addFamily(CF2);
    scan.addFamily(CF3);
    
    ResultScanner s = htable.getScanner(scan);
    Result result = null;
    int count = 0;
    while((result = s.next()) != null) {
      count++;

      // We should only see CF1 and CF2. And not see CF3 since, we also added
      // Delete kvs in the MR job.
      Assert.assertEquals(2, result.list().size());
      KeyValue kvFromCF1 = result.list().get(0);
      KeyValue kvFromCF2 = result.list().get(1);

      Assert.assertTrue(Bytes.compareTo(kvFromCF1.getFamily(), CF1) == 0);
      Assert.assertTrue(Bytes.compareTo(kvFromCF2.getFamily(), CF2) == 0);

      Assert.assertTrue(Bytes.compareTo(kvFromCF1.getRow(), kvFromCF2.getRow()) == 0);
      Assert.assertTrue(Bytes.compareTo(kvFromCF1.getQualifier(), kvFromCF2.getQualifier()) == 0);
      Assert.assertTrue(kvFromCF1.getTimestamp() == kvFromCF2.getTimestamp());
      Assert.assertTrue(Bytes.compareTo(kvFromCF1.getValue(), kvFromCF2.getValue()) == 0);
    }
    
    Assert.assertEquals(REGION_NUM, count);
  }
  
  public static class DummyMapper extends TableMapper<ImmutableBytesWritable, KeyValue> {
    private KeyValue previousKV = null;
    public void map(ImmutableBytesWritable key, Result result, Context context)
    throws IOException, InterruptedException {
      Assert.assertEquals(2, result.size());
      KeyValue tmp = result.list().get(0);
      KeyValue currentKV = new KeyValue(tmp.getRow(), CF2, tmp.getQualifier(), tmp.getTimestamp(), tmp.getValue());

      KeyValue tmp2 = result.list().get(1);
      KeyValue deleteKV = new KeyValue(tmp2.getRow(), CF3, tmp2.getQualifier(), tmp.getTimestamp(), KeyValue.Type.Delete);

      // Sanity check that the output key value is sorted
      if (previousKV != null) {
        Assert.assertTrue(KeyValue.COMPARATOR.compare(currentKV, previousKV) >= 0);
      }
      previousKV = currentKV;
      System.out.println("current KV: " + Bytes.toStringBinary(currentKV.getBuffer()));
      context.write(key, currentKV);
      context.write(key, deleteKV);
    }
  }
}
