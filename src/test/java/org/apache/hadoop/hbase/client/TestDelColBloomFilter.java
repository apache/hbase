package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TestStoreFile;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestDelColBloomFilter {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] TABLE = Bytes.toBytes("mytable");
  private HBaseTestingUtility util = new HBaseTestingUtility();
  private final String DIR = util.getTestDir() + "/TestHRegion/";
  private static int SLAVES = 3;
  public static AtomicInteger delBloomData = new AtomicInteger(0);

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   *
   * @param numPuts - the number of puts we will insert
   * @param numDeletes - number of deletes we will insert
   * @param delColBloomFilterEnabled - will the delete col bloom filter be
   * enabled
   * @param toRead - whether we want to read the del bloom filter info
   * @throws Exception
   */
  private void testDelColBloomFilter(int numPuts, int numDeletes,
      boolean delColBloomFilterEnabled, boolean toRead) throws Exception {
    util.startMiniCluster();
    try {
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      htd.addFamily(new HColumnDescriptor(FAMILY));

      HRegionInfo info = new HRegionInfo(htd, null, null, false);
      Path path = new Path(DIR);
      HRegion region = HRegion.createHRegion(info, path,
          util.getConfiguration());
      for (int i = 0; i < numPuts; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(FAMILY, QUALIFIER, Bytes.toBytes("value"));
        region.put(put);
      }
      for (int i = 0; i < numDeletes; i++) {
        Delete del = new Delete(Bytes.toBytes("row" + i));
        del.deleteColumns(FAMILY, QUALIFIER);
        region.delete(del, null, true);
      }
      region.flushcache();
      int collectedDeletes = 0;
      // if we don't want to read it but we wrote it
      if (!toRead && delColBloomFilterEnabled) {
        util.getConfiguration().setBoolean(
            BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false);
        delBloomData.set(0);
      }
      // we need to close the region in order to actually open it again (so that
      // the new config will take effect (if there is a config change)
      region.close();
      HRegion regionNew = HRegion.openHRegion(info, path, null,
          util.getConfiguration());
      List<StoreFile> files = TestStoreFile.getStoreFiles(regionNew
          .getStore(FAMILY));
      for (StoreFile file : files) {
        file.closeReader(false);
        StoreFile.Reader reader = file.createReader();
        Reader hfileReader = reader.getHFileReader();
        if (!toRead && delColBloomFilterEnabled) {
          Assert.assertEquals(0, delBloomData.intValue());
        } else {
          DataInput data = hfileReader.getDeleteColumnBloomFilterMetadata();
          // if there are no deletes we should not get any metadata for the del
          // col bloom filter
          if (numDeletes > 0) {
            // if the delete col bloom filter is disabled we should not get any
            // metadata
            if (!delColBloomFilterEnabled) {
              Assert.assertEquals(null, data);
            } else {
              // if the number of deletes > 0 and delete col bloom filter is
              // disabled, check if metadata is not null
              Assert.assertTrue(data != null);
            }
          } else {
            Assert.assertEquals(null, data);
          }
          collectedDeletes += reader.getDeleteColumnCnt();
        }
        if (toRead) {
          Assert.assertEquals(numDeletes, collectedDeletes);
        }
      }
    } finally {
      util.shutdownMiniCluster();
    }
  }

  /**
   * This will run different tests of the delete bloom filter, we are testing
   * whether we read/write the del col bloom filter when it is enabled/disabled
   * by conf
   *
   * @throws Exception
   */
  @Test
  public void testWithDifferentConfiguraions() throws Exception {
    util.getConfiguration().setBoolean(
        BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, true);
    // we enable the delete bloom filter on write and we want to read it
    testDelColBloomFilter(1000, 100, true, true);
    util.getConfiguration().setBoolean(
        BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false);
    // we disable the delete bloom filter on write and we do not want to read it
    testDelColBloomFilter(1000, 100, false, false);
    util.getConfiguration().setBoolean(
        BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, true);
    // we enable the delete bloom filter on write and we do not want to read it
    testDelColBloomFilter(1000, 100, true, false);
  }
}
