/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link MetaReader}, {@link MetaEditor}.
 */
@Category(MediumTests.class)
public class TestMetaReaderEditor {
  private static final Log LOG = LogFactory.getLog(TestMetaReaderEditor.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static ZooKeeperWatcher zkw;
  private static CatalogTracker CT;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }
    
    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  @BeforeClass public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(3);

    Configuration c = new Configuration(UTIL.getConfiguration());
    // Tests to 4 retries every 5 seconds. Make it try every 1 second so more
    // responsive.  1 second is default as is ten retries.
    c.setLong("hbase.client.pause", 1000);
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    zkw = new ZooKeeperWatcher(c, "TestMetaReaderEditor", ABORTABLE);
    CT = new CatalogTracker(zkw, c, ABORTABLE);
    CT.start();
  }

  @AfterClass public static void afterClass() throws Exception {
    ABORTABLE.abort("test ending", null);
    CT.stop();
    UTIL.shutdownMiniCluster();
  }

  /**
   * Does {@link MetaReader#getRegion(CatalogTracker, byte[])} and a write
   * against hbase:meta while its hosted server is restarted to prove our retrying
   * works.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test public void testRetrying()
  throws IOException, InterruptedException {
    final TableName name =
        TableName.valueOf("testRetrying");
    LOG.info("Started " + name);
    HTable t = UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    int regionCount = UTIL.createMultiRegions(t, HConstants.CATALOG_FAMILY);
    // Test it works getting a region from just made user table.
    final List<HRegionInfo> regions =
      testGettingTableRegions(CT, name, regionCount);
    MetaTask reader = new MetaTask(CT, "reader") {
      @Override
      void metaTask() throws Throwable {
        testGetRegion(this.ct, regions.get(0));
        LOG.info("Read " + regions.get(0).getEncodedName());
      }
    };
    MetaTask writer = new MetaTask(CT, "writer") {
      @Override
      void metaTask() throws Throwable {
        MetaEditor.addRegionToMeta(this.ct, regions.get(0));
        LOG.info("Wrote " + regions.get(0).getEncodedName());
      }
    };
    reader.start();
    writer.start();

    // We're gonna check how it takes. If it takes too long, we will consider
    //  it as a fail. We can't put that in the @Test tag as we want to close
    //  the threads nicely
    final long timeOut = 180000;
    long startTime = System.currentTimeMillis();

    try {
      // Make sure reader and writer are working.
      assertTrue(reader.isProgressing());
      assertTrue(writer.isProgressing());

      // Kill server hosting meta -- twice  . See if our reader/writer ride over the
      // meta moves.  They'll need to retry.
      for (int i = 0; i < 2; i++) {
        LOG.info("Restart=" + i);
        UTIL.ensureSomeRegionServersAvailable(2);
        int index = -1;
        do {
          index = UTIL.getMiniHBaseCluster().getServerWithMeta();
        } while (index == -1 &&
          startTime + timeOut < System.currentTimeMillis());

        if (index != -1){
          UTIL.getMiniHBaseCluster().abortRegionServer(index);
          UTIL.getMiniHBaseCluster().waitOnRegionServer(index);
        }
      }

      assertTrue("reader: " + reader.toString(), reader.isProgressing());
      assertTrue("writer: " + writer.toString(), writer.isProgressing());
    } catch (IOException e) {
      throw e;
    } finally {
      reader.stop = true;
      writer.stop = true;
      reader.join();
      writer.join();
      t.close();
    }
    long exeTime = System.currentTimeMillis() - startTime;
    assertTrue("Timeout: test took " + exeTime / 1000 + " sec", exeTime < timeOut);
  }

  /**
   * Thread that runs a MetaReader/MetaEditor task until asked stop.
   */
  abstract static class MetaTask extends Thread {
    boolean stop = false;
    int count = 0;
    Throwable t = null;
    final CatalogTracker ct;

    MetaTask(final CatalogTracker ct, final String name) {
      super(name);
      this.ct = ct;
    }

    @Override
    public void run() {
      try {
        while(!this.stop) {
          LOG.info("Before " + this.getName()+ ", count=" + this.count);
          metaTask();
          this.count += 1;
          LOG.info("After " + this.getName() + ", count=" + this.count);
          Thread.sleep(100);
        }
      } catch (Throwable t) {
        LOG.info(this.getName() + " failed", t);
        this.t = t;
      }
    }

    boolean isProgressing() throws InterruptedException {
      int currentCount = this.count;
      while(currentCount == this.count) {
        if (!isAlive()) return false;
        if (this.t != null) return false;
        Thread.sleep(10);
      }
      return true;
    }

    @Override
    public String toString() {
      return "count=" + this.count + ", t=" +
        (this.t == null? "null": this.t.toString());
    }

    abstract void metaTask() throws Throwable;
  }

  @Test public void testGetRegionsCatalogTables()
  throws IOException, InterruptedException {
    List<HRegionInfo> regions =
      MetaReader.getTableRegions(CT, TableName.META_TABLE_NAME);
    assertTrue(regions.size() >= 1);
    assertTrue(MetaReader.getTableRegionsAndLocations(CT,
      TableName.META_TABLE_NAME).size() >= 1);
  }

  @Test public void testTableExists() throws IOException {
    final TableName name =
        TableName.valueOf("testTableExists");
    assertFalse(MetaReader.tableExists(CT, name));
    UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    assertTrue(MetaReader.tableExists(CT, name));
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.disableTable(name);
    admin.deleteTable(name);
    assertFalse(MetaReader.tableExists(CT, name));
    assertTrue(MetaReader.tableExists(CT,
      TableName.META_TABLE_NAME));
  }

  @Test public void testGetRegion() throws IOException, InterruptedException {
    final String name = "testGetRegion";
    LOG.info("Started " + name);
    // Test get on non-existent region.
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(CT, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished " + name);
  }

  // Test for the optimization made in HBASE-3650
  @Test public void testScanMetaForTable()
  throws IOException, InterruptedException {
    final TableName name =
        TableName.valueOf("testScanMetaForTable");
    LOG.info("Started " + name);

    /** Create 2 tables
     - testScanMetaForTable
     - testScanMetaForTablf
    **/

    UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    // name that is +1 greater than the first one (e+1=f)
    TableName greaterName =
        TableName.valueOf("testScanMetaForTablf");
    UTIL.createTable(greaterName, HConstants.CATALOG_FAMILY);

    // Now make sure we only get the regions from 1 of the tables at a time

    assertEquals(1, MetaReader.getTableRegions(CT, name).size());
    assertEquals(1, MetaReader.getTableRegions(CT, greaterName).size());
  }

  private static List<HRegionInfo> testGettingTableRegions(final CatalogTracker ct,
      final TableName name, final int regionCount)
  throws IOException, InterruptedException {
    List<HRegionInfo> regions = MetaReader.getTableRegions(ct, name);
    assertEquals(regionCount, regions.size());
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(ct, regions.get(0).getRegionName());
    assertEquals(regions.get(0).getEncodedName(),
      pair.getFirst().getEncodedName());
    return regions;
  }

  private static void testGetRegion(final CatalogTracker ct,
      final HRegionInfo region)
  throws IOException, InterruptedException {
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(ct, region.getRegionName());
    assertEquals(region.getEncodedName(),
      pair.getFirst().getEncodedName());
  }

}

