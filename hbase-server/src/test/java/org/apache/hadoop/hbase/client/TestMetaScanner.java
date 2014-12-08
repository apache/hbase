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
package org.apache.hadoop.hbase.client;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(MediumTests.class)
public class TestMetaScanner {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaScanner() throws Exception {
    LOG.info("Starting testMetaScanner");
    setUp();
    final TableName TABLENAME =
        TableName.valueOf("testMetaScanner");
    final byte[] FAMILY = Bytes.toBytes("family");
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    Configuration conf = TEST_UTIL.getConfiguration();
    HTable table = new HTable(conf, TABLENAME);
    TEST_UTIL.createMultiRegions(conf, table, FAMILY,
        new byte[][]{
          HConstants.EMPTY_START_ROW,
          Bytes.toBytes("region_a"),
          Bytes.toBytes("region_b")});
    // Make sure all the regions are deployed
    TEST_UTIL.countRows(table);

    MetaScanner.MetaScannerVisitor visitor =
      mock(MetaScanner.MetaScannerVisitor.class);
    doReturn(true).when(visitor).processRow((Result)anyObject());

    // Scanning the entire table should give us three rows
    MetaScanner.metaScan(conf, null, visitor, TABLENAME);
    verify(visitor, times(3)).processRow((Result)anyObject());

    // Scanning the table with a specified empty start row should also
    // give us three hbase:meta rows
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, HConstants.EMPTY_BYTE_ARRAY, 1000);
    verify(visitor, times(3)).processRow((Result)anyObject());

    // Scanning the table starting in the middle should give us two rows:
    // region_a and region_b
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, Bytes.toBytes("region_ac"), 1000);
    verify(visitor, times(2)).processRow((Result)anyObject());

    // Scanning with a limit of 1 should only give us one row
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, Bytes.toBytes("region_ac"), 1);
    verify(visitor, times(1)).processRow((Result)anyObject());
    table.close();
  }

  @Test
  public void testConcurrentMetaScannerAndCatalogJanitor() throws Throwable {
    /* TEST PLAN: start with only one region in a table. Have a splitter
     * thread  and metascanner threads that continously scan the meta table for regions.
     * CatalogJanitor from master will run frequently to clean things up
     */
    TEST_UTIL.getConfiguration().setLong("hbase.catalogjanitor.interval", 500);
    setUp();

    final long runtime = 30 * 1000; //30 sec
    LOG.info("Starting testConcurrentMetaScannerAndCatalogJanitor");
    final TableName TABLENAME =
        TableName.valueOf("testConcurrentMetaScannerAndCatalogJanitor");
    final byte[] FAMILY = Bytes.toBytes("family");
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    final CatalogTracker catalogTracker = mock(CatalogTracker.class);
    when(catalogTracker.getConnection()).thenReturn(TEST_UTIL.getHBaseAdmin().getConnection());

    class RegionMetaSplitter extends StoppableImplementation implements Runnable {
      Random random = new Random();
      Throwable ex = null;
      @Override
      public void run() {
        while (!isStopped()) {
          try {
            List<HRegionInfo> regions = MetaScanner.listAllRegions(
              TEST_UTIL.getConfiguration(), false);

            //select a random region
            HRegionInfo parent = regions.get(random.nextInt(regions.size()));
            if (parent == null || !TABLENAME.equals(parent.getTable())) {
              continue;
            }

            long startKey = 0, endKey = Long.MAX_VALUE;
            byte[] start = parent.getStartKey();
            byte[] end = parent.getEndKey();
            if (!Bytes.equals(HConstants.EMPTY_START_ROW, parent.getStartKey())) {
              startKey = Bytes.toLong(parent.getStartKey());
            }
            if (!Bytes.equals(HConstants.EMPTY_END_ROW, parent.getEndKey())) {
              endKey = Bytes.toLong(parent.getEndKey());
            }
            if (startKey == endKey) {
              continue;
            }

            long midKey = BigDecimal.valueOf(startKey).add(BigDecimal.valueOf(endKey))
                .divideToIntegralValue(BigDecimal.valueOf(2)).longValue();

            HRegionInfo splita = new HRegionInfo(TABLENAME,
              start,
              Bytes.toBytes(midKey));
            HRegionInfo splitb = new HRegionInfo(TABLENAME,
              Bytes.toBytes(midKey),
              end);

            MetaEditor.splitRegion(catalogTracker, parent, splita, splitb, ServerName.valueOf("fooserver", 1, 0));

            Threads.sleep(random.nextInt(200));
          } catch (Throwable e) {
            ex = e;
            Assert.fail(StringUtils.stringifyException(e));
          }
        }
      }
      void rethrowExceptionIfAny() throws Throwable {
        if (ex != null) { throw ex; }
      }
    }

    class MetaScannerVerifier extends StoppableImplementation implements Runnable {
      Random random = new Random();
      Throwable ex = null;
      @Override
      public void run() {
         while(!isStopped()) {
           try {
            NavigableMap<HRegionInfo, ServerName> regions =
                MetaScanner.allTableRegions(TEST_UTIL.getConfiguration(), null, TABLENAME, false);

            LOG.info("-------");
            byte[] lastEndKey = HConstants.EMPTY_START_ROW;
            for (HRegionInfo hri: regions.navigableKeySet()) {
              long startKey = 0, endKey = Long.MAX_VALUE;
              if (!Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey())) {
                startKey = Bytes.toLong(hri.getStartKey());
              }
              if (!Bytes.equals(HConstants.EMPTY_END_ROW, hri.getEndKey())) {
                endKey = Bytes.toLong(hri.getEndKey());
              }
              LOG.info("start:" + startKey + " end:" + endKey + " hri:" + hri);
              Assert.assertTrue("lastEndKey=" + Bytes.toString(lastEndKey) + ", startKey=" +
                Bytes.toString(hri.getStartKey()), Bytes.equals(lastEndKey, hri.getStartKey()));
              lastEndKey = hri.getEndKey();
            }
            Assert.assertTrue(Bytes.equals(lastEndKey, HConstants.EMPTY_END_ROW));
            LOG.info("-------");
            Threads.sleep(10 + random.nextInt(50));
          } catch (Throwable e) {
            ex = e;
            Assert.fail(StringUtils.stringifyException(e));
          }
         }
      }
      void rethrowExceptionIfAny() throws Throwable {
        if (ex != null) { throw ex; }
      }
    }

    RegionMetaSplitter regionMetaSplitter = new RegionMetaSplitter();
    MetaScannerVerifier metaScannerVerifier = new MetaScannerVerifier();

    Thread regionMetaSplitterThread = new Thread(regionMetaSplitter);
    Thread metaScannerVerifierThread = new Thread(metaScannerVerifier);

    regionMetaSplitterThread.start();
    metaScannerVerifierThread.start();

    Threads.sleep(runtime);

    regionMetaSplitter.stop("test finished");
    metaScannerVerifier.stop("test finished");

    regionMetaSplitterThread.join();
    metaScannerVerifierThread.join();

    regionMetaSplitter.rethrowExceptionIfAny();
    metaScannerVerifier.rethrowExceptionIfAny();
  }

}

