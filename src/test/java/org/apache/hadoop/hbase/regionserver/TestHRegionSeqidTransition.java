/*
 * Copyright 2014 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.TestChangingEncoding;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;

public class TestHRegionSeqidTransition {
  private static final Log LOG = LogFactory.getLog(TestHRegionSeqidTransition.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration c = TEST_UTIL.getConfiguration();
  private static final String TABLENAME = "testHRegionSeqidTransition";
  private static final byte [][] FAMILIES = new byte [][] {TestChangingEncoding.CF_BYTES};
  private static final int NUM_HFILE_BATCHES = 2;


  /**
   * Test if the serialization and deserialization of HRegionSeqidTransition
   * @throws Exception
   */
  @Test
  public void testSerDe() throws Exception {
    HRegionSeqidTransition idTranOrigin = new HRegionSeqidTransition(10, 100);
    byte[] buffer = HRegionSeqidTransition.toBytes(idTranOrigin);
    HRegionSeqidTransition idTranCopy = HRegionSeqidTransition.fromBytes(buffer);
    assertEquals(idTranOrigin, idTranCopy);
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    c.setBoolean("dfs.support.append", true);
    c.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);
    c.setInt("hbase.master.meta.thread.rescanfrequency", 5*1000);
    // enable the meta region seqid recording
    c.setBoolean(HTableDescriptor.METAREGION_SEQID_RECORD_ENABLED, true);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int countOfRegions = TEST_UTIL.createMultiRegions(t, getTestFamily());
    TEST_UTIL.waitUntilAllRegionsAssigned(countOfRegions);
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 3) {
      // Need at least three servers.
      LOG.info("Started new server=" +
                 TEST_UTIL.getHBaseCluster().startRegionServer());
    }
  }

  @Test(timeout=300000)
  public void testSeqidTransitionOnRegionShutdown()
    throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Number of region servers = " + cluster.getLiveRegionServerThreads().size());

    HRegionServer regionServer0 = cluster.getRegionServer(0);
    HRegionServer regionServer1 = cluster.getRegionServer(1);
    HRegionServer regionServer2 = cluster.getRegionServer(2);

    Collection<HRegion> regions = regionServer1.getOnlineRegions();
    LOG.debug("RS " + regionServer1.getServerInfo().getHostnamePort() + " has "
                + regions.size() + " online regions");
    HRegion region = null;
    for (HRegion oneRegion : regions) {
      if (!oneRegion.getRegionInfo().isMetaRegion()) {
        region = oneRegion;
        break;
      }
    }
    assert(region != null);
    // write KVs
    for (int i = 0; i < NUM_HFILE_BATCHES; ++i) {
      TestChangingEncoding.writeTestDataBatchToRegion(region, region.getStartKey(), i);
    }
    // normal shutdown
    LOG.debug("Stopping RS " + regionServer1.getServerInfo().getHostnamePort());
    cluster.stopRegionServer(1);

    regions = regionServer2.getOnlineRegions();
    LOG.debug("RS " + regionServer2.getServerInfo().getHostnamePort() + " has "
                + regions.size() + " online regions");
    HRegion region1 = null;
    for (HRegion oneRegion : regions) {
      if (!oneRegion.getRegionInfo().isMetaRegion()) {
        region1 = oneRegion;
        break;
      }
    }
    assert(region1 != null);
    // write KVs
    for (int i = 0; i < NUM_HFILE_BATCHES; ++i) {
      TestChangingEncoding.writeTestDataBatchToRegion(region1, region1.getStartKey(), i);
    }
    // abnormal shutdown
    LOG.debug("Aborting RS " + regionServer2.getServerInfo().getHostnamePort());
    cluster.abortRegionServer(2);


    regions = regionServer0.getOnlineRegions();
    LOG.debug("RS " + regionServer0.getServerInfo().getHostnamePort() + " has "
                + regions.size() + " online regions");
    HRegion region0 = null;
    for (HRegion oneRegion : regions) {
      if (!oneRegion.getRegionInfo().isMetaRegion()) {
        region0 = oneRegion;
        break;
      }
    }
    assert(region0 != null);
    // write KVs
    for (int i = 0; i < NUM_HFILE_BATCHES; ++i) {
      TestChangingEncoding.writeTestDataBatchToRegion(region0, region0.getStartKey(), i);
    }
    // wait until two regions online on regionServer0
    while (regionServer0.getOnlineRegion(region.getRegionName()) == null) {
      Thread.sleep(500);
    }
    LOG.debug("Region " + region.getRegionNameAsString() + " opened on RS "
                + regionServer0.getServerInfo().getHostnamePort());
    while (regionServer0.getOnlineRegion(region1.getRegionName()) == null) {
      Thread.sleep(500);
    }
    LOG.debug("Region " + region1.getRegionNameAsString() + " opened on RS "
                + regionServer0.getServerInfo().getHostnamePort());

    // start testing
    HRegion[] testRegions = {region, region1};
    int[] regionHLogNums = {0, 0};

    // check hlog
    FileSystem fs = FileSystem.get(c);
    final Path baseDir = new Path(c.get(HConstants.HBASE_DIR));
    final Path logDir = new Path(baseDir, HConstants.HREGION_LOGDIR_NAME);
    int nLogFilesRead = 0;
    ArrayDeque<FileStatus> checkQueue = new ArrayDeque<FileStatus>(
      java.util.Arrays.asList(fs.listStatus(logDir)));
    while (!checkQueue.isEmpty()) {
      FileStatus logFile = checkQueue.pop();
      if (logFile.isDir()) {
        checkQueue.addAll(java.util.Arrays.asList(fs.listStatus(logFile.getPath())));
        continue;
      }
      HLog.Reader r = HLog.getReader(fs, logFile.getPath(), c);
      LOG.info("Reading HLog: " + logFile.getPath());
      HLog.Entry entry = null;
      while ((entry = r.next(entry)) != null) {
        HLogKey key = entry.getKey();
        WALEdit edit = entry.getEdit();
        for (int regionIndex = 0; regionIndex < testRegions.length; ++regionIndex) {
          if (Bytes.equals(key.getRegionName(), testRegions[regionIndex].getRegionName()) &&
            Bytes.equals(key.getTablename(),
                         testRegions[regionIndex].getTableDesc().getName())) {
            int count = 0;
            long lastSeqid = -1, nextSeqid = -1;
            for (KeyValue kv : edit.getKeyValues()) {
              if (Bytes.equals(kv.getRow(), testRegions[regionIndex].getStartKey()) &&
                Bytes.equals(kv.getFamily(), HLog.METAFAMILY)) {
                byte[] qualifier = kv. getQualifier();
                if (Bytes.equals(qualifier, HConstants.LAST_SEQID_QUALIFIER)) {
                  lastSeqid = Bytes.toLong(kv.getValue());
                  ++count;
                  continue;
                }
                if (Bytes.equals(qualifier, HConstants.NEXT_SEQID_QUALIFIER)) {
                  nextSeqid = Bytes.toLong(kv.getValue());
                  ++count;
                  continue;
                }
                if (Bytes.equals(qualifier, HConstants.SERVER_QUALIFIER) ||
                    Bytes.equals(qualifier, HConstants.STARTCODE_QUALIFIER) ||
                    Bytes.equals(qualifier, HConstants.REGIONINFO_QUALIFIER)) {
                  ++count;
                  continue;
                }
              } else {
                break;
              }
            }
            if ((count == 5) || (count == 3)) {
              ++regionHLogNums[regionIndex];
              HRegionSeqidTransition transition = new HRegionSeqidTransition(lastSeqid, nextSeqid);
              LOG.info("Found hlog records for region " +
                  testRegions[regionIndex].getRegionNameAsString() + ": " + transition);
            }
          }
        }
      }
      r.close();
      ++nLogFilesRead;
    }
    LOG.info("Processed " + nLogFilesRead +" log files and found " +
               regionHLogNums[0] + " hlogs for" + testRegions[0].getRegionNameAsString() + ", " +
               regionHLogNums[1] + " hlogs for" + testRegions[1].getRegionNameAsString());
    assertTrue(nLogFilesRead > 0);
    assertTrue(regionHLogNums[0] > 0);
    assertTrue(regionHLogNums[1] > 0);

    // check meta table
    HTable meta = new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.SERVER_QUALIFIER);
    scan.addColumn(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.STARTCODE_QUALIFIER);
    scan.addColumn(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.LAST_SEQID_QUALIFIER);
    scan.addColumn(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.NEXT_SEQID_QUALIFIER);
    scan.addColumn(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    int[] rows = {0, 0};
    for (Result result; (result = s.next()) != null;) {
      for (int regionIndex = 0; regionIndex < testRegions.length; ++regionIndex) {
        if (Bytes.equals(result.getRow(), testRegions[regionIndex].getRegionName())) {
          byte[] server = result.getValue(HConstants.CATALOG_HISTORIAN_FAMILY,
                                          HConstants.SERVER_QUALIFIER);
          byte[] startcode = result.getValue(HConstants.CATALOG_HISTORIAN_FAMILY,
                                             HConstants.STARTCODE_QUALIFIER);
          byte[] lastSeqid = result.getValue(HConstants.CATALOG_HISTORIAN_FAMILY,
                                             HConstants.LAST_SEQID_QUALIFIER);
          byte[] nextSeqid = result.getValue(HConstants.CATALOG_HISTORIAN_FAMILY,
                                             HConstants.NEXT_SEQID_QUALIFIER);
          byte[] regionInfo = result.getValue(HConstants.CATALOG_HISTORIAN_FAMILY,
                                              HConstants.REGIONINFO_QUALIFIER);
          if (server != null && startcode != null &&
              lastSeqid != null && nextSeqid != null &&
              regionInfo!= null) {
            ++rows[regionIndex];
            HRegionSeqidTransition transition = new HRegionSeqidTransition(
              Bytes.toLong(lastSeqid), Bytes.toLong(nextSeqid));
            LOG.info("Found meta records for regions[" + regionIndex +
                "] with a sequence id transition: " + transition);
          }
        }
      }
    }

    LOG.info("Found " + rows[0] + " meta records for " + testRegions[0].getRegionNameAsString() +
               "and " + rows[1] + " meta records for " + testRegions[1].getRegionNameAsString());
    assertTrue(rows[0] > 0);
    assertTrue(rows[1] > 0);
  }

  private static byte [] getTestFamily() {
    return FAMILIES[0];
  }
}
