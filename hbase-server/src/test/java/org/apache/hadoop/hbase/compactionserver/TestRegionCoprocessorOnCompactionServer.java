/**
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
package org.apache.hadoop.hbase.compactionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.testclassification.CompactionServerTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * use verify table test {@link RegionCoprocessorHost.RegionEnvironment#getConnection()} and record
 * coprocessor method invoke {@link RegionObserver#preCompactSelection},
 * {@link RegionObserver#postCompactSelection}, {@link RegionObserver#preCompact},
 * {@link RegionObserver#preCompactScannerOpen}, {@link RegionObserver#preStoreFileReaderOpen},
 * {@link RegionObserver#postStoreFileReaderOpen},
 * {@link RegionObserver#postInstantiateDeleteTracker}
 */
@Category({ CompactionServerTests.class, MediumTests.class })
public class TestRegionCoprocessorOnCompactionServer extends TestCompactionServerBase {
  private static final TableName VERIFY_TABLE_NAME = TableName.valueOf("verifyTable");
  protected static String RS = "RS";
  protected static String CS = "CS";
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionCoprocessorOnCompactionServer.class);
  private static final Set<String> compactionCoprocessor = ImmutableSet.of("preCompactSelection",
    "postCompactSelection", "preCompactScannerOpen", "preCompact", "preStoreFileReaderOpen",
    "postStoreFileReaderOpen", "postInstantiateDeleteTracker");
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionCoprocessorOnCompactionServer.class);

  private static void recordCoprocessorCall(ObserverContext<RegionCoprocessorEnvironment> c,
      String methodName) throws IOException {
    byte[] col = c.getEnvironment().getServerName().equals(COMPACTION_SERVER_NAME) ? CS.getBytes()
        : RS.getBytes();
    Put put = new Put(methodName.getBytes());
    put.addColumn(FAMILY.getBytes(), col, Bytes.toBytes(1));
    c.getEnvironment().getConnection().getTable(VERIFY_TABLE_NAME).put(put);
  }

  private void verifyRecord(byte[] row, byte[] col, boolean exist) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(VERIFY_TABLE_NAME);
    Get get = new Get(row);
    Result r = h.get(get);
    if (exist) {
      assertEquals(1, Bytes.toInt(r.getValue(Bytes.toBytes(FAMILY), col)));
    } else {
      assertNull(r.getValue(Bytes.toBytes(FAMILY), col));
    }
    h.close();
  }

  public static class TestCompactionCoprocessor implements RegionObserver, RegionCoprocessor {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker)
        throws IOException {
      recordCoprocessorCall(c, "preCompactSelection");
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends StoreFile> selected, CompactionLifeCycleTracker tracker,
        CompactionRequest request) {
      try {
        recordCoprocessorCall(c, "postCompactSelection");
      } catch (IOException e) {
        LOG.error("postCompactSelection catch IOException:", e);
      }
    }

    @Override
    public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
      recordCoprocessorCall(c, "preCompactScannerOpen");
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
      recordCoprocessorCall(c, "preCompact");
      return scanner;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request)
        throws IOException {
      recordCoprocessorCall(c, "postCompact");
    }

    @Override
    public StoreFileReader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
        FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
        Reference r, StoreFileReader reader) throws IOException {
      recordCoprocessorCall(ctx, "preStoreFileReaderOpen");
      return reader;
    }

    @Override
    public StoreFileReader postStoreFileReaderOpen(
        ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p,
        FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r,
        StoreFileReader reader) throws IOException {
      recordCoprocessorCall(ctx, "postStoreFileReaderOpen");
      return reader;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
        ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
        throws IOException {
      recordCoprocessorCall(ctx, "postInstantiateDeleteTracker");
      return delTracker;
    }

  }

  @CoreCoprocessor
  public static class TestCompactionCoreCoprocessor extends TestCompactionCoprocessor {
  }

  public static class TestCoprocessorNotCompactionRelated
      implements RegionObserver, RegionCoprocessor {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
      recordCoprocessorCall(c, "preOpen");
    }
  }

  @Before
  public void before() throws Exception {
    super.before();
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(VERIFY_TABLE_NAME).build();
    TEST_UTIL.createTable(tableDescriptor, Bytes.toByteArrays(FAMILY),
      TEST_UTIL.getConfiguration());
    TEST_UTIL.waitTableAvailable(VERIFY_TABLE_NAME);
  }

  @After
  public void after() throws IOException {
    super.after();
    TEST_UTIL.deleteTableIfAny(VERIFY_TABLE_NAME);
  }

  /**
   * Test coprocessor related compaction will be load on compaction server, but postCompact be
   * executed on region server.
   */
  @Test
  public void testLoadCoprocessor() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    ColumnFamilyDescriptor cfd =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build();
    TableDescriptor modifiedTableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
        .setColumnFamily(cfd).setCompactionOffloadEnabled(true)
        .setCoprocessor(TestCompactionCoprocessor.class.getName()).build();
    TEST_UTIL.getAdmin().modifyTable(modifiedTableDescriptor);
    TEST_UTIL.waitTableAvailable(TABLENAME);
    doPutRecord(1, 1000, true);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);
    Thread.sleep(5000);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.requestCount.sum() > 0 && COMPACTION_SERVER.compactionThreadManager
          .getRunningCompactionTasks().values().size() == 0);
    for (String methodName : compactionCoprocessor) {
      verifyRecord(methodName.getBytes(), CS.getBytes(), true);
    }
    verifyRecord("postCompact".getBytes(), CS.getBytes(), false);
    verifyRecord("postCompact".getBytes(), RS.getBytes(), true);
  }

  /**
   * Test core coprocessor will not be load on compaction server
   */
  @Test
  public void testNotLoadCoreCoprocessor() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    ColumnFamilyDescriptor cfd =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build();
    TableDescriptor modifiedTableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
        .setColumnFamily(cfd).setCompactionOffloadEnabled(true)
        .setCoprocessor(TestCompactionCoreCoprocessor.class.getName()).build();
    TEST_UTIL.getAdmin().modifyTable(modifiedTableDescriptor);
    TEST_UTIL.waitTableAvailable(TABLENAME);
    doPutRecord(1, 1000, true);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);
    Thread.sleep(5000);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.requestCount.sum() > 0 && COMPACTION_SERVER.compactionThreadManager
          .getRunningCompactionTasks().values().size() == 0);
    for (String methodName : compactionCoprocessor) {
      verifyRecord(methodName.getBytes(), CS.getBytes(), false);
    }
    verifyRecord("postCompact".getBytes(), CS.getBytes(), false);
    verifyRecord("postCompact".getBytes(), RS.getBytes(), true);
  }

  /**
   * Test coprocessor not compaction related will not be load on compaction server
   */
  @Test
  public void testNotLoadCompactionNotRelatedCoprocessor() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    ColumnFamilyDescriptor cfd =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build();
    TableDescriptor modifiedTableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
        .setColumnFamily(cfd).setCompactionOffloadEnabled(true)
        .setCoprocessor(TestCoprocessorNotCompactionRelated.class.getName()).build();
    TEST_UTIL.getAdmin().modifyTable(modifiedTableDescriptor);
    TEST_UTIL.waitTableAvailable(TABLENAME);
    doPutRecord(1, 1000, true);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);
    Thread.sleep(5000);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.requestCount.sum() > 0 && COMPACTION_SERVER.compactionThreadManager
        .getRunningCompactionTasks().values().size() == 0);
    verifyRecord("preOpen".getBytes(), CS.getBytes(), false);
    verifyRecord("preOpen".getBytes(), RS.getBytes(), true);
  }
}
