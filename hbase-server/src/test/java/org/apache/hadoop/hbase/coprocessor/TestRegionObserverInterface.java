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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterAllFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({ CoprocessorTests.class, LargeTests.class })
public class TestRegionObserverInterface {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionObserverInterface.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionObserverInterface.class);

  public static final TableName TEST_TABLE = TableName.valueOf("TestTable");
  public static final byte[] FAMILY = Bytes.toBytes("f");
  public final static byte[] A = Bytes.toBytes("a");
  public final static byte[] B = Bytes.toBytes("b");
  public final static byte[] C = Bytes.toBytes("c");
  public final static byte[] ROW = Bytes.toBytes("testrow");

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.setBoolean("hbase.master.distributed.log.replay", true);
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      SimpleRegionObserver.class.getName());

    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testRegionObserver() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    // recreate table every time in order to reset the status of the
    // coprocessor.
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadDelete",
            "hadPostStartRegionOperation", "hadPostCloseRegionOperation",
            "hadPostBatchMutateIndispensably" },
        tableName, new Boolean[] { false, false, false, false, false, false, false, false });

      Put put = new Put(ROW);
      put.addColumn(A, A, A);
      put.addColumn(B, B, B);
      put.addColumn(C, C, C);
      table.put(put);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadPreBatchMutate",
            "hadPostBatchMutate", "hadDelete", "hadPostStartRegionOperation",
            "hadPostCloseRegionOperation", "hadPostBatchMutateIndispensably" },
        TEST_TABLE,
        new Boolean[] { false, false, true, true, true, true, false, true, true, true });

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getCtPreOpen", "getCtPostOpen", "getCtPreClose", "getCtPostClose" },
        tableName, new Integer[] { 1, 1, 0, 0 });

      Get get = new Get(ROW);
      get.addColumn(A, A);
      get.addColumn(B, B);
      get.addColumn(C, C);
      table.get(get);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadDelete",
            "hadPrePreparedDeleteTS" },
        tableName, new Boolean[] { true, true, true, true, false, false });

      Delete delete = new Delete(ROW);
      delete.addColumn(A, A);
      delete.addColumn(B, B);
      delete.addColumn(C, C);
      table.delete(delete);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadPreBatchMutate",
            "hadPostBatchMutate", "hadDelete", "hadPrePreparedDeleteTS" },
        tableName, new Boolean[] { true, true, true, true, true, true, true, true });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "getCtPreOpen", "getCtPostOpen", "getCtPreClose", "getCtPostClose" },
      tableName, new Integer[] { 1, 1, 1, 1 });
  }

  @Test
  public void testRowMutation() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadDeleted" },
        tableName, new Boolean[] { false, false, false, false, false });
      Put put = new Put(ROW);
      put.addColumn(A, A, A);
      put.addColumn(B, B, B);
      put.addColumn(C, C, C);

      Delete delete = new Delete(ROW);
      delete.addColumn(A, A);
      delete.addColumn(B, B);
      delete.addColumn(C, C);

      RowMutations arm = new RowMutations(ROW);
      arm.add(put);
      arm.add(delete);
      table.mutateRow(arm);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadDeleted" },
        tableName, new Boolean[] { false, false, true, true, true });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testIncrementHook() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      Increment inc = new Increment(Bytes.toBytes(0));
      inc.addColumn(A, A, 1);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreIncrement", "hadPostIncrement", "hadPreIncrementAfterRowLock",
          "hadPreBatchMutate", "hadPostBatchMutate", "hadPostBatchMutateIndispensably" },
        tableName, new Boolean[] { false, false, false, false, false, false });

      table.increment(inc);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreIncrement", "hadPostIncrement", "hadPreIncrementAfterRowLock",
          "hadPreBatchMutate", "hadPostBatchMutate", "hadPostBatchMutateIndispensably" },
        tableName, new Boolean[] { true, true, true, true, true, true });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testCheckAndPutHooks() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    try (Table table = util.createTable(tableName, new byte[][] { A, B, C })) {
      Put p = new Put(Bytes.toBytes(0));
      p.addColumn(A, A, A);
      table.put(p);
      p = new Put(Bytes.toBytes(0));
      p.addColumn(A, A, A);
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getPreCheckAndPut", "getPreCheckAndPutAfterRowLock", "getPostCheckAndPut",
          "getPreCheckAndPutWithFilter", "getPreCheckAndPutWithFilterAfterRowLock",
          "getPostCheckAndPutWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 });

      table.checkAndMutate(Bytes.toBytes(0), A).qualifier(A).ifEquals(A).thenPut(p);
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getPreCheckAndPut", "getPreCheckAndPutAfterRowLock", "getPostCheckAndPut",
          "getPreCheckAndPutWithFilter", "getPreCheckAndPutWithFilterAfterRowLock",
          "getPostCheckAndPutWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1, 0, 0, 0, 1, 1, 1 });

      table.checkAndMutate(Bytes.toBytes(0),
          new SingleColumnValueFilter(A, A, CompareOperator.EQUAL, A))
        .thenPut(p);
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getPreCheckAndPut", "getPreCheckAndPutAfterRowLock", "getPostCheckAndPut",
          "getPreCheckAndPutWithFilter", "getPreCheckAndPutWithFilterAfterRowLock",
          "getPostCheckAndPutWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1, 1, 1, 1, 2, 2, 2 });
    } finally {
      util.deleteTable(tableName);
    }
  }

  @Test
  public void testCheckAndDeleteHooks() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      Put p = new Put(Bytes.toBytes(0));
      p.addColumn(A, A, A);
      table.put(p);
      Delete d = new Delete(Bytes.toBytes(0));
      table.delete(d);
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndDelete",
          "getPreCheckAndDeleteAfterRowLock", "getPostCheckAndDelete",
          "getPreCheckAndDeleteWithFilter", "getPreCheckAndDeleteWithFilterAfterRowLock",
          "getPostCheckAndDeleteWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 });

      table.checkAndMutate(Bytes.toBytes(0), A).qualifier(A).ifEquals(A).thenDelete(d);
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndDelete",
          "getPreCheckAndDeleteAfterRowLock", "getPostCheckAndDelete",
          "getPreCheckAndDeleteWithFilter", "getPreCheckAndDeleteWithFilterAfterRowLock",
          "getPostCheckAndDeleteWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1, 0, 0, 0, 1, 1, 1 });

      table.checkAndMutate(Bytes.toBytes(0),
          new SingleColumnValueFilter(A, A, CompareOperator.EQUAL, A))
        .thenDelete(d);
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndDelete",
          "getPreCheckAndDeleteAfterRowLock", "getPostCheckAndDelete",
          "getPreCheckAndDeleteWithFilter", "getPreCheckAndDeleteWithFilterAfterRowLock",
          "getPostCheckAndDeleteWithFilter", "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1, 1, 1, 1, 2, 2, 2 });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testCheckAndIncrementHooks() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." +
      name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      byte[] row = Bytes.toBytes(0);

      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 0, 0, 0 });

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifNotExists(A, A)
        .build(new Increment(row).addColumn(A, A, 1)));
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1 });

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifEquals(A, A, Bytes.toBytes(1L))
        .build(new Increment(row).addColumn(A, A, 1)));
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 2, 2, 2 });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testCheckAndAppendHooks() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." +
      name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      byte[] row = Bytes.toBytes(0);

      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 0, 0, 0 });

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifNotExists(A, A)
        .build(new Append(row).addColumn(A, A, A)));
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1 });

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifEquals(A, A, A)
        .build(new Append(row).addColumn(A, A, A)));
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 2, 2, 2 });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testCheckAndRowMutationsHooks() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." +
      name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      byte[] row = Bytes.toBytes(0);

      Put p = new Put(row).addColumn(A, A, A);
      table.put(p);
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 0, 0, 0 });

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifEquals(A, A, A)
        .build(new RowMutations(row)
          .add((Mutation) new Put(row).addColumn(B, B, B))
          .add((Mutation) new Delete(row))));
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 1, 1, 1 });

      Object[] result = new Object[2];
      table.batch(Arrays.asList(p, CheckAndMutate.newBuilder(row)
        .ifEquals(A, A, A)
        .build(new RowMutations(row)
          .add((Mutation) new Put(row).addColumn(B, B, B))
          .add((Mutation) new Delete(row)))), result);
      verifyMethodResult(
        SimpleRegionObserver.class, new String[] { "getPreCheckAndMutate",
          "getPreCheckAndMutateAfterRowLock", "getPostCheckAndMutate" },
        tableName, new Integer[] { 2, 2, 2 });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testAppendHook() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try {
      Append app = new Append(Bytes.toBytes(0));
      app.addColumn(A, A, A);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreAppend", "hadPostAppend", "hadPreAppendAfterRowLock",
          "hadPreBatchMutate", "hadPostBatchMutate", "hadPostBatchMutateIndispensably" },
        tableName,
        new Boolean[] { false, false, false, false, false, false });

      table.append(app);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreAppend", "hadPostAppend", "hadPreAppendAfterRowLock",
          "hadPreBatchMutate", "hadPostBatchMutate", "hadPostBatchMutateIndispensably" },
        tableName,
        new Boolean[] { true, true, true, true, true, true });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  // HBase-3583
  public void testHBase3583() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    util.createTable(tableName, new byte[][] { A, B, C });
    util.waitUntilAllRegionsAssigned(tableName);

    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "hadPreGet", "hadPostGet", "wasScannerNextCalled", "wasScannerCloseCalled" },
      tableName, new Boolean[] { false, false, false, false });

    Table table = util.getConnection().getTable(tableName);
    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    table.put(put);

    Get get = new Get(ROW);
    get.addColumn(A, A);
    table.get(get);

    // verify that scannerNext and scannerClose upcalls won't be invoked
    // when we perform get().
    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "hadPreGet", "hadPostGet", "wasScannerNextCalled", "wasScannerCloseCalled" },
      tableName, new Boolean[] { true, true, false, false });

    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      }
    } finally {
      scanner.close();
    }

    // now scanner hooks should be invoked.
    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "wasScannerNextCalled", "wasScannerCloseCalled" }, tableName,
      new Boolean[] { true, true });
    util.deleteTable(tableName);
    table.close();
  }

  @Test
  public void testHBASE14489() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A });
    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    table.put(put);

    Scan s = new Scan();
    s.setFilter(new FilterAllFilter());
    ResultScanner scanner = table.getScanner(s);
    try {
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      }
    } finally {
      scanner.close();
    }
    verifyMethodResult(SimpleRegionObserver.class, new String[] { "wasScannerFilterRowCalled" },
      tableName, new Boolean[] { true });
    util.deleteTable(tableName);
    table.close();

  }

  @Test
  // HBase-3758
  public void testHBase3758() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    util.createTable(tableName, new byte[][] { A, B, C });

    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "hadDeleted", "wasScannerOpenCalled" }, tableName,
      new Boolean[] { false, false });

    Table table = util.getConnection().getTable(tableName);
    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    table.put(put);

    Delete delete = new Delete(ROW);
    table.delete(delete);

    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "hadDeleted", "wasScannerOpenCalled" }, tableName,
      new Boolean[] { true, false });

    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      }
    } finally {
      scanner.close();
    }

    // now scanner hooks should be invoked.
    verifyMethodResult(SimpleRegionObserver.class, new String[] { "wasScannerOpenCalled" },
      tableName, new Boolean[] { true });
    util.deleteTable(tableName);
    table.close();
  }

  /* Overrides compaction to only output rows with keys that are even numbers */
  public static class EvenOnlyCompactor implements RegionCoprocessor, RegionObserver {
    long lastCompaction;
    long lastFlush;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) {
      return new InternalScanner() {

        @Override
        public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {
          List<Cell> internalResults = new ArrayList<>();
          boolean hasMore;
          do {
            hasMore = scanner.next(internalResults, scannerContext);
            if (!internalResults.isEmpty()) {
              long row = Bytes.toLong(CellUtil.cloneValue(internalResults.get(0)));
              if (row % 2 == 0) {
                // return this row
                break;
              }
              // clear and continue
              internalResults.clear();
            }
          } while (hasMore);

          if (!internalResults.isEmpty()) {
            results.addAll(internalResults);
          }
          return hasMore;
        }

        @Override
        public void close() throws IOException {
          scanner.close();
        }
      };
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
        StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request) {
      lastCompaction = EnvironmentEdgeManager.currentTime();
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
        FlushLifeCycleTracker tracker) {
      lastFlush = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * Tests overriding compaction handling via coprocessor hooks
   * @throws Exception
   */
  @Test
  public void testCompactionOverride() throws Exception {
    final TableName compactTable = TableName.valueOf(name.getMethodName());
    Admin admin = util.getAdmin();
    if (admin.tableExists(compactTable)) {
      admin.disableTable(compactTable);
      admin.deleteTable(compactTable);
    }

    HTableDescriptor htd = new HTableDescriptor(compactTable);
    htd.addFamily(new HColumnDescriptor(A));
    htd.addCoprocessor(EvenOnlyCompactor.class.getName());
    admin.createTable(htd);

    Table table = util.getConnection().getTable(compactTable);
    for (long i = 1; i <= 10; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put put = new Put(iBytes);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(A, A, iBytes);
      table.put(put);
    }

    HRegion firstRegion = cluster.getRegions(compactTable).get(0);
    Coprocessor cp = firstRegion.getCoprocessorHost().findCoprocessor(EvenOnlyCompactor.class);
    assertNotNull("EvenOnlyCompactor coprocessor should be loaded", cp);
    EvenOnlyCompactor compactor = (EvenOnlyCompactor) cp;

    // force a compaction
    long ts = System.currentTimeMillis();
    admin.flush(compactTable);
    // wait for flush
    for (int i = 0; i < 10; i++) {
      if (compactor.lastFlush >= ts) {
        break;
      }
      Thread.sleep(1000);
    }
    assertTrue("Flush didn't complete", compactor.lastFlush >= ts);
    LOG.debug("Flush complete");

    ts = compactor.lastFlush;
    admin.majorCompact(compactTable);
    // wait for compaction
    for (int i = 0; i < 30; i++) {
      if (compactor.lastCompaction >= ts) {
        break;
      }
      Thread.sleep(1000);
    }
    LOG.debug("Last compaction was at " + compactor.lastCompaction);
    assertTrue("Compaction didn't complete", compactor.lastCompaction >= ts);

    // only even rows should remain
    ResultScanner scanner = table.getScanner(new Scan());
    try {
      for (long i = 2; i <= 10; i += 2) {
        Result r = scanner.next();
        assertNotNull(r);
        assertFalse(r.isEmpty());
        byte[] iBytes = Bytes.toBytes(i);
        assertArrayEquals("Row should be " + i, r.getRow(), iBytes);
        assertArrayEquals("Value should be " + i, r.getValue(A, A), iBytes);
      }
    } finally {
      scanner.close();
    }
    table.close();
  }

  @Test
  public void bulkLoadHFileTest() throws Exception {
    final String testName = TestRegionObserverInterface.class.getName() + "." + name.getMethodName();
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Configuration conf = util.getConfiguration();
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {
      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreBulkLoadHFile", "hadPostBulkLoadHFile" }, tableName,
        new Boolean[] { false, false });

      FileSystem fs = util.getTestFileSystem();
      final Path dir = util.getDataTestDirOnTestFS(testName).makeQualified(fs);
      Path familyDir = new Path(dir, Bytes.toString(A));

      createHFile(util.getConfiguration(), fs, new Path(familyDir, Bytes.toString(A)), A, A);

      // Bulk load
      new LoadIncrementalHFiles(conf).doBulkLoad(dir, util.getAdmin(), table, locator);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreBulkLoadHFile", "hadPostBulkLoadHFile" }, tableName,
        new Boolean[] { true, true });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testRecovery() throws Exception {
    LOG.info(TestRegionObserverInterface.class.getName() + "." + name.getMethodName());
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() + "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });
    try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {

      JVMClusterUtil.RegionServerThread rs1 = cluster.startRegionServer();
      ServerName sn2 = rs1.getRegionServer().getServerName();
      String regEN = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();

      util.getAdmin().move(Bytes.toBytes(regEN), sn2);
      while (!sn2.equals(locator.getAllRegionLocations().get(0).getServerName())) {
        Thread.sleep(100);
      }

      Put put = new Put(ROW);
      put.addColumn(A, A, A);
      put.addColumn(B, B, B);
      put.addColumn(C, C, C);
      table.put(put);

      // put two times
      table.put(put);

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "hadPreGet", "hadPostGet", "hadPrePut", "hadPostPut", "hadPreBatchMutate",
            "hadPostBatchMutate", "hadDelete" },
        tableName, new Boolean[] { false, false, true, true, true, true, false });

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getCtPreReplayWALs", "getCtPostReplayWALs", "getCtPreWALRestore",
            "getCtPostWALRestore", "getCtPrePut", "getCtPostPut" },
        tableName, new Integer[] { 0, 0, 0, 0, 2, 2 });

      cluster.killRegionServer(rs1.getRegionServer().getServerName());
      Threads.sleep(1000); // Let the kill soak in.
      util.waitUntilAllRegionsAssigned(tableName);
      LOG.info("All regions assigned");

      verifyMethodResult(SimpleRegionObserver.class,
        new String[] { "getCtPreReplayWALs", "getCtPostReplayWALs", "getCtPreWALRestore",
            "getCtPostWALRestore", "getCtPrePut", "getCtPostPut" },
        tableName, new Integer[] { 1, 1, 2, 2, 0, 0 });
    } finally {
      util.deleteTable(tableName);
      table.close();
    }
  }

  @Test
  public void testPreWALRestoreSkip() throws Exception {
    LOG.info(TestRegionObserverInterface.class.getName() + "." + name.getMethodName());
    TableName tableName = TableName.valueOf(SimpleRegionObserver.TABLE_SKIPPED);
    Table table = util.createTable(tableName, new byte[][] { A, B, C });

    try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {
      JVMClusterUtil.RegionServerThread rs1 = cluster.startRegionServer();
      ServerName sn2 = rs1.getRegionServer().getServerName();
      String regEN = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();

      util.getAdmin().move(Bytes.toBytes(regEN), sn2);
      while (!sn2.equals(locator.getAllRegionLocations().get(0).getServerName())) {
        Thread.sleep(100);
      }

      Put put = new Put(ROW);
      put.addColumn(A, A, A);
      put.addColumn(B, B, B);
      put.addColumn(C, C, C);
      table.put(put);

      cluster.killRegionServer(rs1.getRegionServer().getServerName());
      Threads.sleep(20000); // just to be sure that the kill has fully started.
      util.waitUntilAllRegionsAssigned(tableName);
    }

    verifyMethodResult(SimpleRegionObserver.class,
      new String[] { "getCtPreWALRestore", "getCtPostWALRestore", }, tableName,
      new Integer[] { 0, 0 });

    util.deleteTable(tableName);
    table.close();
  }

  //called from testPreWALAppendIsWrittenToWAL
  private void testPreWALAppendHook(Table table, TableName tableName) throws IOException {
    int expectedCalls = 0;
    String [] methodArray = new String[1];
    methodArray[0] = "getCtPreWALAppend";
    Object[] resultArray = new Object[1];

    Put p = new Put(ROW);
    p.addColumn(A, A, A);
    table.put(p);
    resultArray[0] = ++expectedCalls;
    verifyMethodResult(SimpleRegionObserver.class, methodArray, tableName, resultArray);

    Append a = new Append(ROW);
    a.addColumn(B, B, B);
    table.append(a);
    resultArray[0] = ++expectedCalls;
    verifyMethodResult(SimpleRegionObserver.class, methodArray, tableName, resultArray);

    Increment i = new Increment(ROW);
    i.addColumn(C, C, 1);
    table.increment(i);
    resultArray[0] = ++expectedCalls;
    verifyMethodResult(SimpleRegionObserver.class, methodArray, tableName, resultArray);

    Delete d = new Delete(ROW);
    table.delete(d);
    resultArray[0] = ++expectedCalls;
    verifyMethodResult(SimpleRegionObserver.class, methodArray, tableName, resultArray);
  }

  @Test
  public void testPreWALAppend() throws Exception {
    SimpleRegionObserver sro = new SimpleRegionObserver();
    ObserverContext ctx = Mockito.mock(ObserverContext.class);
    WALKey key = new WALKeyImpl(Bytes.toBytes("region"), TEST_TABLE,
        EnvironmentEdgeManager.currentTime());
    WALEdit edit = new WALEdit();
    sro.preWALAppend(ctx, key, edit);
    Assert.assertEquals(1, key.getExtendedAttributes().size());
    Assert.assertArrayEquals(SimpleRegionObserver.WAL_EXTENDED_ATTRIBUTE_BYTES,
        key.getExtendedAttribute(Integer.toString(sro.getCtPreWALAppend())));
  }

  @Test
  public void testPreWALAppendIsWrittenToWAL() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() +
        "." + name.getMethodName());
    Table table = util.createTable(tableName, new byte[][] { A, B, C });

    PreWALAppendWALActionsListener listener = new PreWALAppendWALActionsListener();
    List<HRegion> regions = util.getHBaseCluster().getRegions(tableName);
    //should be only one region
    HRegion region = regions.get(0);
    region.getWAL().registerWALActionsListener(listener);
    testPreWALAppendHook(table, tableName);
    boolean[] expectedResults = {true, true, true, true};
    Assert.assertArrayEquals(expectedResults, listener.getWalKeysCorrectArray());

  }

  @Test
  public void testPreWALAppendNotCalledOnMetaEdit() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_TABLE.getNameAsString() +
        "." + name.getMethodName());
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
    tdBuilder.setColumnFamily(cfBuilder.build());
    tdBuilder.setCoprocessor(SimpleRegionObserver.class.getName());
    TableDescriptor td = tdBuilder.build();
    Table table = util.createTable(td, new byte[][] { A, B, C });

    PreWALAppendWALActionsListener listener = new PreWALAppendWALActionsListener();
    List<HRegion> regions = util.getHBaseCluster().getRegions(tableName);
    //should be only one region
    HRegion region = regions.get(0);

    region.getWAL().registerWALActionsListener(listener);
    //flushing should write to the WAL
    region.flush(true);
    //so should compaction
    region.compact(false);
    //and so should closing the region
    region.close();

    //but we still shouldn't have triggered preWALAppend because no user data was written
    String[] methods = new String[] {"getCtPreWALAppend"};
    Object[] expectedResult = new Integer[]{0};
    verifyMethodResult(SimpleRegionObserver.class, methods, tableName, expectedResult);
  }

  // check each region whether the coprocessor upcalls are called or not.
  private void verifyMethodResult(Class<?> coprocessor, String methodName[], TableName tableName,
      Object value[]) throws IOException {
    try {
      for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
        if (!t.isAlive() || t.getRegionServer().isAborted() || t.getRegionServer().isStopping()) {
          continue;
        }
        for (RegionInfo r : ProtobufUtil
            .getOnlineRegions(t.getRegionServer().getRSRpcServices())) {
          if (!r.getTable().equals(tableName)) {
            continue;
          }
          RegionCoprocessorHost cph =
              t.getRegionServer().getOnlineRegion(r.getRegionName()).getCoprocessorHost();

          Coprocessor cp = cph.findCoprocessor(coprocessor.getName());
          assertNotNull(cp);
          for (int i = 0; i < methodName.length; ++i) {
            Method m = coprocessor.getMethod(methodName[i]);
            Object o = m.invoke(cp);
            assertTrue("Result of " + coprocessor.getName() + "." + methodName[i]
                    + " is expected to be " + value[i].toString() + ", while we get "
                    + o.toString(), o.equals(value[i]));
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(e.toString());
    }
  }

  private static void createHFile(Configuration conf, FileSystem fs, Path path, byte[] family,
      byte[] qualifier) throws IOException {
    HFileContext context = new HFileContextBuilder().build();
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, path)
        .withFileContext(context).create();
    long now = System.currentTimeMillis();
    try {
      for (int i = 1; i <= 9; i++) {
        KeyValue kv =
            new KeyValue(Bytes.toBytes(i + ""), family, qualifier, now, Bytes.toBytes(i + ""));
        writer.append(kv);
      }
    } finally {
      writer.close();
    }
  }

  private static class PreWALAppendWALActionsListener implements WALActionsListener {
    boolean[] walKeysCorrect = {false, false, false, false};

    @Override
    public void postAppend(long entryLen, long elapsedTimeMillis,
                           WALKey logKey, WALEdit logEdit) throws IOException {
      for (int k = 0; k < 4; k++) {
        if (!walKeysCorrect[k]) {
          walKeysCorrect[k] = Arrays.equals(SimpleRegionObserver.WAL_EXTENDED_ATTRIBUTE_BYTES,
              logKey.getExtendedAttribute(Integer.toString(k + 1)));
        }
      }
    }

    boolean[] getWalKeysCorrectArray() {
      return walKeysCorrect;
    }
  }
}
