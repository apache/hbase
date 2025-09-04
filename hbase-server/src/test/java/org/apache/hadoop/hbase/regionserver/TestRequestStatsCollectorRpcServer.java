/*
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

import com.github.benmanes.caffeine.cache.Cache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRequestStatsCollectorRpcServer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestStatsCollectorRpcServer.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] CF1 = Bytes.toBytes("cf1");
  private static final byte[] Q1 = Bytes.toBytes("q1");
  private static final byte[] Q2 = Bytes.toBytes("q2");

  private static Admin admin;

  private TableName tableName;
  private Table table;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    try (SingleProcessHBaseCluster cluster = TEST_UTIL.startMiniCluster()) {
      cluster.waitForActiveAndReadyMaster();
    }
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();

    tableName = TableName.valueOf(testName.getMethodName());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cf1).build();
    admin.createTable(td);
    table = admin.getConnection().getTable(tableName);
  }

  @After
  public void afterTestMethod() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testRequestStats() throws IOException, InterruptedException {
    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    byte[] regionNameAsBytes = region.getRegionInfo().getEncodedNameAsBytes();

    RpcServer rpcServer =
      (RpcServer) (TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer());
    RequestStatsCollector collector = rpcServer.getRequestStatsCollector();
    collector.start(1, 100);

    RequestStat requestStat;
    long responseTimePrev;
    CheckAndMutate checkAndMutate;

    byte[] rowKey = Bytes.toBytes("row");
    byte[] rowKey2 = Bytes.toBytes("row2");
    byte[] rowKeyNonExisting = Bytes.toBytes("rowKeyNonExisting");
    Cache<Request, RequestStat> requestStats = collector.getRequestStats();

    // To test responseTime
    EnvironmentEdgeManager.injectEdge(new IncrementingEnvironmentEdge());

    // Verify put
    Put put = new Put(rowKey);
    put.addColumn(CF1, Q1, Bytes.toBytes("value"));
    table.put(put);
    Optional<Map.Entry<Request, RequestStat>> statEntry = requestStats.asMap().entrySet().stream()
      .filter(entry -> Arrays.equals(entry.getKey().rowKey(), rowKey)
        && Arrays.equals(entry.getKey().region(), regionNameAsBytes)
        && entry.getKey().operation() == Operation.PUT)
      .findFirst();
    String client = statEntry.isPresent() ? statEntry.get().getKey().client() : "unknown";
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(2, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    responseTimePrev = requestStat.responseTimeSumMs();

    // Verify put again
    table.put(put);
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.PUT, client));
    assertEquals(2, requestStat.counts());
    assertEquals(4, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > responseTimePrev);

    // Verify get
    Get get = new Get(rowKey);
    get.addFamily(CF1);
    table.get(get);
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.GET, client));
    assertEquals(1, requestStat.counts());
    assertEquals(43, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    responseTimePrev = requestStat.responseTimeSumMs();

    // Verify get again
    table.get(get);
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.GET, client));
    assertEquals(2, requestStat.counts());
    assertEquals(43 * 2, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > responseTimePrev);

    // Verify get non-existing row
    get = new Get(rowKeyNonExisting);
    table.get(get);
    requestStat =
      collector.get(new Request(rowKeyNonExisting, regionNameAsBytes, Operation.GET, client));
    assertEquals(1, requestStat.counts());
    assertEquals(4, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify scan
    ResultScanner scanner = table.getScanner(new Scan());
    scanner.next();
    scanner.close();
    requestStat =
      collector.get(new Request("".getBytes(), regionNameAsBytes, Operation.SCAN, client));
    assertEquals(1, requestStat.counts());
    assertTrue(
      requestStat.responseSizeSumBytes() == 98 || requestStat.responseSizeSumBytes() == 99);
    assertTrue(requestStat.responseTimeSumMs() > 0);

    scanner = table.getScanner(new Scan().withStartRow(rowKey));
    scanner.next();
    scanner.close();
    requestStat =
      collector.get(new Request("row ~ ".getBytes(), regionNameAsBytes, Operation.SCAN, client));
    assertEquals(1, requestStat.counts());
    assertTrue(
      requestStat.responseSizeSumBytes() == 98 || requestStat.responseSizeSumBytes() == 99);
    assertTrue(requestStat.responseTimeSumMs() > 0);

    scanner = table.getScanner(new Scan().withStartRow(rowKey).withStopRow(rowKey2));
    scanner.next();
    scanner.close();
    requestStat = collector
      .get(new Request("row ~ row2".getBytes(), regionNameAsBytes, Operation.SCAN, client));
    assertEquals(1, requestStat.counts());
    assertTrue(
      requestStat.responseSizeSumBytes() == 98 || requestStat.responseSizeSumBytes() == 99);
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify append
    Append append = new Append(rowKey);
    append.addColumn(CF1, Q1, Bytes.toBytes("append"));
    table.append(append);
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.APPEND, client));
    assertEquals(1, requestStat.counts());
    assertEquals(92, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify increment
    Increment increment = new Increment(rowKey);
    increment.addColumn(CF1, Q2, 1);
    table.increment(increment);
    requestStat =
      collector.get(new Request(rowKey, regionNameAsBytes, Operation.INCREMENT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(86, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify checkAndPut
    Put checkAndPut = new Put(rowKey);
    checkAndPut.addColumn(CF1, Q1, Bytes.toBytes("value2"));
    checkAndMutate = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q1, Bytes.toBytes("value"))
      .build(checkAndPut);
    table.checkAndMutate(checkAndMutate);
    requestStat =
      collector.get(new Request(rowKey, regionNameAsBytes, Operation.CHECK_AND_PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(2, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify checkAndDelete
    Delete checkAndDelete = new Delete(rowKey);
    checkAndDelete.addColumn(CF1, Q1);
    checkAndMutate = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q1, Bytes.toBytes("value2"))
      .build(checkAndDelete);
    table.checkAndMutate(checkAndMutate);
    requestStat =
      collector.get(new Request(rowKey, regionNameAsBytes, Operation.CHECK_AND_DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(2, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify delete
    table.delete(new Delete(rowKey));
    requestStat = collector.get(new Request(rowKey, regionNameAsBytes, Operation.DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(2, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify puts
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(CF1, Q1, Bytes.toBytes("value"));
      puts.add(p);
    }
    table.put(puts);
    requestStat =
      collector.get(new Request("row0".getBytes(), regionNameAsBytes, Operation.BATCH_PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(16, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat =
      collector.get(new Request("row1".getBytes(), regionNameAsBytes, Operation.BATCH_PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(16, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify deletes
    List<Delete> deletes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Delete d = new Delete(Bytes.toBytes("row" + i));
      deletes.add(d);
    }
    table.delete(deletes);
    requestStat = collector
      .get(new Request("row0".getBytes(), regionNameAsBytes, Operation.BATCH_DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(16, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat = collector
      .get(new Request("row1".getBytes(), regionNameAsBytes, Operation.BATCH_DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(16, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify batch
    Put put5 = new Put(Bytes.toBytes("row5"));
    put5.addColumn(CF1, Q1, Bytes.toBytes("value"));
    table.put(put5);
    List<Row> actions = new ArrayList<>();
    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(CF1, Q1, Bytes.toBytes("value"));
    actions.add(put3);
    actions.add(new Delete(Bytes.toBytes("row4")));
    actions.add(new Get(Bytes.toBytes("row5")));
    Object[] results = new Object[actions.size()];
    table.batch(actions, results);
    assertEquals(3, results.length);
    requestStat =
      collector.get(new Request("row3".getBytes(), regionNameAsBytes, Operation.BATCH_PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(102, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat = collector
      .get(new Request("row4".getBytes(), regionNameAsBytes, Operation.BATCH_DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(102, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat =
      collector.get(new Request("row5".getBytes(), regionNameAsBytes, Operation.BATCH_GET, client));
    assertEquals(1, requestStat.counts());
    assertEquals(102, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify gets
    List<Get> gets = new ArrayList<>();
    gets.add(new Get(Bytes.toBytes("row6")));
    gets.add(new Get(Bytes.toBytes("row7")));
    table.get(gets);
    requestStat =
      collector.get(new Request("row6".getBytes(), regionNameAsBytes, Operation.BATCH_GET, client));
    assertEquals(1, requestStat.counts());
    assertEquals(20, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat =
      collector.get(new Request("row7".getBytes(), regionNameAsBytes, Operation.BATCH_GET, client));
    assertEquals(1, requestStat.counts());
    assertEquals(20, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify batch increment and append
    actions = new ArrayList<>();
    actions.add(new Increment(Bytes.toBytes("row8")).addColumn(CF1, Q1, 1));
    actions.add(new Append(Bytes.toBytes("row9")).addColumn(CF1, Q1, Bytes.toBytes("value")));
    results = new Object[actions.size()];
    table.batch(actions, results);
    requestStat = collector
      .get(new Request("row8".getBytes(), regionNameAsBytes, Operation.BATCH_INCREMENT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(182, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat = collector
      .get(new Request("row9".getBytes(), regionNameAsBytes, Operation.BATCH_APPEND, client));
    assertEquals(1, requestStat.counts());
    assertEquals(182, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    // Verify mutateRow
    RowMutations rm = new RowMutations(Bytes.toBytes("row10"));
    rm.add(new Put(Bytes.toBytes("row10")).addColumn(CF1, Q1, Bytes.toBytes("value")));
    rm.add(new Delete(Bytes.toBytes("row10")).addColumn(CF1, Q2));
    table.mutateRow(rm);
    requestStat = collector
      .get(new Request("row10".getBytes(), regionNameAsBytes, Operation.BATCH_PUT, client));
    assertEquals(1, requestStat.counts());
    assertEquals(20, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);
    requestStat = collector
      .get(new Request("row10".getBytes(), regionNameAsBytes, Operation.BATCH_DELETE, client));
    assertEquals(1, requestStat.counts());
    assertEquals(20, requestStat.responseSizeSumBytes());
    assertTrue(requestStat.responseTimeSumMs() > 0);

    EnvironmentEdgeManager.reset();
  }
}
