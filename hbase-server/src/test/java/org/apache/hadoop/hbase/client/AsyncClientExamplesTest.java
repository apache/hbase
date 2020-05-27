/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* Examples for the HBase Async Client. The purpose of this class is provide examples
* for the CompletableFuture API that can be copied, extended, or used for inspiration.
*
* We stick mostly to simple operations like create table, get, and put, but the examples easily
* extend to swapping in or adding the other admin or data calls available.
*/
@Category({ MediumTests.class, ClientTests.class })
public class AsyncClientExamplesTest {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncClientExamplesTest.class);

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTable.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // constants for family, column, row
  public static final byte[] FAMILY1 = Bytes.toBytes("family1");
  public static final byte[] QUALIFIER1 = Bytes.toBytes("family1:col1");
  public static final byte[] QUALIFIER2 = Bytes.toBytes("family1:col2");
  public static final byte[] QUALIFIER3 = Bytes.toBytes("family1:col3");
  public static final byte[] ROW_1_S = Bytes.toBytes("row1");

  @Test
  public void createTableCustomExecutorForCallbacks() {
    // by default, result callback handling logic will be executed by the event thread
    // unless you supply an executor to handle callbacks instead

    Configuration conf = TEST_UTIL.getConfiguration();

    ExecutorService executor =
      new ThreadPoolExecutor(4, 12, 3L, TimeUnit.SECONDS, new SynchronousQueue<>(),

        new ThreadFactory() {
          AtomicInteger threadNumber = new AtomicInteger(1);
          ThreadGroup group;

          {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
          }

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, "My ResultEvent Handler", 0);
            return t;
          }
        });

    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      // create the table
      TableDescriptor td = getTableDescriptor("Table1");
      admin.createTable(td).get(30, TimeUnit.SECONDS);

      // build a customized client Table
      AsyncTableBuilder<ScanResultConsumer> tb =
        asyncConnection.getTableBuilder(td.getTableName(), executor);

      // customize the table
      tb.setReadRpcTimeout(30, TimeUnit.SECONDS);
      tb.setRpcTimeout(30, TimeUnit.SECONDS);

      AsyncTable<ScanResultConsumer> table = tb.build();

      // add one value
      Put put = new Put(ROW_1_S);
      put.addColumn(FAMILY1, QUALIFIER1, Bytes.toBytes("col1val"));
      table.put(put).exceptionally(t -> {
        LOG.error("Got an exception!", t);
        return null;
      }).get(30, TimeUnit.SECONDS);

      // get the value back
      Get get = new Get(ROW_1_S).addColumn(FAMILY1, QUALIFIER1);
      Result result = table.get(get).get(30, TimeUnit.SECONDS);

      // check it's the correct value
      assertEquals("col1val", Bytes.toString(result.getValue(FAMILY1, QUALIFIER1)));

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException((e));
      }
    }

  }

  @Test
  public void dataCheckAndMutateExample() {
    TableDescriptor td = getTableDescriptor("Table1");

    Configuration conf = TEST_UTIL.getConfiguration();

    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {
      AsyncTable<AdvancedScanResultConsumer> table = createTable(td, asyncConnection);

      // add one value
      Put put = new Put(ROW_1_S);
      put.addColumn(FAMILY1, QUALIFIER1, Bytes.toBytes("col1val"));
      table.put(put).get(30, TimeUnit.SECONDS);

      // get the value back
      Get get = new Get(ROW_1_S).addColumn(FAMILY1, QUALIFIER1);
      Result result = table.get(get).get(30, TimeUnit.SECONDS);

      // check it's the correct value
      assertEquals("col1val", Bytes.toString(result.getValue(FAMILY1, QUALIFIER1)));

      // create a filter list
      FilterList filterList = new FilterList(
        new SingleColumnValueFilter(FAMILY1, QUALIFIER1, CompareOperator.EQUAL,
          Bytes.toBytes("col1val")));

      // perform the checkAndMutate call
      boolean ok = table.checkAndMutate(ROW_1_S, filterList)
        .thenPut(new Put(ROW_1_S).addColumn(FAMILY1, QUALIFIER2, Bytes.toBytes("col2val")))
        .get(30, TimeUnit.SECONDS);

      assertTrue(ok); // should succeed

      // retrieve the value we just updated to see if it's col2val
      CompletableFuture<Result> getRequest = table.get(new Get(ROW_1_S).
        addColumn(FAMILY1, QUALIFIER2));

      AtomicReference resultString = new AtomicReference();

      // instead of handling an exception with the #get method, we can
      // also handle in the future logic
      CompletableFuture<Void> handleGetRequestResult = getRequest.thenAccept(r -> {
        resultString.set(Bytes.toString(r.getValue(FAMILY1, QUALIFIER2)));
      }).whenComplete((aVoid, throwable) -> {
        // you could also use #exceptionally to handle an exception
        if (throwable != null) {
          LOG.error("There was a problem!", throwable);
        }
      });

      // wait for a the future to complete
      handleGetRequestResult.get(30, TimeUnit.SECONDS);

      // verify the result
      assertNotNull(resultString.get());
      assertEquals("col2val", resultString.get().toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void dataParallelPutAndGet() {

    TableDescriptor td = getTableDescriptor("Table1");

    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      // create a table
      AsyncTable<AdvancedScanResultConsumer> table = createTable(td, asyncConnection);

      Put put = new Put(ROW_1_S);
      put.addColumn(FAMILY1, QUALIFIER1, Bytes.toBytes("col1val"));
      Put put2 = new Put(ROW_1_S);
      put2.addColumn(FAMILY1, QUALIFIER2, Bytes.toBytes("col2val"));
      Put put3 = new Put(ROW_1_S);
      put3.addColumn(FAMILY1, QUALIFIER3, Bytes.toBytes("col3val"));

      CompletableFuture<Void> putFuture1 = table.put(put);
      CompletableFuture<Void> putFuture2 = table.put(put);
      CompletableFuture<Void> putFuture3 = table.put(put);

      // wait for puts to finish
      CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(putFuture1, putFuture2, putFuture3);
      combinedFuture.get(30, TimeUnit.SECONDS);

      // now do gets
      CompletableFuture<Result> getFuture1 =
        table.get(new Get(ROW_1_S).addColumn(FAMILY1, QUALIFIER1));
      CompletableFuture<Result> getFuture2 =
        table.get(new Get(ROW_1_S).addColumn(FAMILY1, QUALIFIER2));
      CompletableFuture<Result> getFuture3 =
        table.get(new Get(ROW_1_S).addColumn(FAMILY1, QUALIFIER3));

      // process results
      List<Result> results =
        // join is like get but will throw a RuntimeException
        Stream.of(getFuture1, getFuture2, getFuture3).map(CompletableFuture::join)
          .collect(Collectors.toList());

      for (Result result : results) {
        System.out.println("result:" + result);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void dataBulkParallelPutAndGet() {

    TableDescriptor td = getTableDescriptor("Table1");

    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      // create a table
      AsyncTable<AdvancedScanResultConsumer> table = createTable(td, asyncConnection);

      Put put = new Put(ROW_1_S);
      put.addColumn(FAMILY1, QUALIFIER1, Bytes.toBytes("col1val"));
      Put put2 = new Put(ROW_1_S);
      put2.addColumn(FAMILY1, QUALIFIER2, Bytes.toBytes("col2val"));
      Put put3 = new Put(ROW_1_S);
      put3.addColumn(FAMILY1, QUALIFIER3, Bytes.toBytes("col3val"));

      List<Put> putList = new ArrayList<>();
      putList.add(put);
      putList.add(put2);
      putList.add(put3);

      List<CompletableFuture<Void>> putFutures = table.put(putList);


      // wait for puts to finish
      CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0]));

      // now do gets

      Get get1 = new Get(ROW_1_S);
      get1.addColumn(FAMILY1, QUALIFIER1);
      Get get2 = new Get(ROW_1_S);
      get2.addColumn(FAMILY1, QUALIFIER2);
      Get get3 = new Get(ROW_1_S);
      get3.addColumn(FAMILY1, QUALIFIER3);

      List<Get> getList = new ArrayList<>();
      getList.add(get1);
      getList.add(get2);
      getList.add(get3);

      List<CompletableFuture<Result>> getFutures = table.get(getList);

      // process results
      List<Result> results =
        // join is like get but will throw a RuntimeException
        getFutures.stream().map(CompletableFuture::join)
          .collect(Collectors.toList());

      for (Result result : results) {
        System.out.println("result:" + result);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void createTablesParallel() {

    // create table1, table2, and table3 in parallel

    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      TableDescriptor td = getTableDescriptor("Table1");
      TableDescriptor td2 = getTableDescriptor("Table2");
      TableDescriptor td3 = getTableDescriptor("Table3");

      // create table1, table2 and Table3 in parallel
      CompletableFuture<Void> createTable1 = admin.createTable(td);
      CompletableFuture<Void> createTable2 = admin.createTable(td2);
      CompletableFuture<Void> createTable3 = admin.createTable(td3);

      // wait for creates to finish
      CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(createTable1, createTable2, createTable3);
      combinedFuture.get(30, TimeUnit.SECONDS);


      assertTablesExist(Arrays.asList(new TableDescriptor[]{td, td2, td3}));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void createTablesSerial() {

    // create table1, table2, and table3 serially

    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      TableDescriptor td = getTableDescriptor("Table1");
      TableDescriptor td2 = getTableDescriptor("Table2");
      TableDescriptor td3 = getTableDescriptor("Table3");

      // create table1, table2 and Table3 in parallel
      CompletableFuture<Void> createTable = admin.createTable(td);
      CompletableFuture<Void> createTable2 = admin.createTable(td2);
      CompletableFuture<Void> createTable3 = admin.createTable(td3);

      createTable.thenCompose(aVoid -> createTable2).thenCompose(aVoid -> createTable3);

      // wait for creates to finish
      createTable.get(30, TimeUnit.SECONDS);

      assertTablesExist(Arrays.asList(new TableDescriptor[]{td, td2, td3}));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void createTableCreateTable3Conditionally() {

    // create table3 if table1 and table2 can be created in parallel

    Configuration conf = TEST_UTIL.getConfiguration();

    TableDescriptor td = getTableDescriptor("Table1");
    TableDescriptor td2 = getTableDescriptor("Table2");
    TableDescriptor td3 = getTableDescriptor("Table3");

    // get a connection
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      CompletableFuture<Void> createTable1 = admin.createTable(td);
      CompletableFuture<Void> createTable2 = admin.createTable(td2);

      // create table 1 and 2 can run in parallel - allOf indicates both must
      // complete to create table 3
      CompletableFuture<Void> createTable1And2 =
        CompletableFuture.allOf(createTable1, createTable2);

      CompletableFuture<Void> createTable1And2And3 =
        createTable1And2.thenCompose(r -> admin.createTable(td3));

      // wait for tables to be created
      try {
        createTable1And2And3.get(1, TimeUnit.MINUTES);
      } catch (ExecutionException e) {
        LOG.error("Exception while creating tables", e.getCause());
      }


      assertTablesExist(Arrays.asList(new TableDescriptor[]{td, td2, td3}));

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }

  }

  @Test
  public void createTables2AndThenDeleteOne() {

    // create table1 and table2

    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      TableDescriptor td = getTableDescriptor("Table1");
      TableDescriptor td2 = getTableDescriptor("Table2");

      // create table1 and table2 and when done, disable and delete table1
      admin.createTable(td).thenAcceptBoth(admin.createTable(td2), (r1, r2) -> {
        // we could inspect the first 2 results here if the call returned them
      }).thenCompose(r3 -> admin.disableTable(td.getTableName())
        .thenCompose(r4 -> admin.deleteTable(td.getTableName()))).get(30, TimeUnit.SECONDS);

      // OR
      //    CompletableFuture.allOf(admin.createTable(td), admin.createTable(td2)).thenCompose(
      //        r3 -> admin.disableTable(td.getTableName())
      //            .thenCompose(r4 -> admin.deleteTable(td.getTableName()))).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void dropTableSerialFutures() {

    Configuration conf = TEST_UTIL.getConfiguration();

    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      TableDescriptor td = getTableDescriptor("Table1");

      // create the table
      admin.createTable(td).get(30, TimeUnit.SECONDS);

      // if the table exists, check if it's enabled, if so, disable it, then delete it
      boolean exists = admin.tableExists(td.getTableName()).get(30, TimeUnit.SECONDS);
      if (exists) {
        boolean enabled = admin.isTableEnabled(td.getTableName()).get(30, TimeUnit.SECONDS);
        if (enabled) {
          admin.disableTable(td.getTableName()).get(30, TimeUnit.SECONDS);
        }
        admin.deleteTable(td.getTableName()).get(30, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void dropTableSingleFuture() {

    Configuration conf = TEST_UTIL.getConfiguration();
    
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf)
      .get(30, TimeUnit.SECONDS)) {

      AsyncAdmin admin = asyncConnection.getAdmin();

      // create the table
      TableDescriptor td = getTableDescriptor("Table1");
      admin.createTable(td).get(30, TimeUnit.SECONDS);

      // drop the table
      // we use compose to serially link the futures together
      admin.tableExists(td.getTableName()).thenCompose(exists -> {
        if (exists) {
          return admin.isTableEnabled(td.getTableName()).thenCompose(enabled -> {
            CompletableFuture<Boolean> future;
            if (enabled) {
              // we use thenApply to create a return value compatible with the 'exists' return type
              future = admin.disableTable(td.getTableName())
                .thenCompose((r) -> admin.deleteTable(td.getTableName())).thenApply(r -> true);
            } else {
              future = admin.deleteTable(td.getTableName()).thenApply(r -> true);
            }
            return future;
          });
        }
        // return false if the table did not exist
        return CompletableFuture.completedFuture(false);
      }).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting for rpc calls to finish.");
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Exception during rpc call", e.getCause());
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.error("Timeout waiting on rpc call", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Problem creating connection", e);
      throw new RuntimeException(e);
    }
  }

  // simple util method to create a table
  private AsyncTable<AdvancedScanResultConsumer> createTable(TableDescriptor td,
    AsyncConnection asyncConnection)
    throws InterruptedException, ExecutionException, TimeoutException {
    asyncConnection.getAdmin().createTable(td).get(30, TimeUnit.SECONDS);
    return asyncConnection.getTable(TableName.valueOf("Table1"));
  }

  // simple util method to create a table descriptor
  private TableDescriptor getTableDescriptor(String name) {
    TableDescriptor tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(name)).build();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tdb);
    ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY1);
    builder.setColumnFamily(cfdb.build());
    return builder.build();
  }

  // simple util method to assert the given tables exist
  private void assertTablesExist(List<TableDescriptor> tds)
    throws ExecutionException, InterruptedException, IOException, TimeoutException {
    Set<String> names = new HashSet<>();
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(
      TEST_UTIL.getConfiguration()).get(30, TimeUnit.SECONDS)) {

      List<TableName> tableNames = asyncConnection.getAdmin()
        .listTableNames().get(30, TimeUnit.SECONDS);

      for (TableName name : tableNames) {
        names.add(name.getNameAsString());
      }
    }
    for (TableDescriptor td : tds) {
      assert(names.contains(td.getTableName().getNameAsString()));
    }
  }


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // you can use io.netty.eventLoopThreads to control the number of threads
    // netty will use to service rpc requests on the backend

    TEST_UTIL.getConfiguration().setInt("io.netty.eventLoopThreads", 200);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException, ExecutionException {

  }

  @After
  public void tearDown()
    throws IOException, InterruptedException, ExecutionException, TimeoutException {
    try (AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(
      TEST_UTIL.getConfiguration()).get(30, TimeUnit.SECONDS)) {

      List<TableName> tableNames = asyncConnection.getAdmin().listTableNames()
        .get(30, TimeUnit.SECONDS);
      for (TableName name : tableNames) {
        asyncConnection.getAdmin().disableTable(name).get(30, TimeUnit.SECONDS);
        asyncConnection.getAdmin().deleteTable(name).get(30, TimeUnit.SECONDS);
      }
    }
  }
}
