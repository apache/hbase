/*
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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RequestController.ReturnCode;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestSimpleRequestController {

  private static final TableName DUMMY_TABLE
          = TableName.valueOf("DUMMY_TABLE");
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static final byte[] DUMMY_BYTES_3 = "DUMMY_BYTES_3".getBytes();
  private static final ServerName SN = ServerName.valueOf("s1:1,1");
  private static final ServerName SN2 = ServerName.valueOf("s2:2,2");
  private static final HRegionInfo HRI1
          = new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_1, DUMMY_BYTES_2, false, 1);
  private static final HRegionInfo HRI2
          = new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_2, HConstants.EMPTY_END_ROW, false, 2);
  private static final HRegionInfo HRI3
          = new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_3, HConstants.EMPTY_END_ROW, false, 3);
  private static final HRegionLocation LOC1 = new HRegionLocation(HRI1, SN);
  private static final HRegionLocation LOC2 = new HRegionLocation(HRI2, SN);
  private static final HRegionLocation LOC3 = new HRegionLocation(HRI3, SN2);

  @Test
  public void testIllegalRequestHeapSize() {
    testIllegalArgument(SimpleRequestController.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, -1);
  }

  @Test
  public void testIllegalRsTasks() {
    testIllegalArgument(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS, -1);
  }

  @Test
  public void testIllegalRegionTasks() {
    testIllegalArgument(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS, -1);
  }

  @Test
  public void testIllegalSubmittedSize() {
    testIllegalArgument(SimpleRequestController.HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE, -1);
  }

  @Test
  public void testIllegalRequestRows() {
    testIllegalArgument(SimpleRequestController.HBASE_CLIENT_MAX_PERREQUEST_ROWS, -1);
  }

  private void testIllegalArgument(String key, long value) {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(key, value);
    try {
      SimpleRequestController controller = new SimpleRequestController(conf);
      fail("The " + key + " must be bigger than zero");
    } catch (IllegalArgumentException e) {
    }
  }

  private static Put createPut(long maxHeapSizePerRequest) {
    return new Put(Bytes.toBytes("row")) {
      @Override
      public long heapSize() {
        return maxHeapSizePerRequest;
      }
    };
  }

  @Test
  public void testTaskCheckerHost() throws IOException {
    final int maxTotalConcurrentTasks = 100;
    final int maxConcurrentTasksPerServer = 2;
    final int maxConcurrentTasksPerRegion = 1;
    final AtomicLong tasksInProgress = new AtomicLong(0);
    final Map<ServerName, AtomicInteger> taskCounterPerServer = new HashMap<>();
    final Map<byte[], AtomicInteger> taskCounterPerRegion = new HashMap<>();
    SimpleRequestController.TaskCountChecker countChecker = new SimpleRequestController.TaskCountChecker(
            maxTotalConcurrentTasks,
            maxConcurrentTasksPerServer,
            maxConcurrentTasksPerRegion,
            tasksInProgress, taskCounterPerServer, taskCounterPerRegion);
    final long maxHeapSizePerRequest = 2 * 1024 * 1024;
    // unlimiited
    SimpleRequestController.RequestHeapSizeChecker sizeChecker = new SimpleRequestController.RequestHeapSizeChecker(maxHeapSizePerRequest);
    RequestController.Checker checker = SimpleRequestController.newChecker(Arrays.asList(countChecker, sizeChecker));
    ReturnCode loc1Code = checker.canTakeRow(LOC1, createPut(maxHeapSizePerRequest));
    assertEquals(ReturnCode.INCLUDE, loc1Code);

    ReturnCode loc1Code_2 = checker.canTakeRow(LOC1, createPut(maxHeapSizePerRequest));
    // rejected for size
    assertNotEquals(ReturnCode.INCLUDE, loc1Code_2);

    ReturnCode loc2Code = checker.canTakeRow(LOC2, createPut(maxHeapSizePerRequest));
    // rejected for size
    assertNotEquals(ReturnCode.INCLUDE, loc2Code);

    // fill the task slots for LOC3.
    taskCounterPerRegion.put(LOC3.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(LOC3.getServerName(), new AtomicInteger(100));

    ReturnCode loc3Code = checker.canTakeRow(LOC3, createPut(1L));
    // rejected for count
    assertNotEquals(ReturnCode.INCLUDE, loc3Code);

    // release the task slots for LOC3.
    taskCounterPerRegion.put(LOC3.getRegionInfo().getRegionName(), new AtomicInteger(0));
    taskCounterPerServer.put(LOC3.getServerName(), new AtomicInteger(0));

    ReturnCode loc3Code_2 = checker.canTakeRow(LOC3, createPut(1L));
    assertEquals(ReturnCode.INCLUDE, loc3Code_2);
  }

  @Test
  public void testRequestHeapSizeChecker() throws IOException {
    final long maxHeapSizePerRequest = 2 * 1024 * 1024;
    SimpleRequestController.RequestHeapSizeChecker checker
            = new SimpleRequestController.RequestHeapSizeChecker(maxHeapSizePerRequest);

    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, maxHeapSizePerRequest);
      assertEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, maxHeapSizePerRequest);
      assertEquals(ReturnCode.INCLUDE, code);
    }

    // accept the data located on LOC1 region.
    ReturnCode acceptCode = checker.canTakeOperation(LOC1, maxHeapSizePerRequest);
    assertEquals(ReturnCode.INCLUDE, acceptCode);
    checker.notifyFinal(acceptCode, LOC1, maxHeapSizePerRequest);

    // the sn server reachs the limit.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, maxHeapSizePerRequest);
      assertNotEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, maxHeapSizePerRequest);
      assertNotEquals(ReturnCode.INCLUDE, code);
    }

    // the request to sn2 server should be accepted.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC3, maxHeapSizePerRequest);
      assertEquals(ReturnCode.INCLUDE, code);
    }

    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, maxHeapSizePerRequest);
      assertEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, maxHeapSizePerRequest);
      assertEquals(ReturnCode.INCLUDE, code);
    }
  }

  @Test
  public void testRequestRowsChecker() throws IOException {
    final long maxRowCount = 100;
    SimpleRequestController.RequestRowsChecker checker
      = new SimpleRequestController.RequestRowsChecker(maxRowCount);

    final long heapSizeOfRow = 100; //unused
    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
    }

    // accept the data located on LOC1 region.
    for (int i = 0; i != maxRowCount; ++i) {
      ReturnCode acceptCode = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, acceptCode);
      checker.notifyFinal(acceptCode, LOC1, heapSizeOfRow);
    }

    // the sn server reachs the limit.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertNotEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, heapSizeOfRow);
      assertNotEquals(ReturnCode.INCLUDE, code);
    }

    // the request to sn2 server should be accepted.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC3, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
    }

    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(LOC2, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
    }
  }

  @Test
  public void testSubmittedSizeChecker() {
    final long maxHeapSizeSubmit = 2 * 1024 * 1024;
    SimpleRequestController.SubmittedSizeChecker checker
            = new SimpleRequestController.SubmittedSizeChecker(maxHeapSizeSubmit);

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(LOC1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }

    for (int i = 0; i != 10; ++i) {
      checker.notifyFinal(ReturnCode.INCLUDE, LOC1, maxHeapSizeSubmit);
    }

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(LOC1, 100000);
      assertEquals(ReturnCode.END, include);
    }
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(LOC2, 100000);
      assertEquals(ReturnCode.END, include);
    }
    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(LOC1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }
  }

  @Test
  public void testTaskCountChecker() throws InterruptedIOException {
    long heapSizeOfRow = 12345;
    int maxTotalConcurrentTasks = 100;
    int maxConcurrentTasksPerServer = 2;
    int maxConcurrentTasksPerRegion = 1;
    AtomicLong tasksInProgress = new AtomicLong(0);
    Map<ServerName, AtomicInteger> taskCounterPerServer = new HashMap<>();
    Map<byte[], AtomicInteger> taskCounterPerRegion = new HashMap<>();
    SimpleRequestController.TaskCountChecker checker = new SimpleRequestController.TaskCountChecker(
            maxTotalConcurrentTasks,
            maxConcurrentTasksPerServer,
            maxConcurrentTasksPerRegion,
            tasksInProgress, taskCounterPerServer, taskCounterPerRegion);

    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, code);
    }
    // add LOC1 region.
    ReturnCode code = checker.canTakeOperation(LOC1, heapSizeOfRow);
    assertEquals(ReturnCode.INCLUDE, code);
    checker.notifyFinal(code, LOC1, heapSizeOfRow);

    // fill the task slots for LOC1.
    taskCounterPerRegion.put(LOC1.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(LOC1.getServerName(), new AtomicInteger(100));

    // the region was previously accepted, so it must be accpted now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, LOC1, heapSizeOfRow);
    }

    // fill the task slots for LOC3.
    taskCounterPerRegion.put(LOC3.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(LOC3.getServerName(), new AtomicInteger(100));

    // no task slots.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode excludeCode = checker.canTakeOperation(LOC3, heapSizeOfRow);
      assertNotEquals(ReturnCode.INCLUDE, excludeCode);
      checker.notifyFinal(excludeCode, LOC3, heapSizeOfRow);
    }

    // release the tasks for LOC3.
    taskCounterPerRegion.put(LOC3.getRegionInfo().getRegionName(), new AtomicInteger(0));
    taskCounterPerServer.put(LOC3.getServerName(), new AtomicInteger(0));

    // add LOC3 region.
    ReturnCode code3 = checker.canTakeOperation(LOC3, heapSizeOfRow);
    assertEquals(ReturnCode.INCLUDE, code3);
    checker.notifyFinal(code3, LOC3, heapSizeOfRow);

    // the region was previously accepted, so it must be accpted now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(LOC3, heapSizeOfRow);
      assertEquals(ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, LOC3, heapSizeOfRow);
    }

    checker.reset();
    // the region was previously accepted,
    // but checker have reseted and task slots for LOC1 is full.
    // So it must be rejected now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(LOC1, heapSizeOfRow);
      assertNotEquals(ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, LOC1, heapSizeOfRow);
    }
  }

  @Test
  public void testWaitForMaximumCurrentTasks() throws Exception {
    final AtomicInteger max = new AtomicInteger(0);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    SimpleRequestController controller = new SimpleRequestController(HBaseConfiguration.create());
    final AtomicLong tasks = controller.tasksInProgress;
    Runnable runnable = () -> {
      try {
        barrier.await();
        controller.waitForMaximumCurrentTasks(max.get(), 123, 1, null);
      } catch (InterruptedIOException e) {
        Assert.fail(e.getMessage());
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (BrokenBarrierException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    // First test that our runnable thread only exits when tasks is zero.
    Thread t = new Thread(runnable);
    t.start();
    barrier.await();
    t.join();
    // Now assert we stay running if max == zero and tasks is > 0.
    barrier.reset();
    tasks.set(1000000);
    t = new Thread(runnable);
    t.start();
    barrier.await();
    while (tasks.get() > 0) {
      assertTrue(t.isAlive());
      tasks.set(tasks.get() - 1);
    }
    t.join();
  }
}
