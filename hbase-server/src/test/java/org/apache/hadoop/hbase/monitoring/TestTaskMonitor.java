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
package org.apache.hadoop.hbase.monitoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestTaskMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(TestTaskMonitor.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTaskMonitor.class);

  @Test
  public void testTaskMonitorBasics() {
    TaskMonitor tm = new TaskMonitor(new Configuration());
    assertTrue("Task monitor should start empty",
        tm.getTasks().isEmpty());

    // Make a task and fetch it back out
    MonitoredTask task = tm.createStatus("Test task");
    MonitoredTask taskFromTm = tm.getTasks().get(0);

    // Make sure the state is reasonable.
    assertEquals(task.getDescription(), taskFromTm.getDescription());
    assertEquals(-1, taskFromTm.getCompletionTimestamp());
    assertEquals(MonitoredTask.State.RUNNING, taskFromTm.getState());

    // Mark it as finished
    task.markComplete("Finished!");
    assertEquals(MonitoredTask.State.COMPLETE, task.getState());

    // It should still show up in the TaskMonitor list
    assertEquals(1, tm.getTasks().size());

    // If we mark its completion time back a few minutes, it should get gced
    task.expireNow();
    assertEquals(0, tm.getTasks().size());

    tm.shutdown();
  }

  @Test
  public void testTasksGetAbortedOnLeak() throws InterruptedException {
    final TaskMonitor tm = new TaskMonitor(new Configuration());
    assertTrue("Task monitor should start empty",
        tm.getTasks().isEmpty());

    final AtomicBoolean threadSuccess = new AtomicBoolean(false);
    // Make a task in some other thread and leak it
    Thread t = new Thread() {
      @Override
      public void run() {
        MonitoredTask task = tm.createStatus("Test task");
        assertEquals(MonitoredTask.State.RUNNING, task.getState());
        threadSuccess.set(true);
      }
    };
    t.start();
    t.join();
    // Make sure the thread saw the correct state
    assertTrue(threadSuccess.get());

    // Make sure the leaked reference gets cleared
    System.gc();
    System.gc();
    System.gc();

    // Now it should be aborted
    MonitoredTask taskFromTm = tm.getTasks().get(0);
    assertEquals(MonitoredTask.State.ABORTED, taskFromTm.getState());

    tm.shutdown();
  }

  @Test
  public void testTaskLimit() throws Exception {
    TaskMonitor tm = new TaskMonitor(new Configuration());
    for (int i = 0; i < TaskMonitor.DEFAULT_MAX_TASKS + 10; i++) {
      tm.createStatus("task " + i);
    }
    // Make sure it was limited correctly
    assertEquals(TaskMonitor.DEFAULT_MAX_TASKS, tm.getTasks().size());
    // Make sure we culled the earlier tasks, not later
    // (i.e. tasks 0 through 9 should have been deleted)
    assertEquals("task 10", tm.getTasks().get(0).getDescription());
    tm.shutdown();
  }

  @Test
  public void testDoNotPurgeRPCTask() throws Exception {
    int RPCTaskNums = 10;
    TaskMonitor tm = TaskMonitor.get();
    for(int i = 0; i < RPCTaskNums; i++) {
      tm.createRPCStatus("PRCTask" + i);
    }
    for(int i = 0; i < TaskMonitor.DEFAULT_MAX_TASKS; i++) {
      tm.createStatus("otherTask" + i);
    }
    int remainRPCTask = 0;
    for(MonitoredTask task: tm.getTasks()) {
      if(task instanceof MonitoredRPCHandler) {
        remainRPCTask++;
      }
    }
    assertEquals("RPC Tasks have been purged!", RPCTaskNums, remainRPCTask);
    tm.shutdown();
  }

  @Test
  public void testWarnStuckTasks() throws Exception {
    final int RPC_WARN_TIME = 1500;
    final int MONITOR_INTERVAL = 500;
    Configuration conf = new Configuration();
    conf.setLong(TaskMonitor.RPC_WARN_TIME_KEY, RPC_WARN_TIME);
    conf.setLong(TaskMonitor.MONITOR_INTERVAL_KEY, MONITOR_INTERVAL);
    final TaskMonitor tm = new TaskMonitor(conf);
    MonitoredRPCHandler t = tm.createRPCStatus("test task");
    long beforeSetRPC = EnvironmentEdgeManager.currentTime();
    assertTrue("Validating initialization assumption", t.getWarnTime() <= beforeSetRPC);
    Thread.sleep(MONITOR_INTERVAL * 2);
    t.setRPC("testMethod", new Object[0], beforeSetRPC);
    long afterSetRPC = EnvironmentEdgeManager.currentTime();
    Thread.sleep(MONITOR_INTERVAL * 2);
    assertTrue("Validating no warn after starting RPC", t.getWarnTime() <= afterSetRPC);
    Thread.sleep(MONITOR_INTERVAL * 2);
    assertTrue("Validating warn after RPC_WARN_TIME", t.getWarnTime() > afterSetRPC);
    tm.shutdown();
  }

  @Test
  public void testGetTasksWithFilter() throws Exception {
    TaskMonitor tm = new TaskMonitor(new Configuration());
    assertTrue("Task monitor should start empty", tm.getTasks().isEmpty());
    // Create 5 general tasks
    tm.createStatus("General task1");
    tm.createStatus("General task2");
    tm.createStatus("General task3");
    tm.createStatus("General task4");
    tm.createStatus("General task5");
    // Create 5 rpc tasks, and mark 1 completed
    int length = 5;
    ArrayList<MonitoredRPCHandler> rpcHandlers = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      MonitoredRPCHandler rpcHandler = tm.createRPCStatus("Rpc task" + i);
      rpcHandlers.add(rpcHandler);
    }
    // Create rpc opertions
    byte[] row = new byte[] { 0x01 };
    Mutation m = new Put(row);
    Query q = new Scan();
    String notOperation = "for test";
    rpcHandlers.get(0).setRPC("operations", new Object[]{ m, q }, 3000);
    rpcHandlers.get(1).setRPC("operations", new Object[]{ m, q }, 3000);
    rpcHandlers.get(2).setRPC("operations", new Object[]{ m, q }, 3000);
    rpcHandlers.get(3).setRPC("operations", new Object[]{ notOperation }, 3000);
    rpcHandlers.get(4).setRPC("operations", new Object[]{ m, q }, 3000);
    MonitoredRPCHandler completed = rpcHandlers.get(4);
    completed.markComplete("Completed!");
    // Test get tasks with filter
    List<MonitoredTask> generalTasks = tm.getTasks("general");
    assertEquals(5, generalTasks.size());
    List<MonitoredTask> handlerTasks = tm.getTasks("handler");
    assertEquals(5, handlerTasks.size());
    List<MonitoredTask> rpcTasks = tm.getTasks("rpc");
    // The last rpc handler is stopped
    assertEquals(4, rpcTasks.size());
    List<MonitoredTask> operationTasks = tm.getTasks("operation");
    // Handler 3 doesn't handle Operation.
    assertEquals(3, operationTasks.size());
    tm.shutdown();
  }

  @Test
  public void testStatusJournal() {
    TaskMonitor tm = new TaskMonitor(new Configuration());
    MonitoredTask task = tm.createStatus("Test task");
    assertTrue(task.getStatusJournal().isEmpty());
    task.disableStatusJournal();
    task.setStatus("status1");
    // journal should be empty since it is disabled
    assertTrue(task.getStatusJournal().isEmpty());
    task.enableStatusJournal(true);
    // check existing status entered in journal
    assertEquals("status1", task.getStatusJournal().get(0).getStatus());
    assertTrue(task.getStatusJournal().get(0).getTimeStamp() > 0);
    task.disableStatusJournal();
    task.setStatus("status2");
    // check status 2 not added since disabled
    assertEquals(1, task.getStatusJournal().size());
    task.enableStatusJournal(false);
    // size should still be 1 since we didn't include current status
    assertEquals(1, task.getStatusJournal().size());
    task.setStatus("status3");
    assertEquals("status3", task.getStatusJournal().get(1).getStatus());
    tm.shutdown();
  }

  @Test
  public void testClone() throws Exception {
    MonitoredRPCHandlerImpl monitor = new MonitoredRPCHandlerImpl();
    monitor.abort("abort RPC");
    TestParam testParam = new TestParam("param1");
    monitor.setRPC("method1", new Object[]{ testParam }, 0);
    MonitoredRPCHandlerImpl clone = monitor.clone();
    assertEquals(clone.getDescription(), monitor.getDescription());
    assertEquals(clone.getState(), monitor.getState());
    assertEquals(clone.getStatus(), monitor.getStatus());
    assertEquals(clone.toString(), monitor.toString());
    assertEquals(clone.toMap(), monitor.toMap());
    assertEquals(clone.toJSON(), monitor.toJSON());

    // mark complete and make param dirty
    monitor.markComplete("complete RPC");
    testParam.setParam("dirtyParam");
    assertEquals(clone.getDescription(), monitor.getDescription());
    assertNotEquals(clone.getState(), monitor.getState());
    assertNotEquals(clone.getStatus(), monitor.getStatus());
    monitor.setState(MonitoredTask.State.RUNNING);
    try {
      // when markComplete, the param in monitor is set null, so toMap should fail here
      monitor.toMap();
      fail("Should not call toMap successfully, because param=null");
    } catch (Exception e) {
    }
    // the param of clone monitor should not be dirty
    assertNotEquals("[dirtyString]",
      String.valueOf(((Map<String, Object>) clone.toMap().get("rpcCall")).get("params")));

    monitor.resume("resume");
    monitor.setRPC("method2", new Object[]{new TestParam("param2")}, 1);
    assertNotEquals(((Map<String, Object>) clone.toMap().get("rpcCall")).get("params"),
      ((Map<String, Object>) monitor.toMap().get("rpcCall")).get(
        "params"));
    LOG.info(String.valueOf(clone.toMap()));
    LOG.info(String.valueOf(monitor.toMap()));
    assertNotEquals(clone.toString(), monitor.toString());
    assertNotEquals(clone.getRPCQueueTime(), monitor.getRPCQueueTime());
    assertNotEquals(clone.toMap(), monitor.toMap());
    assertNotEquals(clone.toJSON(), monitor.toJSON());
  }

  private class TestParam {
    public String param = null;

    public TestParam(String param) {
      this.param = param;
    }

    public void setParam(String param) {
      this.param = param;
    }

    @Override
    public String toString() {
      return param;
    }
  }
}

