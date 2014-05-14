/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableLockTimeoutException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DelayInducingInjectionHandler;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Category(MediumTests.class)
public class TestSchemaModificationLocks {

  private static final Log LOG =
    LogFactory.getLog(TestSchemaModificationLocks.class);

  private static final byte[] TABLE_NAME = Bytes.toBytes("TestTableLevelLocks");

  private static final byte[] FAMILY = Bytes.toBytes("f1");

  private static final byte[] NEW_FAMILY = Bytes.toBytes("f2");

  private final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();


  private HMaster getMaster() {
    return TEST_UTIL.getMiniHBaseCluster().getMaster();
  }

  public void prepareMiniCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 60000)
  public void testLockTimeoutException() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.MASTER_SCHEMA_CHANGES_LOCK_TIMEOUT_MS, 3000);
    prepareMiniCluster();
    DelayInducingInjectionHandler delayer =
      new DelayInducingInjectionHandler();
    delayer.setEventDelay(InjectionEvent.HMASTER_ALTER_TABLE, 10000);
    InjectionHandler.set(delayer);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Object> shouldFinish = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        admin.deleteColumn(TABLE_NAME, FAMILY);
        return null;
      }
    });

    delayer.awaitEvent(InjectionEvent.HMASTER_ALTER_TABLE);
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    boolean caughtTimeoutException = false;
    try {
      admin.addColumn(TABLE_NAME, new HColumnDescriptor(NEW_FAMILY));
      fail();
    } catch (TableLockTimeoutException e) {
      caughtTimeoutException = true;
    }
    assertTrue(caughtTimeoutException);
    shouldFinish.get();
  }

  @Test(timeout = 60000)
  public void testAlterAndDisable() throws Exception {
    prepareMiniCluster();
    // Send a request to alter a table, then sleep during
    // the alteration phase. In the mean time, from another
    // thread, send a request to disable, and then delete a table.

    // The requests should be serialized
    final DelayInducingInjectionHandler delayer =
      new DelayInducingInjectionHandler();
    delayer.setEventDelay(InjectionEvent.HMASTER_ALTER_TABLE, 6000);
    delayer.setEventDelay(InjectionEvent.HMASTER_DISABLE_TABLE, 3000);
    InjectionHandler.set(delayer);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future alterTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HMaster master = getMaster();
        master.addColumn(TABLE_NAME, new HColumnDescriptor(NEW_FAMILY));
        LOG.info("Added new column family");
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        assertTrue(admin.isTableEnabled(TABLE_NAME));
        HTableDescriptor tableDesc = master.getTableDescriptor(TABLE_NAME);
        assertTrue(tableDesc.getFamiliesKeys().contains(NEW_FAMILY));
        return null;
      }
    });
    Future disableTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        delayer.awaitEvent(InjectionEvent.HMASTER_ALTER_TABLE);
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        admin.disableTable(TABLE_NAME);
        assertTrue(admin.isTableDisabled(TABLE_NAME));
        admin.deleteTable(TABLE_NAME);
        assertFalse(admin.tableExists(TABLE_NAME));
        return null;
      }
    });

    try {
      disableTableFuture.get();
      alterTableFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
      throw e;
    }
  }

}
