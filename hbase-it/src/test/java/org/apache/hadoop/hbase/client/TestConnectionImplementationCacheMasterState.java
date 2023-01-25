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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@Category({ ClientTests.class, MediumTests.class })
@RunWith(MockitoJUnitRunner.class)
public class TestConnectionImplementationCacheMasterState {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionImplementationCacheMasterState.class);
  private static final IntegrationTestingUtility TEST_UTIL = new IntegrationTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetMaster_noCachedMasterState() throws IOException, IllegalAccessException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 0L);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    ConnectionImplementation.MasterServiceState masterServiceState = spyMasterServiceState(conn);
    conn.getMaster(); // This initializes the stubs but don't call isMasterRunning
    conn.getMaster(); // Calls isMasterRunning since stubs are initialized. Invocation 1
    conn.getMaster(); // Calls isMasterRunning since stubs are initialized. Invocation 2
    Mockito.verify(masterServiceState, Mockito.times(2)).isMasterRunning();
    conn.close();
  }

  @Test
  public void testGetMaster_masterStateCacheHit() throws IOException, IllegalAccessException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 15L);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    ConnectionImplementation.MasterServiceState masterServiceState = spyMasterServiceState(conn);
    conn.getMaster(); // This initializes the stubs but don't call isMasterRunning
    conn.getMaster(); // Uses cached value, don't call isMasterRunning
    conn.getMaster(); // Uses cached value, don't call isMasterRunning
    Mockito.verify(masterServiceState, Mockito.times(0)).isMasterRunning();
    conn.close();
  }

  @Test
  public void testGetMaster_masterStateCacheMiss()
    throws IOException, InterruptedException, IllegalAccessException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 5L);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    ConnectionImplementation.MasterServiceState masterServiceState = spyMasterServiceState(conn);
    conn.getMaster(); // This initializes the stubs but don't call isMasterRunning
    conn.getMaster(); // Uses cached value, don't call isMasterRunning
    conn.getMaster(); // Uses cached value, don't call isMasterRunning
    Thread.sleep(10000);
    conn.getMaster(); // Calls isMasterRunning after cache expiry
    Mockito.verify(masterServiceState, Mockito.times(1)).isMasterRunning();
    conn.close();
  }

  @Test
  public void testIsKeepAliveMasterConnectedAndRunning_UndeclaredThrowableException()
    throws IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 0);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    conn.getMaster(); // Initializes stubs

    ConnectionImplementation.MasterServiceState masterServiceState = spyMasterServiceState(conn);
    Mockito.doThrow(new UndeclaredThrowableException(new Exception("DUMMY EXCEPTION")))
      .when(masterServiceState).isMasterRunning();

    // Verify that masterState is "false" because of to injected exception
    boolean isKeepAliveMasterRunning =
      (boolean) getIsKeepAliveMasterConnectedAndRunningMethod().invoke(conn);
    Assert.assertFalse(isKeepAliveMasterRunning);
    conn.close();
  }

  @Test
  public void testIsKeepAliveMasterConnectedAndRunning_IOException()
    throws IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 0);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    conn.getMaster();

    ConnectionImplementation.MasterServiceState masterServiceState = spyMasterServiceState(conn);
    Mockito.doThrow(new IOException("DUMMY EXCEPTION")).when(masterServiceState).isMasterRunning();

    boolean isKeepAliveMasterRunning =
      (boolean) getIsKeepAliveMasterConnectedAndRunningMethod().invoke(conn);

    // Verify that masterState is "false" because of to injected exception
    Assert.assertFalse(isKeepAliveMasterRunning);
    conn.close();
  }

  // Spy the masterServiceState object using reflection
  private ConnectionImplementation.MasterServiceState
    spyMasterServiceState(ConnectionImplementation conn) throws IllegalAccessException {
    ConnectionImplementation.MasterServiceState spiedMasterServiceState =
      Mockito.spy(conn.getMasterServiceState());
    FieldUtils.writeDeclaredField(conn, "masterServiceState", spiedMasterServiceState, true);
    return spiedMasterServiceState;
  }

  // Get isKeepAliveMasterConnectedAndRunning using reflection
  private Method getIsKeepAliveMasterConnectedAndRunningMethod() throws NoSuchMethodException {
    Method method =
      ConnectionImplementation.class.getDeclaredMethod("isKeepAliveMasterConnectedAndRunning");
    method.setAccessible(true);
    return method;
  }
}
