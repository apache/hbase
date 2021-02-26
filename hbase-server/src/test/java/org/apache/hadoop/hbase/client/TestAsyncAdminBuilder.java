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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncAdminBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncAdminBuilder.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncAdminBuilder.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static AsyncConnection ASYNC_CONN;

  @Parameter
  public Supplier<AsyncAdminBuilder> getAdminBuilder;

  private static AsyncAdminBuilder getRawAsyncAdminBuilder() {
    return ASYNC_CONN.getAdminBuilder();
  }

  private static AsyncAdminBuilder getAsyncAdminBuilder() {
    return ASYNC_CONN.getAdminBuilder(ForkJoinPool.commonPool());
  }

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncAdminBuilder::getRawAsyncAdminBuilder },
      new Supplier<?>[] { TestAsyncAdminBuilder::getAsyncAdminBuilder });
  }

  private static final int DEFAULT_RPC_TIMEOUT = 10000;
  private static final int DEFAULT_OPERATION_TIMEOUT = 30000;
  private static final int DEFAULT_RETRIES_NUMBER = 2;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, DEFAULT_RPC_TIMEOUT);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      DEFAULT_OPERATION_TIMEOUT);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      DEFAULT_RETRIES_NUMBER);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRpcTimeout() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      TestRpcTimeoutCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster(2);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    try {
      getAdminBuilder.get().setRpcTimeout(DEFAULT_RPC_TIMEOUT / 2, TimeUnit.MILLISECONDS).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
      fail("We expect an exception here");
    } catch (Exception e) {
      // expected
    }

    try {
      getAdminBuilder.get().setRpcTimeout(DEFAULT_RPC_TIMEOUT * 2, TimeUnit.MILLISECONDS).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
    } catch (Exception e) {
      fail("The Operation should succeed, unexpected exception: " + e.getMessage());
    }
  }

  @Test
  public void testOperationTimeout() throws Exception {
    // set retry number to 100 to make sure that this test only be affected by operation timeout
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 100);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      TestOperationTimeoutCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster(2);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    try {
      getAdminBuilder.get()
          .setOperationTimeout(DEFAULT_OPERATION_TIMEOUT / 2, TimeUnit.MILLISECONDS).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
      fail("We expect an exception here");
    } catch (Exception e) {
      // expected
    }

    try {
      getAdminBuilder.get()
          .setOperationTimeout(DEFAULT_OPERATION_TIMEOUT * 2, TimeUnit.MILLISECONDS).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
    } catch (Exception e) {
      fail("The Operation should succeed, unexpected exception: " + e.getMessage());
    }
  }

  @Test
  public void testMaxRetries() throws Exception {
    // set operation timeout to 300s to make sure that this test only be affected by retry number
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 300000);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      TestMaxRetriesCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster(2);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    try {
      getAdminBuilder.get().setMaxRetries(DEFAULT_RETRIES_NUMBER / 2).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
      fail("We expect an exception here");
    } catch (Exception e) {
      // expected
    }

    try {
      getAdminBuilder.get().setMaxRetries(DEFAULT_RETRIES_NUMBER * 2).build()
          .getNamespaceDescriptor(DEFAULT_NAMESPACE_NAME_STR).get();
    } catch (Exception e) {
      fail("The Operation should succeed, unexpected exception: " + e.getMessage());
    }
  }

  public static class TestRpcTimeoutCoprocessor implements MasterCoprocessor, MasterObserver {
    public TestRpcTimeoutCoprocessor() {
    }


    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }
    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String namespace) throws IOException {
      Threads.sleep(DEFAULT_RPC_TIMEOUT);
    }
  }

  public static class TestOperationTimeoutCoprocessor implements MasterCoprocessor, MasterObserver {
    AtomicLong sleepTime = new AtomicLong(0);

    public TestOperationTimeoutCoprocessor() {
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String namespace) throws IOException {
      Threads.sleep(DEFAULT_RPC_TIMEOUT / 2);
      if (sleepTime.addAndGet(DEFAULT_RPC_TIMEOUT / 2) < DEFAULT_OPERATION_TIMEOUT) {
        throw new IOException("call fail");
      }
    }
  }

  public static class TestMaxRetriesCoprocessor implements MasterCoprocessor, MasterObserver {
    AtomicLong retryNum = new AtomicLong(0);

    public TestMaxRetriesCoprocessor() {
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String namespace) throws IOException {
      if (retryNum.getAndIncrement() < DEFAULT_RETRIES_NUMBER) {
        throw new IOException("call fail");
      }
    }
  }
}
