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

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Class to test AsyncAdmin.
 */
public abstract class TestAsyncAdminBase extends AbstractTestUpdateConfiguration {

  protected static final Logger LOG = LoggerFactory.getLogger(TestAsyncAdminBase.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static final byte[] FAMILY = Bytes.toBytes("testFamily");
  protected static final byte[] FAMILY_0 = Bytes.toBytes("cf0");
  protected static final byte[] FAMILY_1 = Bytes.toBytes("cf1");

  protected static AsyncConnection ASYNC_CONN;
  protected AsyncAdmin admin;

  @Parameter
  public Supplier<AsyncAdmin> getAdmin;

  private static AsyncAdmin getRawAsyncAdmin() {
    return ASYNC_CONN.getAdmin();
  }

  private static AsyncAdmin getAsyncAdmin() {
    return ASYNC_CONN.getAdmin(ForkJoinPool.commonPool());
  }

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncAdminBase::getRawAsyncAdmin },
      new Supplier<?>[] { TestAsyncAdminBase::getAsyncAdmin });
  }

  @Rule
  public TestName testName = new TestName();
  protected TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(2).
        numMasters(2).build();
    TEST_UTIL.startMiniCluster(option);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    admin = getAdmin.get();
    String methodName = testName.getMethodName();
    tableName = TableName.valueOf(methodName.substring(0, methodName.length() - 3));
  }

  @After
  public void tearDown() throws Exception {
    admin.listTableNames(Pattern.compile(tableName.getNameAsString() + ".*"), false)
      .whenCompleteAsync((tables, err) -> {
        if (tables != null) {
          tables.forEach(table -> {
            try {
              admin.disableTable(table).join();
            } catch (Exception e) {
              LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
            }
            admin.deleteTable(table).join();
          });
        }
      }, ForkJoinPool.commonPool()).join();
    if (!admin.isBalancerEnabled().join()) {
      admin.balancerSwitch(true, true);
    }
  }

  protected void createTableWithDefaultConf(TableName tableName) throws IOException {
    createTableWithDefaultConf(tableName, null);
  }

  protected void createTableWithDefaultConf(TableName tableName, int regionReplication)
      throws IOException {
    createTableWithDefaultConf(tableName, regionReplication, null, FAMILY);
  }

  protected void createTableWithDefaultConf(TableName tableName, byte[][] splitKeys)
      throws IOException {
    createTableWithDefaultConf(tableName, splitKeys, FAMILY);
  }

  protected void createTableWithDefaultConf(TableName tableName, int regionReplication,
      byte[][] splitKeys) throws IOException {
    createTableWithDefaultConf(tableName, regionReplication, splitKeys, FAMILY);
  }

  protected void createTableWithDefaultConf(TableName tableName, byte[][] splitKeys,
      byte[]... families) throws IOException {
    createTableWithDefaultConf(tableName, 1, splitKeys, families);
  }

  protected void createTableWithDefaultConf(TableName tableName, int regionReplication,
      byte[][] splitKeys, byte[]... families) throws IOException {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(regionReplication);
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    CompletableFuture<Void> future = splitKeys == null ? admin.createTable(builder.build())
      : admin.createTable(builder.build(), splitKeys);
    future.join();
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
  }
}
