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

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncTableUseMetaReplicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableUseMetaReplicas.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("Replica");

  private static byte[] FAMILY = Bytes.toBytes("Family");

  private static byte[] QUALIFIER = Bytes.toBytes("Qual");

  private static byte[] ROW = Bytes.toBytes("Row");

  private static byte[] VALUE = Bytes.toBytes("Value");

  private static volatile boolean FAIL_PRIMARY_SCAN = false;

  public static final class FailPrimaryMetaScanCp implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
        throws IOException {
      RegionInfo region = c.getEnvironment().getRegionInfo();
      if (FAIL_PRIMARY_SCAN && TableName.isMetaTableName(region.getTable()) &&
        region.getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
        throw new IOException("Inject error");
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      FailPrimaryMetaScanCp.class.getName());
    UTIL.startMiniCluster(3);
    HBaseTestingUtility.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    try (ConnectionRegistry registry = ConnectionRegistryFactory.getRegistry(conf)) {
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(UTIL, registry);
    }
    try (Table table = UTIL.createTable(TABLE_NAME, FAMILY)) {
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
    }
    UTIL.flush(TableName.META_TABLE_NAME);
    // wait for the store file refresh so we can read the region location from secondary meta
    // replicas
    Thread.sleep(2000);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDownAfterTest() {
    // make sure we do not mess up cleanup code.
    FAIL_PRIMARY_SCAN = false;
  }

  private void testRead(boolean useMetaReplicas)
      throws IOException, InterruptedException, ExecutionException {
    FAIL_PRIMARY_SCAN = true;
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setBoolean(HConstants.USE_META_REPLICAS, useMetaReplicas);
    conf.setLong(HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT, TimeUnit.SECONDS.toMicros(1));
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      Result result = FutureUtils.get(conn.getTableBuilder(TABLE_NAME)
        .setOperationTimeout(3, TimeUnit.SECONDS).build().get(new Get(ROW)));
      assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    }
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testNotUseMetaReplicas()
      throws IOException, InterruptedException, ExecutionException {
    testRead(false);
  }

  @Test
  public void testUseMetaReplicas() throws IOException, InterruptedException, ExecutionException {
    testRead(true);
  }
}
