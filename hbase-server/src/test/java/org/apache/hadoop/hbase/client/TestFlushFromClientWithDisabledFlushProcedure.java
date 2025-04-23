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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestFlushFromClientWithDisabledFlushProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFlushFromClientWithDisabledFlushProcedure.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestFlushFromClientWithDisabledFlushProcedure.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static AsyncConnection asyncConn;
  private static final byte[] FAMILY = Bytes.toBytes("info");
  private static final byte[] QUALIFIER = Bytes.toBytes("name");

  @Rule
  public TestName name = new TestName();

  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    configuration.setBoolean(MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED, false);
    TEST_UTIL.startMiniCluster(1);
    asyncConn = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(asyncConn, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i <= 10; ++i) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i));
        puts.add(put);
      }
      t.put(puts);
    }
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
    assertFalse(regions.isEmpty());
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : TEST_UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void flushTableWithNonExistingFamily() {
    AsyncAdmin admin = asyncConn.getAdmin();
    List<byte[]> families = new ArrayList<>();
    families.add(FAMILY);
    families.add(Bytes.toBytes("non_family01"));
    families.add(Bytes.toBytes("non_family02"));
    assertFalse(TEST_UTIL.getConfiguration().getBoolean(
      MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED,
      MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED_DEFAULT));
    CompletableFuture<Void> future = CompletableFuture.allOf(admin.flush(tableName, families));
    assertThrows(NoSuchColumnFamilyException.class, () -> FutureUtils.get(future));
  }
}
