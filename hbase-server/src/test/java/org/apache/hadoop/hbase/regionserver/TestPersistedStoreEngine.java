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

import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestPersistedStoreEngine {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPersistedStoreEngine.class);

  @Rule
  public TestName name = new TestName();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] DEFAULT_STORE_BYTE = TEST_UTIL.fam1;

  private TableName tableName;
  private Configuration conf;
  private HStore store;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, true);
    TEST_UTIL.getConfiguration().set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      PersistedStoreEngine.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    store = Mockito.mock(HStore.class);
    StoreContext context = new StoreContext.Builder().build();

    conf = TEST_UTIL.getConfiguration();
    Mockito.when(store.getStoreContext()).thenReturn(context);
    Mockito.when(store.getRegionInfo()).thenReturn(RegionInfoBuilder.FIRST_META_REGIONINFO);
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd: TEST_UTIL.getAdmin().listTableDescriptors()) {
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEngine() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    createTableAndLoadData();
    StoreEngine storeEngine = TEST_UTIL.getMiniHBaseCluster().getRegions(tableName).get(0)
      .getStore(DEFAULT_STORE_BYTE).getStoreEngine();
    verifyStoreEngineAndStoreFileManager(storeEngine, PersistedStoreEngine.class,
      PersistedStoreFileManager.class);
  }

  @Test
  public void testEngineWithStorefileTrackingPersistDisabled() throws IOException {
    expectedException.expect(IOException.class);
    expectedException.expectMessage("Unable to load configured store engine '"
      + PersistedStoreEngine.class.getName() + "'");
    expectedException.expectCause(isA(IllegalArgumentException.class));
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, false);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, PersistedStoreEngine.class.getName());
    CellComparator cellComparator = new CellComparatorImpl();
    StoreEngine.create(store, conf, cellComparator);
  }

  @Test
  public void testEngineWithOnlyStorefileTrackingPersistEnabled() throws IOException {
    // just setting the storefile tracking enabled will not take any consideration for store engine
    // creation because it does not go thru PersistedStoreEngine, but the master startup will fail
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, true);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    CellComparator cellComparator = new CellComparatorImpl();
    StoreEngine storeEngine = StoreEngine.create(store, conf, cellComparator);
    verifyStoreEngineAndStoreFileManager(storeEngine, DefaultStoreEngine.class,
      DefaultStoreFileManager.class);
  }

  private void createTableAndLoadData() throws IOException {
    Table testTable = TEST_UTIL.createMultiRegionTable(tableName, DEFAULT_STORE_BYTE);
    int loadedRows = TEST_UTIL.loadTable(testTable, DEFAULT_STORE_BYTE);
    int actualCount = TEST_UTIL.countRows(testTable);
    assertEquals(loadedRows, actualCount);
  }

  private void verifyStoreEngineAndStoreFileManager(StoreEngine storeEngine, Class storeEngineClass,
    Class storeFileManagerClass) {
    StoreFileManager storeFileManager = storeEngine.getStoreFileManager();
    assertEquals(storeEngineClass, storeEngine.getClass());
    assertEquals(storeFileManagerClass, storeFileManager.getClass());
  }

}
