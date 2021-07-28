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
package org.apache.hadoop.hbase.regionserver.compactions;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


/**
 * Test class for DirectStoreCompactor.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestDirectStoreCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDirectStoreCompactor.class);

  @Rule
  public TestName name = new TestName();

  private final Configuration config = new Configuration();
  private final String cfName = "cf";

  private HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private TableName table;

  @Before
  public void setup() throws Exception {
    UTIL.startMiniCluster();
    table = TableName.valueOf(name.getMethodName());
    UTIL.createTable(table, Bytes.toBytes(cfName));

  }

  @After
  public void shutdown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInitWriter() throws Exception {
    HStore store = UTIL.getMiniHBaseCluster().getRegionServer(0).
      getRegions(table).get(0).getStores().get(0);
    DirectStoreCompactor compactor = new DirectStoreCompactor(config, store);
    Compactor.FileDetails mockFileDetails = mock(Compactor.FileDetails.class);
    StoreFileWriter writer = compactor.initWriter(mockFileDetails, false, false);
    //asserts the parent dir is the family dir itself, not .tmp
    assertEquals(cfName, writer.getPath().getParent().getName());
  }

  @Test
  public void testCreateFileInStoreDir() throws Exception {
    HStoreFile mockFile = mock(HStoreFile.class);
    final StringBuilder builder = new StringBuilder();
    HStore store = UTIL.getMiniHBaseCluster().getRegionServer(0).
      getRegions(table).get(0).getStores().get(0);
    DirectStoreCompactor compactor = new DirectStoreCompactor(config, store);
    Compactor.FileDetails mockFileDetails = mock(Compactor.FileDetails.class);
    StoreFileWriter writer = compactor.initWriter(mockFileDetails, false, false);
    compactor.createFileInStoreDir(writer.getPath(), p -> {
      builder.append(p.getParent().getName());
      return mockFile;
    });
    assertEquals(writer.getPath().getParent().getName(), builder.toString());
  }
}
