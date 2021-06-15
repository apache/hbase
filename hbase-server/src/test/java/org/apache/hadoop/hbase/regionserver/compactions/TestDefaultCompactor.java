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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
 * Test class for DirectInStoreCompactor.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestDefaultCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultCompactor.class);

  @Rule
  public TestName name = new TestName();

  private final Configuration config = new Configuration();
  private HStore store;
  private final String cfName = "cf";
  private Compactor.FileDetails mockFileDetails;

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
    store = UTIL.getMiniHBaseCluster().getRegionServer(0).
      getRegions(table).get(0).getStores().get(0);
    DefaultCompactor compactor = new DefaultCompactor(config, store);
    mockFileDetails = mock(Compactor.FileDetails.class);
    StoreFileWriter writer = compactor.initWriter(mockFileDetails, false, false);
    Path tmpPath = new Path(store.getRegionFileSystem().getRegionDir(), ".tmp");
    assertEquals(new Path(tmpPath, cfName), writer.getPath().getParent());
  }

  @Test
  public void testCommitCompaction() throws Exception {
    //Performs a put, then flush to create a valid store file
    Table tbl = UTIL.getConnection().getTable(table);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("1"), Bytes.toBytes("v1"));
    tbl.put(put);
    UTIL.flush(table);
    store = UTIL.getMiniHBaseCluster().getRegionServer(0).
      getRegions(table).get(0).getStores().get(0);
    //Will move the existing file into a tmp folder,
    // so that we can use it later as parameter for Compactor.commitCompaction
    Path filePath = null;
    List<Path> tmpFilesList = new ArrayList<>();
    for(HStoreFile file : store.getStorefiles()){
      filePath = file.getPath();
      Path tmpPath = new Path(store.getRegionFileSystem().getRegionDir(), ".tmp");
      tmpPath = new Path(tmpPath, filePath.getName());
      store.getFileSystem().rename(filePath, tmpPath);
      tmpFilesList.add(tmpPath);
      break;
    }
    DefaultCompactor compactor = new DefaultCompactor(config, store);
    //pass the renamed original file, then asserts it has the proper store dir path
    List<HStoreFile> result = compactor.commitCompaction(mock(CompactionRequestImpl.class),
      tmpFilesList, null);
    assertEquals(1, result.size());
    assertEquals(filePath, result.get(0).getPath());
  }

}
