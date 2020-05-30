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
package org.apache.hadoop.hbase.procedure2.store.region;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;

/**
 * This runs on local filesystem. hsync and hflush are not supported. May lose data! Only use where
 * data loss is not of consequence.
 */
public class RegionProcedureStoreTestBase {

  protected HBaseCommonTestingUtility htu;

  protected MasterRegion region;

  protected RegionProcedureStore store;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    Path testDir = htu.getDataTestDir();
    CommonFSUtils.setRootDir(htu.getConfiguration(), testDir);
    Server server = RegionProcedureStoreTestHelper.mockServer(conf);
    region = MasterRegionFactory.create(server);
    store = RegionProcedureStoreTestHelper.createStore(server, region, new LoadCounter());
  }

  @After
  public void tearDown() throws IOException {
    store.stop(true);
    region.close(true);
    htu.cleanupTestDir();
  }
}
