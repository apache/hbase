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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;

public class RegionProcedureStoreTestBase {

  protected HBaseCommonTestingUtility htu;

  protected RegionProcedureStore store;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    htu.getConfiguration().setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    Path testDir = htu.getDataTestDir();
    CommonFSUtils.setWALRootDir(htu.getConfiguration(), testDir);
    store = RegionProcedureStoreTestHelper.createStore(htu.getConfiguration(), new LoadCounter());
  }

  @After
  public void tearDown() throws IOException {
    store.stop(true);
    htu.cleanupTestDir();
  }
}
