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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureLoader;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings("deprecation")
@Category({ MasterTests.class, SmallTests.class })
public class TestRegionProcedureStoreMigration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionProcedureStoreMigration.class);

  private HBaseCommonTestingUtility htu;

  private Server server;

  private MasterRegion region;

  private RegionProcedureStore store;

  private WALProcedureStore walStore;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    htu.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    Path testDir = htu.getDataTestDir();
    CommonFSUtils.setRootDir(conf, testDir);
    walStore = new WALProcedureStore(conf, new LeaseRecovery() {

      @Override
      public void recoverFileLease(FileSystem fs, Path path) throws IOException {
      }
    });
    walStore.start(1);
    walStore.recoverLease();
    walStore.load(new LoadCounter());
    server = RegionProcedureStoreTestHelper.mockServer(conf);
    region = MasterRegionFactory.create(server);
  }

  @After
  public void tearDown() throws IOException {
    if (store != null) {
      store.stop(true);
    }
    region.close(true);
    walStore.stop(true);
    htu.cleanupTestDir();
  }

  @Test
  public void test() throws IOException {
    List<RegionProcedureStoreTestProcedure> procs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RegionProcedureStoreTestProcedure proc = new RegionProcedureStoreTestProcedure();
      walStore.insert(proc, null);
      procs.add(proc);
    }
    for (int i = 5; i < 10; i++) {
      walStore.delete(procs.get(i).getProcId());
    }
    walStore.stop(true);
    SortedSet<RegionProcedureStoreTestProcedure> loadedProcs =
      new TreeSet<>((p1, p2) -> Long.compare(p1.getProcId(), p2.getProcId()));
    MutableLong maxProcIdSet = new MutableLong(0);
    store = RegionProcedureStoreTestHelper.createStore(server, region, new ProcedureLoader() {

      @Override
      public void setMaxProcId(long maxProcId) {
        maxProcIdSet.setValue(maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        while (procIter.hasNext()) {
          RegionProcedureStoreTestProcedure proc =
            (RegionProcedureStoreTestProcedure) procIter.next();
          loadedProcs.add(proc);
        }
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        if (procIter.hasNext()) {
          fail("Found corrupted procedures");
        }
      }
    });
    assertEquals(10, maxProcIdSet.longValue());
    assertEquals(5, loadedProcs.size());
    int procId = 1;
    for (RegionProcedureStoreTestProcedure proc : loadedProcs) {
      assertEquals(procId, proc.getProcId());
      procId++;
    }
    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(htu.getConfiguration());
    Path oldProcWALDir = new Path(testDir, WALProcedureStore.MASTER_PROCEDURE_LOGDIR);
    // make sure the old proc wal directory has been deleted.
    assertFalse(fs.exists(oldProcWALDir));
  }

  @Test
  public void testMigrateWithUnsupportedProcedures() throws IOException {
    AssignProcedure assignProc = new AssignProcedure();
    assignProc.setProcId(1L);
    assignProc.setRegionInfo(RegionInfoBuilder.newBuilder(TableName.valueOf("table")).build());
    walStore.insert(assignProc, null);
    walStore.stop(true);

    try {
      store = RegionProcedureStoreTestHelper.createStore(server, region, new LoadCounter());
      fail("Should fail since AssignProcedure is not supported");
    } catch (HBaseIOException e) {
      assertThat(e.getMessage(), startsWith("Unsupported"));
    }
  }
}
