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
package org.apache.hadoop.hbase.master.snapshot.manage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.TakeSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test basic snapshot manager functionality
 */
@Category(SmallTests.class)
public class TestSnapshotManager {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  MasterServices services = Mockito.mock(MasterServices.class);
  ExecutorService pool = Mockito.mock(ExecutorService.class);
  MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
  FileSystem fs;
  {
    try {
      fs = UTIL.getTestFileSystem();
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get test filesystem", e);
    }
  }

  private SnapshotManager getNewManager() throws KeeperException, IOException {
    Mockito.reset(services);
    Mockito.when(services.getConfiguration()).thenReturn(UTIL.getConfiguration());
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(mfs.getFileSystem()).thenReturn(fs);
    Mockito.when(mfs.getRootDir()).thenReturn(UTIL.getDataTestDir());
    return new SnapshotManager(services);
  }

  @Test
  public void testInProcess() throws KeeperException, IOException {
    SnapshotManager manager = getNewManager();
    TakeSnapshotHandler handler = Mockito.mock(TakeSnapshotHandler.class);
    assertFalse("Manager is in process when there is no current handler", manager.isTakingSnapshot());
    manager.setSnapshotHandlerForTesting(handler);
    Mockito.when(handler.isFinished()).thenReturn(false);
    assertTrue("Manager isn't in process when handler is running", manager.isTakingSnapshot());
    Mockito.when(handler.isFinished()).thenReturn(true);
    assertFalse("Manager is process when handler isn't running", manager.isTakingSnapshot());
  }
}