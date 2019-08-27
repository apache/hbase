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

package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;


/**
 * Tests for SnapshotsCleanerChore
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSnapshotCleanerChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestSnapshotCleanerChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotCleanerChore.class);

  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();

  private SnapshotManager snapshotManager;

  private Configuration getSnapshotCleanerConf() {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setInt("hbase.master.cleaner.snapshot.interval", 100);
    return conf;
  }


  @Test
  public void testSnapshotCleanerWithoutAnyCompletedSnapshot() throws IOException {
    snapshotManager = Mockito.mock(SnapshotManager.class);
    Stoppable stopper = new StoppableImplementation();
    Configuration conf = getSnapshotCleanerConf();
    SnapshotCleanerChore snapshotCleanerChore =
            new SnapshotCleanerChore(stopper, conf, snapshotManager);
    try {
      snapshotCleanerChore.chore();
    } finally {
      stopper.stop("Stopping Test Stopper");
    }
    Mockito.verify(snapshotManager, Mockito.times(0)).deleteSnapshot(Mockito.any());
  }

  @Test
  public void testSnapshotCleanerWithNoTtlExpired() throws IOException {
    snapshotManager = Mockito.mock(SnapshotManager.class);
    Stoppable stopper = new StoppableImplementation();
    Configuration conf = getSnapshotCleanerConf();
    SnapshotCleanerChore snapshotCleanerChore =
            new SnapshotCleanerChore(stopper, conf, snapshotManager);
    List<SnapshotProtos.SnapshotDescription> snapshotDescriptionList = new ArrayList<>();
    snapshotDescriptionList.add(getSnapshotDescription(-2, "snapshot01", "table01",
            EnvironmentEdgeManager.currentTime() - 100000));
    snapshotDescriptionList.add(getSnapshotDescription(10, "snapshot02", "table02",
            EnvironmentEdgeManager.currentTime()));
    Mockito.when(snapshotManager.getCompletedSnapshots()).thenReturn(snapshotDescriptionList);
    try {
      LOG.info("2 Snapshots are completed but TTL is not expired for any of them");
      snapshotCleanerChore.chore();
    } finally {
      stopper.stop("Stopping Test Stopper");
    }
    Mockito.verify(snapshotManager, Mockito.times(0)).deleteSnapshot(Mockito.any());
  }

  @Test
  public void testSnapshotCleanerWithSomeTtlExpired() throws IOException {
    snapshotManager = Mockito.mock(SnapshotManager.class);
    Stoppable stopper = new StoppableImplementation();
    Configuration conf = getSnapshotCleanerConf();
    SnapshotCleanerChore snapshotCleanerChore =
            new SnapshotCleanerChore(stopper, conf, snapshotManager);
    List<SnapshotProtos.SnapshotDescription> snapshotDescriptionList = new ArrayList<>();
    snapshotDescriptionList.add(getSnapshotDescription(10, "snapshot01", "table01", 1));
    snapshotDescriptionList.add(getSnapshotDescription(5, "snapshot02", "table02", 2));
    snapshotDescriptionList.add(getSnapshotDescription(30, "snapshot01", "table01",
            EnvironmentEdgeManager.currentTime()));
    snapshotDescriptionList.add(getSnapshotDescription(0, "snapshot02", "table02",
            EnvironmentEdgeManager.currentTime()));
    snapshotDescriptionList.add(getSnapshotDescription(40, "snapshot03", "table03",
            EnvironmentEdgeManager.currentTime()));
    Mockito.when(snapshotManager.getCompletedSnapshots()).thenReturn(snapshotDescriptionList);
    try {
      LOG.info("5 Snapshots are completed. TTL is expired for 2 them. Going to delete them");
      snapshotCleanerChore.chore();
    } finally {
      stopper.stop("Stopping Test Stopper");
    }
    Mockito.verify(snapshotManager, Mockito.times(2)).deleteSnapshot(Mockito.any());
  }

  @Test
  public void testSnapshotCleanerWithReadIOE() throws IOException {
    snapshotManager = Mockito.mock(SnapshotManager.class);
    Stoppable stopper = new StoppableImplementation();
    Configuration conf = new HBaseTestingUtility().getConfiguration();
    SnapshotCleanerChore snapshotCleanerChore =
            new SnapshotCleanerChore(stopper, conf, snapshotManager);
    Mockito.when(snapshotManager.getCompletedSnapshots()).thenThrow(IOException.class);
    try {
      LOG.info("While getting completed Snapshots, IOException would occur. Hence, No Snapshot"
              + " should be deleted");
      snapshotCleanerChore.chore();
    } finally {
      stopper.stop("Stopping Test Stopper");
    }
    Mockito.verify(snapshotManager, Mockito.times(0)).deleteSnapshot(Mockito.any());
  }

  @Test
  public void testSnapshotChoreWithTtlOutOfRange() throws IOException {
    snapshotManager = Mockito.mock(SnapshotManager.class);
    Stoppable stopper = new StoppableImplementation();
    Configuration conf = getSnapshotCleanerConf();
    List<SnapshotProtos.SnapshotDescription> snapshotDescriptionList = new ArrayList<>();
    snapshotDescriptionList.add(getSnapshotDescription(Long.MAX_VALUE, "snapshot01", "table01", 1));
    snapshotDescriptionList.add(getSnapshotDescription(5, "snapshot02", "table02", 2));
    Mockito.when(snapshotManager.getCompletedSnapshots()).thenReturn(snapshotDescriptionList);
    SnapshotCleanerChore snapshotCleanerChore =
            new SnapshotCleanerChore(stopper, conf, snapshotManager);
    try {
      LOG.info("Snapshot Chore is disabled. No cleanup performed for Expired Snapshots");
      snapshotCleanerChore.chore();
    } finally {
      stopper.stop("Stopping Test Stopper");
    }
    Mockito.verify(snapshotManager, Mockito.times(1)).getCompletedSnapshots();
  }

  private SnapshotProtos.SnapshotDescription getSnapshotDescription(final long ttl,
          final String snapshotName, final String tableName, final long snapshotCreationTime) {
    SnapshotProtos.SnapshotDescription.Builder snapshotDescriptionBuilder =
            SnapshotProtos.SnapshotDescription.newBuilder();
    snapshotDescriptionBuilder.setTtl(ttl);
    snapshotDescriptionBuilder.setName(snapshotName);
    snapshotDescriptionBuilder.setTable(tableName);
    snapshotDescriptionBuilder.setType(SnapshotProtos.SnapshotDescription.Type.FLUSH);
    snapshotDescriptionBuilder.setCreationTime(snapshotCreationTime);
    return snapshotDescriptionBuilder.build();
  }

  /**
   * Simple helper class that just keeps track of whether or not its stopped.
   */
  private static class StoppableImplementation implements Stoppable {

    private volatile boolean stop = false;

    @Override
    public void stop(String why) {
      this.stop = true;
    }

    @Override
    public boolean isStopped() {
      return this.stop;
    }

  }

}