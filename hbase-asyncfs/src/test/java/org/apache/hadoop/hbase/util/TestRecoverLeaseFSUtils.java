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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test our recoverLease loop against mocked up filesystem.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestRecoverLeaseFSUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRecoverLeaseFSUtils.class);

  private static final HBaseCommonTestingUtility HTU = new HBaseCommonTestingUtility();
  static {
    Configuration conf = HTU.getConfiguration();
    conf.setInt("hbase.lease.recovery.first.pause", 10);
    conf.setInt("hbase.lease.recovery.pause", 10);
  }

  private static Path FILE = new Path(HTU.getDataTestDir(), "file.txt");

  /**
   * Test recover lease eventually succeeding.
   */
  @Test
  public void testRecoverLease() throws IOException {
    long startTime = EnvironmentEdgeManager.currentTime();
    HTU.getConfiguration().setInt("hbase.lease.recovery.dfs.timeout", 1000);
    CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
    Mockito.when(reporter.progress()).thenReturn(true);
    DistributedFileSystem dfs = Mockito.mock(DistributedFileSystem.class);
    // Fail four times and pass on the fifth.
    Mockito.when(dfs.recoverLease(FILE)).thenReturn(false).thenReturn(false).thenReturn(false)
      .thenReturn(false).thenReturn(true);
    RecoverLeaseFSUtils.recoverFileLease(dfs, FILE, HTU.getConfiguration(), reporter);
    Mockito.verify(dfs, Mockito.times(5)).recoverLease(FILE);
    // Make sure we waited at least hbase.lease.recovery.dfs.timeout * 3 (the first two
    // invocations will happen pretty fast... the we fall into the longer wait loop).
    assertTrue((EnvironmentEdgeManager.currentTime() - startTime)
        > (3 * HTU.getConfiguration().getInt("hbase.lease.recovery.dfs.timeout", 61000)));
  }

  /**
   * Test that isFileClosed makes us recover lease faster.
   */
  @Test
  public void testIsFileClosed() throws IOException {
    // Make this time long so it is plain we broke out because of the isFileClosed invocation.
    HTU.getConfiguration().setInt("hbase.lease.recovery.dfs.timeout", 100000);
    CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
    Mockito.when(reporter.progress()).thenReturn(true);
    IsFileClosedDistributedFileSystem dfs = Mockito.mock(IsFileClosedDistributedFileSystem.class);
    // Now make it so we fail the first two times -- the two fast invocations, then we fall into
    // the long loop during which we will call isFileClosed.... the next invocation should
    // therefore return true if we are to break the loop.
    Mockito.when(dfs.recoverLease(FILE)).thenReturn(false).thenReturn(false).thenReturn(true);
    Mockito.when(dfs.isFileClosed(FILE)).thenReturn(true);
    RecoverLeaseFSUtils.recoverFileLease(dfs, FILE, HTU.getConfiguration(), reporter);
    Mockito.verify(dfs, Mockito.times(2)).recoverLease(FILE);
    Mockito.verify(dfs, Mockito.times(1)).isFileClosed(FILE);
  }

  /**
   * Version of DFS that has HDFS-4525 in it.
   */
  private static class IsFileClosedDistributedFileSystem extends DistributedFileSystem {
    /**
     * Close status of a file. Copied over from HDFS-4525
     * @return true if file is already closed
     **/
    @Override
    public boolean isFileClosed(Path f) throws IOException {
      return false;
    }
  }
}
