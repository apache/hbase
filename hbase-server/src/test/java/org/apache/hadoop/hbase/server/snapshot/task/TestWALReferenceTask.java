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
package org.apache.hadoop.hbase.server.snapshot.task;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test that the WAL reference task works as expected
 */
public class TestWALReferenceTask {

  private static final Log LOG = LogFactory.getLog(TestWALReferenceTask.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Test
  public void testRun() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    FileSystem fs = UTIL.getTestFileSystem();
    // setup the log dir
    Path testDir = UTIL.getDataTestDir();
    Set<String> servers = new HashSet<String>();
    Path logDir = new Path(testDir, ".logs");
    Path server1Dir = new Path(logDir, "Server1");
    servers.add(server1Dir.getName());
    Path server2Dir = new Path(logDir, "me.hbase.com,56073,1348618509968");
    servers.add(server2Dir.getName());
    // logs under server 1
    Path log1_1 = new Path(server1Dir, "me.hbase.com%2C56073%2C1348618509968.1348618520536");
    Path log1_2 = new Path(server1Dir, "me.hbase.com%2C56073%2C1348618509968.1234567890123");
    // logs under server 2
    Path log2_1 = new Path(server2Dir, "me.hbase.com%2C56074%2C1348618509998.1348618515589");
    Path log2_2 = new Path(server2Dir, "me.hbase.com%2C56073%2C1348618509968.1234567890123");

    // create all the log files
    fs.createNewFile(log1_1);
    fs.createNewFile(log1_2);
    fs.createNewFile(log2_1);
    fs.createNewFile(log2_2);

    FSUtils.logFileSystemState(fs, testDir, LOG);
    FSUtils.setRootDir(conf, testDir);
    SnapshotDescription snapshot = SnapshotDescription.newBuilder()
        .setName("testWALReferenceSnapshot").build();
    SnapshotExceptionSnare listener = Mockito.mock(SnapshotExceptionSnare.class);

    // reference all the files in the first server directory
    ReferenceServerWALsTask task = new ReferenceServerWALsTask(snapshot, listener, server1Dir,
        conf, fs);
    task.run();

    // reference all the files in the first server directory
    task = new ReferenceServerWALsTask(snapshot, listener, server2Dir, conf, fs);
    task.run();

    // verify that we got everything
    FSUtils.logFileSystemState(fs, testDir, LOG);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, testDir);
    Path snapshotLogDir = new Path(workingDir, HConstants.HREGION_LOGDIR_NAME);

    // make sure we reference the all the wal files
    TakeSnapshotUtils.verifyAllLogsGotReferenced(fs, logDir, servers, snapshot, snapshotLogDir);

    // make sure we never got an error
    Mockito.verify(listener, Mockito.atLeastOnce()).failOnError();
    Mockito.verifyNoMoreInteractions(listener);
  }
}