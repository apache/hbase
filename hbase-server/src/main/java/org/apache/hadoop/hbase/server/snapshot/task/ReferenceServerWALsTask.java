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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Reference all the WAL files under a server's WAL directory
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReferenceServerWALsTask extends SnapshotTask {
  private static final Log LOG = LogFactory.getLog(ReferenceServerWALsTask.class);
  // XXX does this need to be HasThread?
  private final FileSystem fs;
  private final Configuration conf;
  private final String serverName;
  private Path logDir;

  /**
   * @param snapshot snapshot being run
   * @param failureListener listener to check for errors while running the operation and to
   *          propagate errors found while running the task
   * @param logDir log directory for the server. Name of the directory is taken as the name of the
   *          server
   * @param conf {@link Configuration} to extract fileystem information
   * @param fs filesystem where the log files are stored and should be referenced
   * @throws IOException
   */
  public ReferenceServerWALsTask(SnapshotDescription snapshot,
      SnapshotExceptionSnare failureListener, final Path logDir, final Configuration conf,
      final FileSystem fs) throws IOException {
    super(snapshot, failureListener, "Reference WALs for server:" + logDir.getName());
    this.fs = fs;
    this.conf = conf;
    this.serverName = logDir.getName();
    this.logDir = logDir;
  }

  @Override
  public void process() throws IOException {
    // TODO switch to using a single file to reference all required WAL files
    // Iterate through each of the log files and add a reference to it.
    // assumes that all the files under the server's logs directory is a log
    FileStatus[] serverLogs = FSUtils.listStatus(fs, logDir, null);
    if (serverLogs == null) LOG.info("No logs for server directory:" + logDir
        + ", done referencing files.");

    if (LOG.isDebugEnabled()) LOG.debug("Adding references for WAL files:"
        + Arrays.toString(serverLogs));

    for (FileStatus file : serverLogs) {
      this.failOnError();

      // TODO - switch to using MonitoredTask
      // add the reference to the file
      // 0. Build a reference path based on the file name
      // get the current snapshot directory
      Path rootDir = FSUtils.getRootDir(conf);
      Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(this.snapshot, rootDir);
      Path snapshotLogDir = TakeSnapshotUtils.getSnapshotHLogsDir(snapshotDir, serverName);
      // actually store the reference on disk (small file)
      Path ref = new Path(snapshotLogDir, file.getPath().getName());
      if (!fs.createNewFile(ref)) {
        if (!fs.exists(ref)) {
          throw new IOException("Couldn't create reference for:" + file.getPath());
        }
      }
      LOG.debug("Completed WAL referencing for: " + file.getPath() + " to " + ref);
    }
    LOG.debug("Successfully completed WAL referencing for ALL files");
  }
}