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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

/**
 * Copy over each of the files in a region's recovered.edits directory to the region's snapshot
 * directory.
 * <p>
 * This is a serial operation over each of the files in the recovered.edits directory and also
 * streams all the bytes to the client and then back to the filesystem, so the files being copied
 * should be <b>small</b> or it will (a) suck up a lot of bandwidth, and (b) take a long time.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CopyRecoveredEditsTask extends SnapshotTask {

  private static final Log LOG = LogFactory.getLog(CopyRecoveredEditsTask.class);
  private final FileSystem fs;
  private final Path regiondir;
  private final Path outputDir;

  /**
   * @param snapshot Snapshot being taken
   * @param monitor error monitor for the snapshot
   * @param fs {@link FileSystem} where the snapshot is being taken
   * @param regionDir directory for the region to examine for edits
   * @param snapshotRegionDir directory for the region in the snapshot
   */
  public CopyRecoveredEditsTask(SnapshotDescription snapshot, ForeignExceptionDispatcher monitor,
      FileSystem fs, Path regionDir, Path snapshotRegionDir) {
    super(snapshot, monitor);
    this.fs = fs;
    this.regiondir = regionDir;
    this.outputDir = HLog.getRegionDirRecoveredEditsDir(snapshotRegionDir);
  }

  @Override
  public Void call() throws IOException {
    NavigableSet<Path> files = HLog.getSplitEditFilesSorted(this.fs, regiondir);
    if (files == null || files.size() == 0) return null;

    // copy over each file.
    // this is really inefficient (could be trivially parallelized), but is
    // really simple to reason about.
    for (Path source : files) {
      // check to see if the file is zero length, in which case we can skip it
      FileStatus stat = fs.getFileStatus(source);
      if (stat.getLen() <= 0) continue;

      // its not zero length, so copy over the file
      Path out = new Path(outputDir, source.getName());
      LOG.debug("Copying " + source + " to " + out);
      FileUtil.copy(fs, source, fs, out, true, fs.getConf());

      // check for errors to the running operation after each file
      this.rethrowException();
    }
    return null;
  }
}
