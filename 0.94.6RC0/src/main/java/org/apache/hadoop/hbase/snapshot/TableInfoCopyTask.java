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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

/**
 * Copy the table info into the snapshot directory
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TableInfoCopyTask extends SnapshotTask {

  public static final Log LOG = LogFactory.getLog(TableInfoCopyTask.class);
  private final FileSystem fs;
  private final Path rootDir;

  /**
   * Copy the table info for the given table into the snapshot
   * @param monitor listen for errors while running the snapshot
   * @param snapshot snapshot for which we are copying the table info
   * @param fs {@link FileSystem} where the tableinfo is stored (and where the copy will be written)
   * @param rootDir root of the {@link FileSystem} where the tableinfo is stored
   */
  public TableInfoCopyTask(ForeignExceptionDispatcher monitor,
      SnapshotDescription snapshot, FileSystem fs, Path rootDir) {
    super(snapshot, monitor);
    this.rootDir = rootDir;
    this.fs = fs;
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("Running table info copy.");
    this.rethrowException();
    LOG.debug("Attempting to copy table info for snapshot:"
        + SnapshotDescriptionUtils.toString(this.snapshot));
    // get the HTable descriptor
    HTableDescriptor orig = FSTableDescriptors.getTableDescriptor(fs, rootDir,
      Bytes.toBytes(this.snapshot.getTable()));
    this.rethrowException();
    // write a copy of descriptor to the snapshot directory
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, snapshotDir, orig, false);
    LOG.debug("Finished copying tableinfo.");
    return null;
  }
}