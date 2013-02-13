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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Reference all the hfiles in a region for a snapshot.
 * <p>
 * Doesn't take into acccount if the hfiles are valid or not, just keeps track of what's in the
 * region's directory.
 */
public class ReferenceRegionHFilesTask extends SnapshotTask {

  public static final Log LOG = LogFactory.getLog(ReferenceRegionHFilesTask.class);
  private final Path regiondir;
  private final FileSystem fs;
  private final PathFilter fileFilter;
  private final Path snapshotDir;

  /**
   * Reference all the files in the given region directory
   * @param snapshot snapshot for which to add references
   * @param monitor to check/send error
   * @param regionDir region directory to look for errors
   * @param fs {@link FileSystem} where the snapshot/region live
   * @param regionSnapshotDir directory in the snapshot to store region files
   */
  public ReferenceRegionHFilesTask(final SnapshotDescription snapshot,
      SnapshotExceptionSnare monitor, Path regionDir, final FileSystem fs, Path regionSnapshotDir) {
    super(snapshot, monitor, "Reference hfiles for region:" + regionDir.getName());
    this.regiondir = regionDir;
    this.fs = fs;

    this.fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        try {
          return fs.isFile(path);
        } catch (IOException e) {
          LOG.error("Failed to reach fs to check file:" + path + ", marking as not file");
          ReferenceRegionHFilesTask.this.snapshotFailure("Failed to reach fs to check file status",
            e);
          return false;
        }
      }
    };
    this.snapshotDir = regionSnapshotDir;
  }

  @Override
  public void process() throws IOException {
    FileStatus[] families = FSUtils.listStatus(fs, regiondir, new FSUtils.FamilyDirFilter(fs));

    // if no families, then we are done again
    if (families == null || families.length == 0) {
      LOG.info("No families under region directory:" + regiondir
          + ", not attempting to add references.");
      return;
    }

    // snapshot directories to store the hfile reference
    List<Path> snapshotFamilyDirs = TakeSnapshotUtils.getFamilySnapshotDirectories(snapshot,
      snapshotDir, families);

    LOG.debug("Add hfile references to snapshot directories:" + snapshotFamilyDirs);
    for (int i = 0; i < families.length; i++) {
      FileStatus family = families[i];
      Path familyDir = family.getPath();
      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir, fileFilter);

      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0) {
        LOG.debug("Not hfiles found for family: " + familyDir + ", skipping.");
        continue;
      }

      // make the snapshot's family directory
      Path snapshotFamilyDir = snapshotFamilyDirs.get(i);
      fs.mkdirs(snapshotFamilyDir);

      // create a reference for each hfile
      for (FileStatus hfile : hfiles) {
        Path referenceFile = new Path(snapshotFamilyDir, hfile.getPath().getName());
        LOG.debug("Creating reference for:" + hfile.getPath() + " at " + referenceFile);
        if (!fs.createNewFile(referenceFile)) {
          throw new IOException("Failed to create reference file:" + referenceFile);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished referencing hfiles, current region state:");
      FSUtils.logFileSystemState(fs, regiondir, LOG);
      LOG.debug("and the snapshot directory:");
      FSUtils.logFileSystemState(fs, snapshotDir, LOG);
    }
  }
}