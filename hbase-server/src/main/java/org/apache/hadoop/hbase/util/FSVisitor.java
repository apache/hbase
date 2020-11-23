/**
 *
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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for interacting with the hbase.root file system.
 */
@InterfaceAudience.Private
public final class FSVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(FSVisitor.class);

  public interface StoreFileVisitor {
    void storeFile(final String region, final String family, final String hfileName)
       throws IOException;
  }

  private FSVisitor() {
    // private constructor for utility class
  }

  /**
   * Iterate over the table store files
   *
   * @param fs {@link FileSystem}
   * @param tableDir {@link Path} to the table directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitTableStoreFiles(final FileSystem fs, final Path tableDir,
      final StoreFileVisitor visitor) throws IOException {
    List<FileStatus> regions = FSUtils.listStatusWithStatusFilter(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regions == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No regions under directory:" + tableDir);
      }
      return;
    }

    for (FileStatus region: regions) {
      visitRegionStoreFiles(fs, region.getPath(), visitor);
    }
  }

  /**
   * Iterate over the region store files
   *
   * @param fs {@link FileSystem}
   * @param regionDir {@link Path} to the region directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegionStoreFiles(final FileSystem fs, final Path regionDir,
      final StoreFileVisitor visitor) throws IOException {
    List<FileStatus> families = FSUtils.listStatusWithStatusFilter(fs, regionDir, new FSUtils.FamilyDirFilter(fs));
    if (families == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No families under region directory:" + regionDir);
      }
      return;
    }

    PathFilter fileFilter = new FSUtils.FileFilter(fs);
    for (FileStatus family: families) {
      Path familyDir = family.getPath();
      String familyName = familyDir.getName();

      // get all the storeFiles in the family
      FileStatus[] storeFiles = CommonFSUtils.listStatus(fs, familyDir, fileFilter);
      if (storeFiles == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No hfiles found for family: " + familyDir + ", skipping.");
        }
        continue;
      }

      for (FileStatus hfile: storeFiles) {
        Path hfilePath = hfile.getPath();
        visitor.storeFile(regionDir.getName(), familyName, hfilePath.getName());
      }
    }
  }
}
