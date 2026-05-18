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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Safe wrapper around {@link HFileArchiver} that guards against accidental operations on the
 * production HBase root directory.
 * <p>
 * This class is intended for use by {@link org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper}
 * and other callers that operate on temporary/restore directories rather than the live data
 * directory. Before delegating to {@link HFileArchiver}, every method validates that the target
 * paths do <b>not</b> resolve to the production root directory (as configured by
 * {@code hbase.rootdir}). If they do, the operation is refused with an {@link IOException} and a
 * prominent log message.
 * <p>
 * This is a defense-in-depth measure introduced by
 * <a href="https://issues.apache.org/jira/browse/HBASE-29435">HBASE-29435</a> to prevent the class
 * of bugs demonstrated in HBASE-29346, where a MapReduce snapshot restore accidentally archived
 * live HFiles from the production root directory.
 *
 * @see HFileArchiver
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-29435">HBASE-29435</a>
 */
@InterfaceAudience.Private
public final class RestoreSnapshotHFileArchiver {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreSnapshotHFileArchiver.class);

  private RestoreSnapshotHFileArchiver() {
    // Utility class — no instantiation.
  }

  /**
   * Archive a region, after verifying the operation does not target the production root directory.
   * <p>
   * Delegates to {@link HFileArchiver#archiveRegion(Configuration, FileSystem, RegionInfo, Path,
   * Path)} after safety validation.
   * @param conf     the configuration (used to resolve production root dir for the safety check)
   * @param fs       the file system
   * @param hri      region to archive
   * @param rootDir  root directory of the table tree (should be a temp/restore dir, not production)
   * @param tableDir table directory under {@code rootDir}
   * @throws IOException if the target is the production root dir, or if archival fails
   */
  public static void archiveRegion(Configuration conf, FileSystem fs, RegionInfo hri, Path rootDir,
    Path tableDir) throws IOException {
    validateNotProductionRootDir(conf, rootDir, "archiveRegion",
      "region=" + hri.getEncodedName());
    HFileArchiver.archiveRegion(conf, fs, hri, rootDir, tableDir);
  }

  /**
   * Archive all files in a column family directory, after verifying the operation does not target
   * the production root directory.
   * <p>
   * Delegates to
   * {@link HFileArchiver#archiveFamilyByFamilyDir(FileSystem, Configuration, RegionInfo, Path,
   * byte[])} after safety validation.
   * @param fs        the file system
   * @param conf      the configuration (used to resolve production root dir for the safety check)
   * @param parent    region hosting the family
   * @param familyDir path to the family directory to archive
   * @param family    column family name
   * @throws IOException if the target is the production root dir, or if archival fails
   */
  public static void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf, RegionInfo parent,
    Path familyDir, byte[] family) throws IOException {
    validateNotProductionRootDir(conf, familyDir, "archiveFamilyByFamilyDir",
      "region=" + parent.getEncodedName() + ", family=" + new String(family));
    HFileArchiver.archiveFamilyByFamilyDir(fs, conf, parent, familyDir, family);
  }

  /**
   * Validates that the given target path does not fall under the production HBase root directory.
   * <p>
   * The production root directory is resolved from {@code conf} via
   * {@link CommonFSUtils#getRootDir(Configuration)} (i.e., the {@code hbase.rootdir} setting). If
   * {@code targetPath} starts with (is a child of) the production root, this method throws an
   * {@link IOException} and logs an ERROR — the operation must not proceed.
   * @param conf       configuration to resolve the production root directory
   * @param targetPath the path being operated on
   * @param operation  name of the operation (for logging)
   * @param detail     additional context (region, file, family — for logging)
   * @throws IOException if {@code targetPath} is under the production root directory
   */
  static void validateNotProductionRootDir(Configuration conf, Path targetPath, String operation,
    String detail) throws IOException {
    Path productionRootDir = CommonFSUtils.getRootDir(conf);
    Path qualifiedTarget = targetPath.getFileSystem(conf).makeQualified(targetPath);
    Path qualifiedRoot = productionRootDir.getFileSystem(conf).makeQualified(productionRootDir);

    String targetStr = qualifiedTarget.toUri().getPath();
    String rootStr = qualifiedRoot.toUri().getPath();

    if (targetStr.equals(rootStr) || targetStr.startsWith(rootStr + "/")) {
      String message = "BLOCKED: " + operation + " attempted on production root directory! "
        + "targetPath=" + targetPath + ", productionRootDir=" + productionRootDir + ", " + detail
        + ". This operation has been refused to prevent accidental data loss. "
        + "See HBASE-29435 / HBASE-29346.";
      LOG.error(message);
      throw new IOException(message);
    }
  }
}
