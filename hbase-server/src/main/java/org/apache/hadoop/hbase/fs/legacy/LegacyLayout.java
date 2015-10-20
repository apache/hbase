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

package org.apache.hadoop.hbase.fs.legacy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;

public final class LegacyLayout {
  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  private static final String REGION_TEMP_DIR = ".tmp";

  private LegacyLayout() {}

  public static Path getDataDir(final Path rootDir) {
    return new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
  }

  public static Path getSidelineDir(Path rootDir) {
    return new Path(rootDir, HConstants.HBCK_SIDELINEDIR_NAME);
  }

  public static Path getSnapshotDir(Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
  }

  public static Path getSnapshotDir(Path baseSnapshotDir, String snapshotName) {
    return new Path(baseSnapshotDir, snapshotName);
  }

  public static Path getArchiveDir(Path rootDir) {
    return new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
  }

  public static Path getTempDir(Path rootDir) {
    return new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
  }

  public static Path getNamespaceDir(Path baseDataDir, String namespace) {
    return new Path(baseDataDir, namespace);
  }

  public static Path getTableDir(Path baseDataDir, TableName table) {
    Path nsDir = getNamespaceDir(baseDataDir, table.getNamespaceAsString());
    return new Path(nsDir, table.getQualifierAsString());
  }

  public static Path getRegionDir(Path tableDir, HRegionInfo hri) {
    return new Path(tableDir, hri.getEncodedName());
  }

  public static Path getFamilyDir(Path regionDir, String familyName) {
    return new Path(regionDir, familyName);
  }

  public static Path getStoreFile(Path familyDir, String fileName) {
    return new Path(familyDir, fileName);
  }

  public static Path getRegionInfoFile(Path regionDir) {
    return new Path(regionDir, REGION_INFO_FILE);
  }

  public static Path getRegionTempDir(Path regionDir) {
    return new Path(regionDir, REGION_TEMP_DIR);
  }

  public static Path getRegionMergesDir(Path regionDir) {
    return new Path(regionDir, REGION_MERGES_DIR);
  }

  public static Path getRegionMergesDir(Path mergeDir, HRegionInfo hri) {
    return new Path(mergeDir, hri.getEncodedName());
  }

  public static Path getRegionSplitsDir(Path regionDir) {
    return new Path(regionDir, REGION_SPLITS_DIR);
  }

  public static Path getRegionSplitsDir(Path splitDir, HRegionInfo hri) {
    return new Path(splitDir, hri.getEncodedName());
  }

  public static Path getMobDir(Path rootDir) {
    return new Path(rootDir, MobConstants.MOB_DIR_NAME);
  }

  public static Path getBulkDir(Path rootDir) {
    return new Path(rootDir, HConstants.BULKLOAD_STAGING_DIR_NAME);
  }
}
