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
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotManifestV2;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * This class helps manage legacy layout of directories and files on HDFS for HBase. The directories
 * are laid out on disk as below (Note: transient files and directories are enclosed with [],
 * multiple directories, files for namespaces, tables, regions etc. at the same directorydepth is
 * indicated by ...):
 * <p>
 * <pre>
 * Table data           ---&gt;  /hbase/{@value HConstants#BASE_NAMESPACE_DIR}/
 * Default namespace    ---&gt;    default/
 * System namespace     ---&gt;    hbase/
 * Namespace            ---&gt;    ns1/
 * Table                ---&gt;        table1/
 * Table details        ---&gt;          {@value LegacyTableDescriptor#TABLEINFO_DIR}/
 * Table info           ---&gt;            {@value LegacyTableDescriptor#TABLEINFO_FILE_PREFIX}.0000000003
 * Region name          ---&gt;          region1/
 * Region details       ---&gt;            {@value #REGION_INFO_FILE}
 * Column family        ---&gt;            cf1/
 * Store file           ---&gt;              file1
 * Store files          ---&gt;              ...
 * Column families      ---&gt;            .../
 * Regions              ---&gt;          .../
 * Tables               ---&gt;        .../
 * Namespaces           ---&gt;    .../
 * Temp                 ---&gt;  /hbase/{@value HConstants#HBASE_TEMP_DIRECTORY}/
 * Base MOB             ---&gt;  /hbase/{@value MobConstants#MOB_DIR_NAME}/
 * Snapshot             ---&gt;  /hbase/{@value HConstants#SNAPSHOT_DIR_NAME}/
 * Working              ---&gt;    {@value #SNAPSHOT_TMP_DIR_NAME}/
 * In progress snapshot ---&gt;      snap5/
 * Snapshot descriptor  ---&gt;        {@value #SNAPSHOTINFO_FILE}
 * Snapshot manifest    ---&gt;        {@value SnapshotManifest#DATA_MANIFEST_NAME}
 * Region manifest      ---&gt;        [{@value SnapshotManifestV2#SNAPSHOT_MANIFEST_PREFIX}region51]
 * Region manifests     ---&gt;        ...
 * Snapshots            ---&gt;      .../
 * Completed snapshot   ---&gt;    snap1/
 * Snapshot descriptor  ---&gt;        {@value #SNAPSHOTINFO_FILE}
 * Snapshot manifest    ---&gt;        {@value SnapshotManifest#DATA_MANIFEST_NAME}
 * OLD snapshot layout  ---&gt;    snap_old/
 * Snapshot descriptor  ---&gt;      {@value #SNAPSHOTINFO_FILE}
 * Table details        ---&gt;      {@value LegacyTableDescriptor#TABLEINFO_DIR}/
 * Table info           ---&gt;        {@value LegacyTableDescriptor#TABLEINFO_FILE_PREFIX}.0000000006
 * Snapshot region      ---&gt;      region6/
 * Region details       ---&gt;        {@value #REGION_INFO_FILE}
 * Column family        ---&gt;        cf3/
 * Store file           ---&gt;          file3
 * Store files          ---&gt;          ...
 * Column families      ---&gt;        .../
 * Regions              ---&gt;      .../
 * Logs                 ---&gt;      .logs/
 * Server name          ---&gt;        server1/
 * Log files            ---&gt;          logfile1
 * Snapshots            ---&gt;    .../
 * Archive              ---&gt;  /hbase/{@value HConstants#HFILE_ARCHIVE_DIRECTORY}/
 * </pre>
 * </p>
 */
public final class LegacyLayout {
  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  private static final String REGION_TEMP_DIR = ".tmp";

  // snapshot directory constants
  /**
   * The file contains the snapshot basic information and it is under the directory of a snapshot.
   */
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";

  /** Temporary directory under the snapshot directory to store in-progress snapshots */
  public static final String SNAPSHOT_TMP_DIR_NAME = ".tmp";

  private LegacyLayout() {}

  public static Path getDataDir(final Path rootDir) {
    return new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
  }

  public static Path getSidelineDir(final Path rootDir) {
    return new Path(rootDir, HConstants.HBCK_SIDELINEDIR_NAME);
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this directory, i.e.
   * ${hbase.rootdir}/{@value HConstants#SNAPSHOT_DIR_NAME}
   * @param rootDir hbase root directory
   * @return the base directory in which all snapshots are kept
   */
  public static Path getSnapshotDir(final Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
  }

  /**
   * Get the directory for a completed snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param rootDir hbase root directory
   * @param snapshotName name of the snapshot being taken
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final Path rootDir, final String snapshotName) {
    return new Path(getSnapshotDir(rootDir), snapshotName);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param rootDir hbase root directory
   * @param snapshot snapshot description
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final Path rootDir,
                                             final SnapshotDescription snapshot) {
    return getCompletedSnapshotDir(rootDir, snapshot.getName());
  }

  /**
   * Get the general working directory for snapshots - where they are built, where they are
   * temporarily copied on export, etc.
   * i.e.$ {hbase.rootdir}/{@value HConstants#SNAPSHOT_DIR_NAME}/{@value #SNAPSHOT_TMP_DIR_NAME}
   * @param rootDir root directory of the HBase installation
   * @return Path to the snapshot tmp directory, relative to the passed root directory
   */
  public static Path getWorkingSnapshotDir(final Path rootDir) {
    return new Path(getSnapshotDir(rootDir), SNAPSHOT_TMP_DIR_NAME);
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param rootDir root directory of the hbase installation
   * @param snapshotName name of the snapshot
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(final Path rootDir, final String snapshotName) {
    return new Path(getWorkingSnapshotDir(rootDir), snapshotName);
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param rootDir root directory of the hbase installation
   * @param snapshot snapshot that will be built
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(final Path rootDir, final SnapshotDescription snapshot) {
    return getWorkingSnapshotDir(rootDir, snapshot.getName());
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

  public static Path getRegionDir(Path tableDir, String encodedRegionName) {
    return new Path(tableDir, encodedRegionName);
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

  public static Path getMobTableDir(Path rootDir, TableName table) {
    return new Path(getMobDir(rootDir), table.getQualifierAsString());
  }

  public static Path getBulkDir(Path rootDir) {
    return new Path(rootDir, HConstants.BULKLOAD_STAGING_DIR_NAME);
  }

  public static Path getOldLogDir(final Path rootDir) {
    return new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
  }
}
