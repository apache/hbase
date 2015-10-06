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

public final class LegacyLayout {
  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

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

  public static Path getRegionDir(Path baseDataDir, TableName table, HRegionInfo hri) {
    return new Path(getTableDir(baseDataDir, table), hri.getEncodedName());
  }

  public static Path getBulkDir(Path rootDir) {
    return new Path(rootDir, HConstants.BULKLOAD_STAGING_DIR_NAME);
  }
}
