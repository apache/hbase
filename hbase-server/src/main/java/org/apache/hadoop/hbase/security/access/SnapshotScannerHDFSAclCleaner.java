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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a file cleaner that checks if a empty directory with no subdirs and subfiles is
 * deletable when user scan snapshot feature is enabled
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Evolving
public class SnapshotScannerHDFSAclCleaner extends BaseHFileCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotScannerHDFSAclCleaner.class);

  private HMaster master;
  private boolean userScanSnapshotEnabled = false;

  @Override
  public void init(Map<String, Object> params) {
    if (params != null && params.containsKey(HMaster.MASTER)) {
      this.master = (HMaster) params.get(HMaster.MASTER);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    userScanSnapshotEnabled = SnapshotScannerHDFSAclHelper.isAclSyncToHdfsEnabled(conf);
  }

  @Override
  protected boolean isFileDeletable(FileStatus fStat) {
    // This plugin does not handle the file deletions, so return true by default
    return true;
  }

  @Override
  public boolean isEmptyDirDeletable(Path dir) {
    if (userScanSnapshotEnabled) {
      /*
       * If user scan snapshot feature is enabled(see HBASE-21995), then when namespace or table
       * exists, the archive namespace or table directories should not be deleted because the HDFS
       * acls are set at these directories; the archive data directory should not be deleted because
       * the HDFS acls of global permission is set at this directory.
       */
      return isEmptyArchiveDirDeletable(dir);
    }
    return true;
  }

  private boolean isEmptyArchiveDirDeletable(Path dir) {
    try {
      if (isArchiveDataDir(dir)) {
        return false;
      } else if (isArchiveNamespaceDir(dir) && namespaceExists(dir.getName())) {
        return false;
      } else if (isArchiveTableDir(dir)
          && tableExists(TableName.valueOf(dir.getParent().getName(), dir.getName()))) {
        return false;
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Check if empty dir {} is deletable error", dir, e);
      return false;
    }
  }

  @InterfaceAudience.Private
  static boolean isArchiveDataDir(Path path) {
    if (path != null && path.getName().equals(HConstants.BASE_NAMESPACE_DIR)) {
      Path parent = path.getParent();
      return parent != null && parent.getName().equals(HConstants.HFILE_ARCHIVE_DIRECTORY);
    }
    return false;
  }

  @InterfaceAudience.Private
  static boolean isArchiveNamespaceDir(Path path) {
    return path != null && isArchiveDataDir(path.getParent());
  }

  @InterfaceAudience.Private
  static boolean isArchiveTableDir(Path path) {
    return path != null && isArchiveNamespaceDir(path.getParent());
  }

  private boolean namespaceExists(String namespace) throws IOException {
    return master != null && master.listNamespaces().contains(namespace);
  }

  private boolean tableExists(TableName tableName) throws IOException {
    return master != null && master.getTableDescriptors().exists(tableName);
  }
}
