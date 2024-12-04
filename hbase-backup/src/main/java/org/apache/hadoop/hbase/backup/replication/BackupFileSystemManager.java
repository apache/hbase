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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class BackupFileSystemManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupFileSystemManager.class);

  public static final String WALS_DIR = "WALs";
  public static final String BULKLOAD_FILES_DIR = "bulk-load-files";
  private final String peerId;
  private final FileSystem backupFs;
  private final Path backupRootDir;
  private Path walsDir;
  private Path bulkLoadFilesDir;

  public BackupFileSystemManager(String peerId, Configuration conf, String backupRootDirStr)
    throws IOException {
    this.peerId = peerId;
    this.backupRootDir = new Path(backupRootDirStr);
    this.backupFs = FileSystem.get(backupRootDir.toUri(), conf);
    initBackupDirectories();
  }

  private void initBackupDirectories() throws IOException {
    LOG.info("{} Initializing backup directories under root: {}", Utils.logPeerId(peerId),
      backupRootDir);
    try {
      walsDir = createDirectoryIfNotExists(WALS_DIR);
      bulkLoadFilesDir = createDirectoryIfNotExists(BULKLOAD_FILES_DIR);
    } catch (IOException e) {
      LOG.error("{} Failed to initialize backup directories: {}", Utils.logPeerId(peerId),
        e.getMessage(), e);
      throw e;
    }
  }

  private Path createDirectoryIfNotExists(String dirName) throws IOException {
    Path dirPath = new Path(backupRootDir, dirName);
    if (backupFs.exists(dirPath)) {
      LOG.info("{} Directory already exists: {}", Utils.logPeerId(peerId), dirPath);
    } else {
      backupFs.mkdirs(dirPath);
      LOG.info("{} Successfully created directory: {}", Utils.logPeerId(peerId), dirPath);
    }
    return dirPath;
  }

  public Path getWalsDir() {
    return walsDir;
  }

  public Path getBulkLoadFilesDir() {
    return bulkLoadFilesDir;
  }

  public FileSystem getBackupFs() {
    return backupFs;
  }
}
