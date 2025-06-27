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

/**
 * Initializes and organizes backup directories for continuous Write-Ahead Logs (WALs) files within
 * the specified backup root directory.
 */
@InterfaceAudience.Private
public class BackupFileSystemManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupFileSystemManager.class);

  public static final String WALS_DIR = "WALs";
  private final String peerId;
  private final FileSystem backupFs;
  private final Path backupRootDir;
  private final Path walsDir;

  public BackupFileSystemManager(String peerId, Configuration conf, String backupRootDirStr)
    throws IOException {
    this.peerId = peerId;
    this.backupRootDir = new Path(backupRootDirStr);
    this.backupFs = FileSystem.get(backupRootDir.toUri(), conf);
    this.walsDir = createDirectory(WALS_DIR);
  }

  private Path createDirectory(String dirName) throws IOException {
    Path dirPath = new Path(backupRootDir, dirName);
    backupFs.mkdirs(dirPath);
    LOG.info("{} Initialized directory: {}", Utils.logPeerId(peerId), dirPath);
    return dirPath;
  }

  public Path getWalsDir() {
    return walsDir;
  }

  public FileSystem getBackupFs() {
    return backupFs;
  }
}
