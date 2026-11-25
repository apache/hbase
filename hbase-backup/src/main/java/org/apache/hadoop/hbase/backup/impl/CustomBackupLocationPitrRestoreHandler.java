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
package org.apache.hadoop.hbase.backup.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.PointInTimeRestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PITR restore handler that retrieves backup metadata from a custom backup root directory.
 * <p>
 * This implementation is used when the PITR request specifies a custom backup location via
 * {@code backupRootDir}.
 */
@InterfaceAudience.Private
public class CustomBackupLocationPitrRestoreHandler extends AbstractPitrRestoreHandler {
  private static final Logger LOG =
    LoggerFactory.getLogger(CustomBackupLocationPitrRestoreHandler.class);

  public CustomBackupLocationPitrRestoreHandler(Connection conn,
    PointInTimeRestoreRequest request) {
    super(conn, request);
  }

  /**
   * Retrieves completed backup entries from the given custom backup root directory and converts
   * them into {@link PitrBackupMetadata} using {@link BackupImageAdapter}.
   * <p>
   * A backup is considered complete only if its backup directory contains the
   * {@link HBackupFileSystem#BACKUP_COMPLETE_FILE_NAME} marker file. The current cluster's
   * {@link BackupSystemTable} is intentionally not consulted, since the backups in a custom
   * location may have been created by a different cluster.
   * @param request the PITR request
   * @return list of completed backup metadata entries from the custom location
   * @throws IOException if reading from the custom backup directory or checking for a backup
   *                     completion marker fails
   */
  @Override
  protected List<PitrBackupMetadata> getBackupMetadata(PointInTimeRestoreRequest request)
    throws IOException {
    Configuration conf = conn.getConfiguration();
    String backupRootDir = request.getBackupRootDir();
    LOG.info("Getting backup metadata from custom backup location: {}", backupRootDir);
    List<BackupImage> images = HBackupFileSystem.getAllBackupImages(conf, new Path(backupRootDir));
    List<PitrBackupMetadata> result = new ArrayList<>();
    for (BackupImage image : images) {
      if (HBackupFileSystem.isBackupComplete(conf, backupRootDir, image.getBackupId())) {
        result.add(new BackupImageAdapter(image));
      } else {
        LOG.warn("Skipping backup {} in {}: no {} marker file found", image.getBackupId(),
          backupRootDir, HBackupFileSystem.BACKUP_COMPLETE_FILE_NAME);
      }
    }
    return result;
  }
}
