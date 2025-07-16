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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.PointInTimeRestoreRequest;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * PITR restore handler that retrieves backup metadata from a custom backup root directory.
 * <p>
 * This implementation is used when the PITR request specifies a custom backup location via
 * {@code backupRootDir}.
 */
@InterfaceAudience.Private
public class CustomBackupLocationPitrRestoreHandler extends AbstractPitrRestoreHandler {

  public CustomBackupLocationPitrRestoreHandler(Connection conn,
    PointInTimeRestoreRequest request) {
    super(conn, request);
  }

  /**
   * Retrieves completed backup entries from the given custom backup root directory and converts
   * them into {@link PitrBackupMetadata} using {@link BackupImageAdapter}.
   * @param request the PITR request
   * @return list of completed backup metadata entries from the custom location
   * @throws IOException if reading from the custom backup directory fails
   */
  @Override
  protected List<PitrBackupMetadata> getBackupMetadata(PointInTimeRestoreRequest request)
    throws IOException {
    return HBackupFileSystem
      .getAllBackupImages(conn.getConfiguration(), new Path(request.getBackupRootDir())).stream()
      .map(BackupImageAdapter::new).collect(Collectors.toList());
  }
}
