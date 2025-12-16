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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.ColumnFamilyMismatchException;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.jupiter.api.BeforeAll;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

public class IncrementalBackupRestoreTestBase extends TestBackupBase {

  private static final byte[] BULKLOAD_START_KEY = new byte[] { 0x00 };
  private static final byte[] BULKLOAD_END_KEY = new byte[] { Byte.MAX_VALUE };

  @BeforeAll
  public static void setUp() throws Exception {
    provider = "multiwal";
    TestBackupBase.setUp();
  }

  protected void checkThrowsCFMismatch(IOException ex, List<TableName> tables) {
    Throwable cause = Throwables.getRootCause(ex);
    assertEquals(cause.getClass(), ColumnFamilyMismatchException.class);
    ColumnFamilyMismatchException e = (ColumnFamilyMismatchException) cause;
    assertEquals(tables, e.getMismatchedTables());
  }

  protected String takeFullBackup(List<TableName> tables, BackupAdminImpl backupAdmin)
    throws IOException {
    return takeFullBackup(tables, backupAdmin, false);
  }

  protected String takeFullBackup(List<TableName> tables, BackupAdminImpl backupAdmin,
    boolean noChecksumVerify) throws IOException {
    BackupRequest req =
      createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR, noChecksumVerify);
    String backupId = backupAdmin.backupTables(req);
    checkSucceeded(backupId);
    return backupId;
  }

  protected static Path doBulkload(TableName tn, String regionName, byte[]... fams)
    throws IOException {
    Path regionDir = createHFiles(tn, regionName, fams);
    Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> results =
      BulkLoadHFiles.create(conf1).bulkLoad(tn, regionDir);
    assertFalse(results.isEmpty());
    return regionDir;
  }

  private static Path createHFiles(TableName tn, String regionName, byte[]... fams)
    throws IOException {
    Path rootdir = CommonFSUtils.getRootDir(conf1);
    Path regionDir = CommonFSUtils.getRegionDir(rootdir, tn, regionName);

    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    fs.mkdirs(rootdir);

    for (byte[] fam : fams) {
      Path famDir = new Path(regionDir, Bytes.toString(fam));
      Path hFileDir = new Path(famDir, UUID.randomUUID().toString());
      HFileTestUtil.createHFile(conf1, fs, hFileDir, fam, qualName, BULKLOAD_START_KEY,
        BULKLOAD_END_KEY, 1000);
    }

    return regionDir;
  }

  /**
   * Check that backup manifest can be produced for a different root. Users may want to move
   * existing backups to a different location.
   */
  protected void validateRootPathCanBeOverridden(String originalPath, String backupId)
    throws IOException {
    String anotherRootDir = "/some/other/root/dir";
    Path anotherPath = new Path(anotherRootDir, backupId);
    BackupManifest.BackupImage differentLocationImage = BackupManifest.hydrateRootDir(
      HBackupFileSystem.getManifest(conf1, new Path(originalPath), backupId).getBackupImage(),
      anotherPath);
    assertEquals(differentLocationImage.getRootDir(), anotherRootDir);
    for (BackupManifest.BackupImage ancestor : differentLocationImage.getAncestors()) {
      assertEquals(anotherRootDir, ancestor.getRootDir());
    }
  }

  protected List<LocatedFileStatus> getBackupFiles() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(BACKUP_ROOT_DIR), true);
    List<LocatedFileStatus> files = new ArrayList<>();

    while (iter.hasNext()) {
      files.add(iter.next());
    }

    return files;
  }
}
