/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.master.BackupLogCleaner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(LargeTests.class)
public class TestBackupLogCleaner extends TestBackupBase {
  private static final Log LOG = LogFactory.getLog(TestBackupLogCleaner.class);

  // implements all test cases in 1 test since incremental full backup/
  // incremental backup has dependencies
  @Test
  public void testBackupLogCleaner() throws Exception {

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    String tablesetFull =
        table1.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table2.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table3.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table4.getNameAsString();

    BackupSystemTable systemTable = BackupSystemTable.getTable(TEST_UTIL.getConfiguration());
    // Verify that we have no backup sessions yet
    assertFalse(systemTable.hasBackupSessions());

    List<FileStatus> walFiles = getListOfWALFiles(TEST_UTIL.getConfiguration());
    List<String> swalFiles = convert(walFiles);
    BackupLogCleaner cleaner = new BackupLogCleaner();
    cleaner.setConf(TEST_UTIL.getConfiguration());

    Iterable<FileStatus> deletable = cleaner.getDeletableFiles(walFiles);
    // We can delete all files because we do not have yet recorded backup sessions
    assertTrue(Iterables.size(deletable) == walFiles.size());

    systemTable.addWALFiles(swalFiles, "backup");
    String backupIdFull = BackupClient.create("full", BACKUP_ROOT_DIR, tablesetFull, null);
    assertTrue(checkSucceeded(backupIdFull));
    // Check one more time
    deletable = cleaner.getDeletableFiles(walFiles);
    // We can delete wal files because they were saved into hbase:backup table
    int size = Iterables.size(deletable);
    assertTrue(size == walFiles.size());

    List<FileStatus> newWalFiles = getListOfWALFiles(TEST_UTIL.getConfiguration());
    LOG.debug("WAL list after full backup");
    convert(newWalFiles);

    // New list of wal files is greater than the previous one,
    // because new wal per RS have been opened after full backup
    assertTrue(walFiles.size() < newWalFiles.size());
    // TODO : verify that result files are not walFiles collection
    Connection conn = ConnectionFactory.createConnection(conf1);
    // #2 - insert some data to table
    HTable t1 = (HTable) conn.getTable(table1);
    Put p1;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }

    t1.close();

    HTable t2 = (HTable) conn.getTable(table2);
    Put p2;
    for (int i = 0; i < 5; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }

    t2.close();

    // #3 - incremental backup for multiple tables
    String tablesetIncMultiple =
        table1.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table2.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table3.getNameAsString();

    String backupIdIncMultiple =
        BackupClient.create("incremental", BACKUP_ROOT_DIR, tablesetIncMultiple, null);
    assertTrue(checkSucceeded(backupIdIncMultiple));
    deletable = cleaner.getDeletableFiles(newWalFiles);

    assertTrue(Iterables.size(deletable) == newWalFiles.size());

    conn.close();

  }

  private List<String> convert(List<FileStatus> walFiles) {
    List<String> result = new ArrayList<String>();
    for (FileStatus fs : walFiles) {
      LOG.debug("+++WAL: " + fs.getPath().toString());
      result.add(fs.getPath().toString());
    }
    return result;
  }

  private List<FileStatus> getListOfWALFiles(Configuration c) throws IOException {
    Path logRoot = new Path(FSUtils.getRootDir(c), HConstants.HREGION_LOGDIR_NAME);
    FileSystem fs = FileSystem.get(c);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(logRoot, true);
    List<FileStatus> logFiles = new ArrayList<FileStatus>();
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.isFile() && !DefaultWALProvider.isMetaFile(lfs.getPath())) {
        logFiles.add(lfs);
        LOG.info(lfs);
      }
    }
    return logFiles;
  }

}
