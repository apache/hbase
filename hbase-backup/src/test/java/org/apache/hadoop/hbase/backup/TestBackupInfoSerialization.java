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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestBackupInfoSerialization {

  private static BackupInfo newIncrementalBackupInfo() {
    return new BackupInfo("backup_1234567890", BackupType.INCREMENTAL,
      new TableName[] { TableName.valueOf("t1") }, "/hbase/backup");
  }

  private static BackupInfo newFullBackupInfo() {
    return new BackupInfo("backup_1234567890", BackupType.FULL,
      new TableName[] { TableName.valueOf("t1") }, "/hbase/backup");
  }

  @Test
  public void testIncrBackupFileListSurvivesRoundTrip() throws IOException {
    List<String> walFiles = Arrays.asList("/hbase/oldWALs/host1.example.com,16020,1234567890.100",
      "/hbase/oldWALs/host2.example.com,16020,1234567890.200");

    BackupInfo original = newIncrementalBackupInfo();
    original.setIncrBackupFileList(walFiles);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertEquals(walFiles, roundTripped.getIncrBackupFileList());
  }

  @Test
  public void testIncrBackupFileListOrderIsPreserved() throws IOException {
    List<String> walFiles = Arrays.asList("/hbase/oldWALs/c.example.com,16020,1.300",
      "/hbase/oldWALs/a.example.com,16020,1.100", "/hbase/oldWALs/b.example.com,16020,1.200");

    BackupInfo original = newIncrementalBackupInfo();
    original.setIncrBackupFileList(walFiles);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertEquals(walFiles, roundTripped.getIncrBackupFileList());
  }

  @Test
  public void testUnsetIncrBackupFileListRemainsNull() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertNull(roundTripped.getIncrBackupFileList());
  }

  @Test
  public void testEmptyIncrBackupFileListRemainsNull() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();
    original.setIncrBackupFileList(Arrays.asList());

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertNull(roundTripped.getIncrBackupFileList());
  }

  @Test
  public void testEqualsDistinguishesIncrBackupFileList() throws IOException {
    BackupInfo withFiles = newIncrementalBackupInfo();
    withFiles.setIncrBackupFileList(Arrays.asList("/hbase/oldWALs/host1,16020,1.100"));

    BackupInfo withoutFiles = newIncrementalBackupInfo();

    assertTrue(!withFiles.equals(withoutFiles));
  }

  @Test
  public void testTotalBytesCopiedSurvivesRoundTrip() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();
    original.setTotalBytesCopied(9876543210L);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertEquals(9876543210L, roundTripped.getTotalBytesCopied());
  }

  @Test
  public void testNoChecksumVerifySurvivesRoundTripWhenTrue() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();
    original.setNoChecksumVerify(true);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertTrue(roundTripped.getNoChecksumVerify());
  }

  @Test
  public void testNoChecksumVerifySurvivesRoundTripWhenFalse() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();
    original.setNoChecksumVerify(false);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertFalse(roundTripped.getNoChecksumVerify());
  }

  @Test
  public void testIncrTimestampMapSurvivesRoundTrip() throws IOException {
    Map<String, Long> t1Timestamps = new HashMap<>();
    t1Timestamps.put("host1.example.com:16020", 100L);
    t1Timestamps.put("host2.example.com:16020", 200L);
    Map<String, Long> t2Timestamps = new HashMap<>();
    t2Timestamps.put("host1.example.com:16020", 300L);

    Map<TableName, Map<String, Long>> incrTimestampMap = new HashMap<>();
    incrTimestampMap.put(TableName.valueOf("t1"), t1Timestamps);
    incrTimestampMap.put(TableName.valueOf("t2"), t2Timestamps);

    BackupInfo original = newIncrementalBackupInfo();
    original.setIncrTimestampMap(incrTimestampMap);

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertEquals(incrTimestampMap, roundTripped.getIncrTimestampMap());
  }

  @Test
  public void testUnsetIncrTimestampMapRemainsNull() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertNull(roundTripped.getIncrTimestampMap());
  }

  @Test
  public void testIncrementalBackupRetainsHLogTargetDir() throws IOException {
    BackupInfo original = newIncrementalBackupInfo();

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertEquals(original.getHLogTargetDir(), roundTripped.getHLogTargetDir());
  }

  @Test
  public void testFullBackupHasNoHLogTargetDir() throws IOException {
    BackupInfo original = newFullBackupInfo();
    assertNull(original.getHLogTargetDir());

    BackupInfo roundTripped = BackupInfo.fromByteArray(original.toByteArray());

    assertNull(roundTripped.getHLogTargetDir());
  }
}
