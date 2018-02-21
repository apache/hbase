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
package org.apache.hadoop.hbase.quotas.policies;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBulkLoadCheckingViolationPolicyEnforcement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBulkLoadCheckingViolationPolicyEnforcement.class);

  FileSystem fs;
  RegionServerServices rss;
  TableName tableName;
  SpaceViolationPolicyEnforcement policy;

  @Before
  public void setup() {
    fs = mock(FileSystem.class);
    rss = mock(RegionServerServices.class);
    tableName = TableName.valueOf("foo");
    policy = new DefaultViolationPolicyEnforcement();
  }

  @Test
  public void testFilesUnderLimit() throws Exception {
    final List<String> paths = new ArrayList<>();
    final List<FileStatus> statuses = new ArrayList<>();
    final long length = 100L * 1024L;
    for (int i = 0; i < 5; i++) {
      String path = "/" + i;
      FileStatus status = mock(FileStatus.class);
      when(fs.getFileStatus(new Path(path))).thenReturn(status);
      when(status.getLen()).thenReturn(length);
      when(status.isFile()).thenReturn(true);
      paths.add(path);
      statuses.add(status);
    }

    // Quota is not in violation now
    SpaceQuotaSnapshot snapshot = new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 0, length * 6);

    policy.initialize(rss, tableName, snapshot);

    policy.computeBulkLoadSize(fs, paths);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFileIsNotAFile() throws Exception {
    final List<String> paths = new ArrayList<>();
    String path = "/1";
    FileStatus status = mock(FileStatus.class);
    when(fs.getFileStatus(new Path(path))).thenReturn(status);
    when(status.getLen()).thenReturn(1000L);
    when(status.isFile()).thenReturn(false);
    paths.add(path);

    // Quota is not in violation now
    SpaceQuotaSnapshot snapshot = new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 0, Long.MAX_VALUE);

    policy.initialize(rss, tableName, snapshot);

    // If the file to bulk load isn't a file, this should throw an exception
    policy.computeBulkLoadSize(fs, paths);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testOneFileInBatchOverLimit() throws Exception {
    final List<String> paths = new ArrayList<>();
    final List<FileStatus> statuses = new ArrayList<>();
    final long length = 1000L * 1024L;
    for (int i = 0; i < 5; i++) {
      String path = "/" + i;
      FileStatus status = mock(FileStatus.class);
      when(fs.getFileStatus(new Path(path))).thenReturn(status);
      when(status.getLen()).thenReturn(length);
      when(status.isFile()).thenReturn(true);
      paths.add(path);
      statuses.add(status);
    }

    // Quota is not in violation now
    SpaceQuotaSnapshot snapshot = new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 0, 1024L);

    policy.initialize(rss, tableName, snapshot);

    policy.computeBulkLoadSize(fs, paths);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testSumOfFilesOverLimit() throws Exception {
    final List<String> paths = new ArrayList<>();
    final List<FileStatus> statuses = new ArrayList<>();
    final long length = 1024L;
    for (int i = 0; i < 5; i++) {
      String path = "/" + i;
      FileStatus status = mock(FileStatus.class);
      when(fs.getFileStatus(new Path(path))).thenReturn(status);
      when(status.getLen()).thenReturn(length);
      when(status.isFile()).thenReturn(true);
      paths.add(path);
      statuses.add(status);
    }

    // Quota is not in violation now, but 5*1024 files would push us to violation
    SpaceQuotaSnapshot snapshot = new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 0, 5000L);

    policy.initialize(rss, tableName, snapshot);

    policy.computeBulkLoadSize(fs, paths);
  }
}
