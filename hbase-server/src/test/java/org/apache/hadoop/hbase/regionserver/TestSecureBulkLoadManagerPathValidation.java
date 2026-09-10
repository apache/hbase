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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verify that {@link SecureBulkLoadManager#validateStagingPath} rejects paths outside the staging
 * directory.
 */
@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestSecureBulkLoadManagerPathValidation {

  private SecureBulkLoadManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "file:///tmp/hbase-test");
    manager = new SecureBulkLoadManager(conf, null);
    manager.start();
  }

  @Test
  public void itAcceptsDirectChildOfStagingDir() {
    Path valid = new Path("file:///tmp/hbase-test/staging/user__table__randomtoken");
    assertDoesNotThrow(() -> manager.validateStagingPath(valid));
  }

  @Test
  public void itRejectsPathTraversal() {
    Path traversal = new Path("file:///tmp/hbase-test/staging/../data/default/important_table");
    assertThrows(DoNotRetryIOException.class, () -> manager.validateStagingPath(traversal));
  }

  @Test
  public void itRejectsAbsolutePathOutsideStaging() {
    Path outside = new Path("file:///etc/passwd");
    assertThrows(DoNotRetryIOException.class, () -> manager.validateStagingPath(outside));
  }

  @Test
  public void itRejectsNestedChildOfStagingDir() {
    Path nested = new Path("file:///tmp/hbase-test/staging/token/deeper");
    assertThrows(DoNotRetryIOException.class, () -> manager.validateStagingPath(nested));
  }

  @Test
  public void itRejectsRelativePathTraversal() {
    Path relative = new Path("../../../etc");
    assertThrows(DoNotRetryIOException.class, () -> manager.validateStagingPath(relative));
  }

  @Test
  public void itRejectsStagingDirItself() {
    Path stagingDir = new Path("file:///tmp/hbase-test/staging");
    assertThrows(DoNotRetryIOException.class, () -> manager.validateStagingPath(stagingDir));
  }
}
