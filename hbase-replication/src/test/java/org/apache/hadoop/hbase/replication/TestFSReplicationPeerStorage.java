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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestFSReplicationPeerStorage extends ReplicationPeerStorageTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFSReplicationPeerStorage.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static FileSystem FS;

  private static Path DIR;

  @BeforeClass
  public static void setUp() throws Exception {
    DIR = UTIL.getDataTestDir("test_fs_peer_storage");
    CommonFSUtils.setRootDir(UTIL.getConfiguration(), DIR);
    FS = FileSystem.get(UTIL.getConfiguration());
    STORAGE = new FSReplicationPeerStorage(FS, UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Override
  protected void assertPeerNameControlException(ReplicationException e) {
    assertThat(e.getMessage(), endsWith("peer already exists"));
  }
}
