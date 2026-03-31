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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.master.assignment.MockMasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for OldWALsDirSizeChore Here we are using the {@link MockMasterServices} to mock the Hbase
 * Master. Chore's won't be running automatically; we need to run every time.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestOldWALsDirSizeChore {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOldWALsDirSizeChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestOldWALsDirSizeChore.class);

  private MockMasterServices master;

  private static final HBaseTestingUtil HBASE_TESTING_UTILITY = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    master = new MockMasterServices(HBASE_TESTING_UTILITY.getConfiguration());
    master.start(10, null);
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
  }

  @Test
  public void testOldWALsDirSizeChore() throws IOException {
    // Assume the OldWALs directory size is initially zero as the chore hasn't run yet
    long currentOldWALsDirSize = master.getMasterWalManager().getOldWALsDirSize();
    assertEquals("Initial OldWALs directory size should be zero before running the chore", 0,
      currentOldWALsDirSize);

    int dummyFileSize = 50 * 1024 * 1024; // 50MB
    byte[] dummyData = new byte[dummyFileSize];

    // Create a dummy file in the OldWALs directory
    Path dummyFileInOldWALsDir = new Path(master.getMasterWalManager().getOldLogDir(), "dummy.txt");
    try (FSDataOutputStream outputStream =
      master.getMasterWalManager().getFileSystem().create(dummyFileInOldWALsDir)) {
      outputStream.write(dummyData);
    }

    // Run the OldWALsDirSizeChore to update the directory size
    OldWALsDirSizeChore oldWALsDirSizeChore = new OldWALsDirSizeChore(master);
    oldWALsDirSizeChore.chore();

    // Verify that the OldWALs directory size has increased by the file size
    assertEquals("OldWALs directory size after chore should be as expected", dummyFileSize,
      master.getMasterWalManager().getOldWALsDirSize());
  }
}
