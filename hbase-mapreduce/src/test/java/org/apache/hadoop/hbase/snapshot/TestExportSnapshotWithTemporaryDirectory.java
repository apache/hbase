/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.snapshot;

import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestExportSnapshotWithTemporaryDirectory extends TestExportSnapshot {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotWithTemporaryDirectory.class);

  protected static String TEMP_DIR = Paths.get("").toAbsolutePath().toString() + Path.SEPARATOR
      + UUID.randomUUID().toString();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestExportSnapshot.tearDownAfterClass();
    FileUtils.deleteDirectory(new File(TEMP_DIR));
  }

  public static void setUpBaseConf(Configuration conf) {
    TestExportSnapshot.setUpBaseConf(conf);
    conf.set(SnapshotDescriptionUtils.SNAPSHOT_WORKING_DIR, "file://" + TEMP_DIR + "/.tmpdir/");
  }
}