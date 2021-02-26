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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test that an HBase cluster can run on top of an existing MiniDfsCluster
 */
@Category(MediumTests.class)
public class TestHBaseOnOtherDfsCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseOnOtherDfsCluster.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testOveralyOnOtherCluster() throws Exception {
    // just run HDFS
    HBaseTestingUtility util1 = new HBaseTestingUtility();
    MiniDFSCluster dfs = util1.startMiniDFSCluster(1);

    // run HBase on that HDFS
    HBaseTestingUtility util2 = new HBaseTestingUtility();
    // set the dfs
    util2.setDFSCluster(dfs, false);
    util2.startMiniCluster();

    //ensure that they are pointed at the same place
    FileSystem fs = dfs.getFileSystem();
    FileSystem targetFs = util2.getDFSCluster().getFileSystem();
    assertFsSameUri(fs, targetFs);

    fs = FileSystem.get(util1.getConfiguration());
    targetFs = FileSystem.get(util2.getConfiguration());
    assertFsSameUri(fs, targetFs);

    Path randomFile = new Path("/"+util1.getRandomUUID());
    assertTrue(targetFs.createNewFile(randomFile));
    assertTrue(fs.exists(randomFile));

    // do a simple create/write to ensure the cluster works as expected
    byte[] family = Bytes.toBytes("testfamily");
    final TableName tablename = TableName.valueOf(name.getMethodName());
    Table table = util2.createTable(tablename, family);
    Put p = new Put(new byte[] { 1, 2, 3 });
    p.addColumn(family, null, new byte[] { 1 });
    table.put(p);

    // shutdown and make sure cleanly shutting down
    util2.shutdownMiniCluster();
    util1.shutdownMiniDFSCluster();
  }

  private void assertFsSameUri(FileSystem sourceFs, FileSystem targetFs) {
    Path source = new Path(sourceFs.getUri());
    Path target = new Path(targetFs.getUri());
    assertEquals(source, target);
  }
}
