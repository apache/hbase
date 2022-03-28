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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionWALRecovery extends MasterRegionTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestMasterRegionWALRecovery.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionWALRecovery.class);

  private Path masterRegionDir;

  @Override
  protected void postSetUp() throws IOException {
    Configuration conf = htu.getConfiguration();
    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    masterRegionDir = new Path(testDir, REGION_DIR_NAME);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    region
      .update(r -> r.put(new Put(Bytes.toBytes(1)).addColumn(CF1, QUALIFIER, Bytes.toBytes(1))));
    region.flush(true);

    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(htu.getConfiguration());
    region.close(false);

    Path masterRegionWalDir = new Path(masterRegionDir, HConstants.HREGION_LOGDIR_NAME);
    LOG.info("WAL dir: {}", masterRegionWalDir);
    assertTrue(fs.exists(masterRegionWalDir));
    // Make sure we have the WAL for the localhost "server"
    FileStatus[] files = fs.listStatus(masterRegionWalDir);
    LOG.info("WAL files: {}", Arrays.toString(files));
    assertEquals(1, files.length);
    LOG.info("Deleting {}", masterRegionWalDir);
    // Delete the WAL directory
    fs.delete(masterRegionWalDir, true);

    // Re-create the MasterRegion and hit the MasterRegion#open() code-path
    // (rather than bootstrap())
    createMasterRegion();

    // Make sure we can read the same data we wrote (we flushed before nuking the WALs,
    // so data should be durable)
    Result r = region.get(new Get(Bytes.toBytes(1)));
    Cell c = r.getColumnLatestCell(CF1, QUALIFIER);
    assertArrayEquals(Bytes.toBytes(1), CellUtil.cloneValue(c));
  }
}
