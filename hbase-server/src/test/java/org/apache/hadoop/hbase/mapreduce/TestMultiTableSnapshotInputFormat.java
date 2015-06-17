/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Category(LargeTests.class)
public class TestMultiTableSnapshotInputFormat extends MultiTableInputFormatTestBase {

  protected Path restoreDir;

  @BeforeClass
  public static void setUpSnapshots() throws Exception {

    TEST_UTIL.enableDebug(MultiTableSnapshotInputFormat.class);
    TEST_UTIL.enableDebug(MultiTableSnapshotInputFormatImpl.class);

    // take a snapshot of every table we have.
    for (String tableName : TABLES) {
      SnapshotTestingUtils
          .createSnapshotAndValidate(TEST_UTIL.getHBaseAdmin(), TableName.valueOf(tableName),
              ImmutableList.of(MultiTableInputFormatTestBase.INPUT_FAMILY), null,
              snapshotNameForTable(tableName), FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
              TEST_UTIL.getTestFileSystem(), true);
    }
  }

  @Before
  public void setUp() throws Exception {
    this.restoreDir = new Path("/tmp");

  }

  @Override
  protected void initJob(List<Scan> scans, Job job) throws IOException {
    TableMapReduceUtil
        .initMultiTableSnapshotMapperJob(getSnapshotScanMapping(scans), ScanMapper.class,
            ImmutableBytesWritable.class, ImmutableBytesWritable.class, job, true, restoreDir);
  }

  protected Map<String, Collection<Scan>> getSnapshotScanMapping(final List<Scan> scans) {
    return Multimaps.index(scans, new Function<Scan, String>() {
      @Nullable
      @Override
      public String apply(Scan input) {
        return snapshotNameForTable(
            Bytes.toStringBinary(input.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME)));
      }
    }).asMap();
  }

  public static String snapshotNameForTable(String tableName) {
    return tableName + "_snapshot";
  }

}
