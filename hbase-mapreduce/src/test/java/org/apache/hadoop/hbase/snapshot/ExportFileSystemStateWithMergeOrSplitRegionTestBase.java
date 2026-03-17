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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.jupiter.api.TestTemplate;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public class ExportFileSystemStateWithMergeOrSplitRegionTestBase extends ExportSnapshotTestBase {

  protected ExportFileSystemStateWithMergeOrSplitRegionTestBase(boolean mob) {
    super(mob);
  }

  @TestTemplate
  public void testExportFileSystemStateWithMergeRegion() throws Exception {
    // disable compaction
    admin.compactionSwitch(false,
      admin.getRegionServers().stream().map(a -> a.getServerName()).collect(Collectors.toList()));
    // create Table
    String suffix = mob ? methodName + "-mob" : methodName;
    TableName tableName0 = TableName.valueOf("testtb-" + suffix + "-1");
    String snapshotName0 = "snaptb0-" + suffix + "-1";
    admin.createTable(
      TableDescriptorBuilder.newBuilder(tableName0)
        .setColumnFamilies(
          Lists.newArrayList(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build()))
        .build(),
      new byte[][] { Bytes.toBytes("2") });
    // put some data
    try (Table table = admin.getConnection().getTable(tableName0)) {
      table.put(new Put(Bytes.toBytes("1")).addColumn(FAMILY, null, Bytes.toBytes("1")));
      table.put(new Put(Bytes.toBytes("2")).addColumn(FAMILY, null, Bytes.toBytes("2")));
    }
    List<RegionInfo> regions = admin.getRegions(tableName0);
    assertEquals(2, regions.size());
    tableNumFiles = regions.size();
    // merge region
    admin.mergeRegionsAsync(new byte[][] { regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes() }, true).get();
    // take a snapshot
    admin.snapshot(snapshotName0, tableName0);
    // export snapshot and verify
    testExportFileSystemState(tableName0, snapshotName0, snapshotName0, tableNumFiles);
    // delete table
    TEST_UTIL.deleteTable(tableName0);
  }

  @TestTemplate
  public void testExportFileSystemStateWithSplitRegion() throws Exception {
    // disable compaction
    admin.compactionSwitch(false,
      admin.getRegionServers().stream().map(a -> a.getServerName()).collect(Collectors.toList()));
    // create Table
    String suffix = mob ? methodName + "-mob" : methodName;
    TableName splitTableName = TableName.valueOf(suffix);
    String splitTableSnap = "snapshot-" + suffix;
    admin.createTable(TableDescriptorBuilder.newBuilder(splitTableName).setColumnFamilies(
      Lists.newArrayList(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build())).build());

    Path output = TEST_UTIL.getDataTestDir("output/cf");
    TEST_UTIL.getTestFileSystem().mkdirs(output);
    // Create and load a large hfile to ensure the execution time of MR job.
    HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), TEST_UTIL.getTestFileSystem(),
      new Path(output, "test_file"), FAMILY, Bytes.toBytes("q"), Bytes.toBytes("1"),
      Bytes.toBytes("9"), 9999999);
    BulkLoadHFilesTool tool = new BulkLoadHFilesTool(TEST_UTIL.getConfiguration());
    tool.run(new String[] { output.getParent().toString(), splitTableName.getNameAsString() });

    List<RegionInfo> regions = admin.getRegions(splitTableName);
    assertEquals(1, regions.size());
    tableNumFiles = regions.size();

    // split region
    admin.split(splitTableName, Bytes.toBytes("5"));
    regions = admin.getRegions(splitTableName);
    assertEquals(2, regions.size());

    // take a snapshot
    admin.snapshot(splitTableSnap, splitTableName);
    // export snapshot and verify
    Configuration tmpConf = TEST_UTIL.getConfiguration();
    // Decrease the buffer size of copier to avoid the export task finished shortly
    tmpConf.setInt("snapshot.export.buffer.size", 1);
    // Decrease the maximum files of each mapper to ensure the three files(1 hfile + 2 reference
    // files)
    // copied in different mappers concurrently.
    tmpConf.setInt("snapshot.export.default.map.group", 1);
    testExportFileSystemState(tmpConf, splitTableName, splitTableSnap, splitTableSnap,
      tableNumFiles, TEST_UTIL.getDefaultRootDirPath(), getHdfsDestinationDir(), false, false,
      getBypassRegionPredicate(), true, false);
    // delete table
    TEST_UTIL.deleteTable(splitTableName);
  }
}
