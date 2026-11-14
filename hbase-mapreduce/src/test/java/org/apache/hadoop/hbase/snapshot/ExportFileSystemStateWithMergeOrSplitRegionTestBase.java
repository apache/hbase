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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.TestTemplate;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

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
    await().atMost(Duration.ofSeconds(30))
      .untilAsserted(() -> assertThat(admin.getRegions(tableName0), hasSize(1)));
    // take a snapshot
    admin.snapshot(snapshotName0, tableName0);
    // export snapshot and verify
    testExportFileSystemState(tableName0, snapshotName0, snapshotName0, tableNumFiles);
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
    // One HFile spanning the split key is enough to create references in both daughters.
    HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), TEST_UTIL.getTestFileSystem(),
      new Path(output, "test_file"), FAMILY, Bytes.toBytes("q"), Bytes.toBytes("1"),
      Bytes.toBytes("9"), 1000);
    BulkLoadHFilesTool tool = new BulkLoadHFilesTool(TEST_UTIL.getConfiguration());
    tool.run(new String[] { output.getParent().toString(), splitTableName.getNameAsString() });

    List<RegionInfo> regions = admin.getRegions(splitTableName);
    assertEquals(1, regions.size());
    tableNumFiles = regions.size();

    // split region
    admin.split(splitTableName, Bytes.toBytes("5"));
    await().atMost(Duration.ofSeconds(30))
      .untilAsserted(() -> assertThat(admin.getRegions(splitTableName), hasSize(2)));

    // take a snapshot
    admin.snapshot(splitTableSnap, splitTableName);
    Configuration tmpConf = new Configuration(TEST_UTIL.getConfiguration());
    tmpConf.setInt("snapshot.export.default.map.group", 1);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(splitTableSnap,
      TEST_UTIL.getDefaultRootDirPath());
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    SnapshotManifest manifest = SnapshotManifest.open(tmpConf, fs, snapshotDir,
      SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir));
    assertEquals(2,
      manifest.getRegionManifests().stream().flatMap(region -> region.getFamilyFilesList().stream())
        .flatMap(family -> family.getStoreFilesList().stream())
        .filter(SnapshotRegionManifest.StoreFile::hasReference).count());

    // Check mapper inputs directly instead of relying on concurrent copies of a large file.
    Configuration inputConf = new Configuration(tmpConf);
    inputConf.set("snapshot.export.format.snapshot.dir", snapshotDir.toString());
    ExportSnapshot.ExportSnapshotInputFormat inputFormat =
      new ExportSnapshot.ExportSnapshotInputFormat();
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(inputConf, new TaskAttemptID());
    int inputFiles = 0;
    for (InputSplit split : inputFormat.getSplits(Job.getInstance(inputConf))) {
      try (RecordReader<BytesWritable, NullWritable> reader =
        inputFormat.createRecordReader(split, context)) {
        reader.initialize(split, context);
        while (reader.nextKeyValue()) {
          inputFiles++;
        }
      }
    }
    assertEquals(tableNumFiles, inputFiles, "Each referenced HFile should be copied only once");

    // export snapshot and verify
    testExportFileSystemState(tmpConf, splitTableName, splitTableSnap, splitTableSnap,
      tableNumFiles, TEST_UTIL.getDefaultRootDirPath(), getHdfsDestinationDir(), false, false,
      getBypassRegionPredicate(), true, false);
  }
}
