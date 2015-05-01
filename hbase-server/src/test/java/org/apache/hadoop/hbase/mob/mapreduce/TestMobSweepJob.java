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
package org.apache.hadoop.hbase.mob.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobSweepJob {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
        JavaSerialization.class.getName() + "," + WritableSerialization.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void writeFileNames(FileSystem fs, Configuration conf, Path path,
      String[] filesNames) throws IOException {
    // write the names to a sequence file
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
        String.class, String.class);
    try {
      for (String fileName : filesNames) {
        writer.append(fileName, MobConstants.EMPTY_STRING);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  @Test
  public void testSweeperJobWithOutUnusedFile() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration configuration = new Configuration(
        TEST_UTIL.getConfiguration());
    Path vistiedFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/0/visited");
    Path allFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/0/all");
    configuration.set(SweepJob.WORKING_VISITED_DIR_KEY,
        vistiedFileNamesPath.toString());
    configuration.set(SweepJob.WORKING_ALLNAMES_FILE_KEY,
        allFileNamesPath.toString());

    writeFileNames(fs, configuration, allFileNamesPath, new String[] { "1",
        "2", "3", "4", "5", "6"});

    Path r0 = new Path(vistiedFileNamesPath, "r0");
    writeFileNames(fs, configuration, r0, new String[] { "1",
        "2", "3"});
    Path r1 = new Path(vistiedFileNamesPath, "r1");
    writeFileNames(fs, configuration, r1, new String[] { "1", "4", "5"});
    Path r2 = new Path(vistiedFileNamesPath, "r2");
    writeFileNames(fs, configuration, r2, new String[] { "2", "3", "6"});

    SweepJob sweepJob = new SweepJob(configuration, fs);
    List<String> toBeArchived = sweepJob.getUnusedFiles(configuration);

    assertEquals(0, toBeArchived.size());
  }

  @Test
  public void testSweeperJobWithUnusedFile() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration configuration = new Configuration(
        TEST_UTIL.getConfiguration());
    Path vistiedFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/1/visited");
    Path allFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/1/all");
    configuration.set(SweepJob.WORKING_VISITED_DIR_KEY,
        vistiedFileNamesPath.toString());
    configuration.set(SweepJob.WORKING_ALLNAMES_FILE_KEY,
        allFileNamesPath.toString());

    writeFileNames(fs, configuration, allFileNamesPath, new String[] { "1",
        "2", "3", "4", "5", "6"});

    Path r0 = new Path(vistiedFileNamesPath, "r0");
    writeFileNames(fs, configuration, r0, new String[] { "1",
        "2", "3"});
    Path r1 = new Path(vistiedFileNamesPath, "r1");
    writeFileNames(fs, configuration, r1, new String[] { "1", "5"});
    Path r2 = new Path(vistiedFileNamesPath, "r2");
    writeFileNames(fs, configuration, r2, new String[] { "2", "3"});

    SweepJob sweepJob = new SweepJob(configuration, fs);
    List<String> toBeArchived = sweepJob.getUnusedFiles(configuration);

    assertEquals(2, toBeArchived.size());
    assertArrayEquals(new String[]{"4", "6"}, toBeArchived.toArray(new String[0]));
  }

  @Test
  public void testSweeperJobWithRedundantFile() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration configuration = new Configuration(
        TEST_UTIL.getConfiguration());
    Path vistiedFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/2/visited");
    Path allFileNamesPath = new Path(MobUtils.getMobHome(configuration),
        "/hbase/mobcompaction/SweepJob/working/names/2/all");
    configuration.set(SweepJob.WORKING_VISITED_DIR_KEY,
        vistiedFileNamesPath.toString());
    configuration.set(SweepJob.WORKING_ALLNAMES_FILE_KEY,
        allFileNamesPath.toString());

    writeFileNames(fs, configuration, allFileNamesPath, new String[] { "1",
        "2", "3", "4", "5", "6"});

    Path r0 = new Path(vistiedFileNamesPath, "r0");
    writeFileNames(fs, configuration, r0, new String[] { "1",
        "2", "3"});
    Path r1 = new Path(vistiedFileNamesPath, "r1");
    writeFileNames(fs, configuration, r1, new String[] { "1", "5", "6", "7"});
    Path r2 = new Path(vistiedFileNamesPath, "r2");
    writeFileNames(fs, configuration, r2, new String[] { "2", "3", "4"});

    SweepJob sweepJob = new SweepJob(configuration, fs);
    List<String> toBeArchived = sweepJob.getUnusedFiles(configuration);

    assertEquals(0, toBeArchived.size());
  }
}
