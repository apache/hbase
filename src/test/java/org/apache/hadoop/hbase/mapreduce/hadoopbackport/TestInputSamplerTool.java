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
package org.apache.hadoop.hbase.mapreduce.hadoopbackport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Tests {@link InputSampler} as a {@link Tool}.
 */
@Category(SmallTests.class)
public class TestInputSamplerTool {

  private static final int NUM_REDUCES = 4;

  private static final String input1Str =
     "2\n"
    +"...5\n"
    +"......8\n";
  private static final String input2Str =
     "2\n"
    +".3\n"
    +"..4\n"
    +"...5\n"
    +"....6\n"
    +".....7\n"
    +"......8\n"
    +".......9\n";

  private static File tempDir;
  private static String input1, input2, output;

  @BeforeClass
  public static void beforeClass() throws IOException {
    tempDir = FileUtil.createLocalTempFile(
      new File(FileUtils.getTempDirectory(), TestInputSamplerTool.class.getName() + "-tmp-"),
      "", false);
    tempDir.delete();
    tempDir.mkdirs();
    assertTrue(tempDir.exists());
    assertTrue(tempDir.isDirectory());
    // define files:
    input1 = tempDir.getAbsolutePath() + "/input1";
    input2 = tempDir.getAbsolutePath() + "/input2";
    output = tempDir.getAbsolutePath() + "/output";
    // create 2 input files:
    IOUtils.copy(new StringReader(input1Str), new FileOutputStream(input1));
    IOUtils.copy(new StringReader(input2Str), new FileOutputStream(input2));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    final File td = tempDir;
    if (td != null && td.exists()) {
      FileUtil.fullyDelete(tempDir);
    }
  }

  @Test
  public void testIncorrectParameters() throws Exception {
    Tool tool = new InputSampler<Object,Object>(new Configuration());

    int result = tool.run(new String[] { "-r" });
    assertTrue(result != 0);

    result = tool.run(new String[] { "-r", "not-a-number" });
    assertTrue(result != 0);

    // more than one reducer is required:
    result = tool.run(new String[] { "-r", "1" });
    assertTrue(result != 0);

    try {
      result = tool.run(new String[] { "-inFormat", "java.lang.Object" });
      fail("ClassCastException expected");
    } catch (ClassCastException cce) {
      // expected
    }

    try {
      result = tool.run(new String[] { "-keyClass", "java.lang.Object" });
      fail("ClassCastException expected");
    } catch (ClassCastException cce) {
      // expected
    }

    result = tool.run(new String[] { "-splitSample", "1", });
    assertTrue(result != 0);

    result = tool.run(new String[] { "-splitRandom", "1.0", "2", "xxx" });
    assertTrue(result != 0);

    result = tool.run(new String[] { "-splitInterval", "yyy", "5" });
    assertTrue(result != 0);

    // not enough subsequent arguments:
    result = tool.run(new String[] { "-r", "2", "-splitInterval", "11.0f", "0", "input" });
    assertTrue(result != 0);
  }

  @Test
  public void testSplitSample() throws Exception {
    Tool tool = new InputSampler<Object,Object>(new Configuration());
    int result = tool.run(new String[] { "-r", Integer.toString(NUM_REDUCES),
        "-splitSample", "10", "100",
        input1, input2, output });
    assertEquals(0, result);

    Object[] partitions = readPartitions(output);
    assertArrayEquals(
        new LongWritable[] { new LongWritable(2L), new LongWritable(7L), new LongWritable(20L),},
        partitions);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSplitRamdom() throws Exception {
    Tool tool = new InputSampler<Object,Object>(new Configuration());
    int result = tool.run(new String[] { "-r", Integer.toString(NUM_REDUCES),
        // Use 0.999 probability to reduce the flakiness of the test because
        // the test will fail if the number of samples is less than (number of reduces + 1).
        "-splitRandom", "0.999f", "20", "100",
        input1, input2, output });
    assertEquals(0, result);
    Object[] partitions = readPartitions(output);
    // must be 3 split points since NUM_REDUCES = 4:
    assertEquals(3, partitions.length);
    // check that the partition array is sorted:
    Object[] sortedPartitions = Arrays.copyOf(partitions, partitions.length);
    Arrays.sort(sortedPartitions, new LongWritable.Comparator());
    assertArrayEquals(sortedPartitions, partitions);
  }

  @Test
  public void testSplitInterval() throws Exception {
    Tool tool = new InputSampler<Object,Object>(new Configuration());
    int result = tool.run(new String[] { "-r", Integer.toString(NUM_REDUCES),
        "-splitInterval", "0.5f", "0",
        input1, input2, output });
    assertEquals(0, result);
    Object[] partitions = readPartitions(output);
    assertArrayEquals(new LongWritable[] { new LongWritable(7L), new LongWritable(9L),
      new LongWritable(35L),}, partitions);
  }

  private Object[] readPartitions(String filePath) throws Exception {
    Configuration conf = new Configuration();
    TotalOrderPartitioner.setPartitionFile(conf, new Path(filePath));
    Object[] partitions = readPartitions(FileSystem.getLocal(conf), new Path(filePath),
      LongWritable.class, conf);
    return partitions;
  }

  private Object[] readPartitions(FileSystem fs, Path p, Class<?> keyClass,
      Configuration conf) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    ArrayList<Object> parts = new ArrayList<Object>();
    Writable key = (Writable)ReflectionUtils.newInstance(keyClass, conf);
    NullWritable value = NullWritable.get();
    while (reader.next(key, value)) {
      parts.add(key);
      key = (Writable)ReflectionUtils.newInstance(keyClass, conf);
    }
    reader.close();
    return parts.toArray((Object[])Array.newInstance(keyClass, parts.size()));
  }
}
