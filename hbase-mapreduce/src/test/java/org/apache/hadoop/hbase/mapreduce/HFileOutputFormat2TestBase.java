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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.TestHRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class HFileOutputFormat2TestBase {

  protected static final int ROWSPERSPLIT = 1024;
  protected static final int DEFAULT_VALUE_LENGTH = 1000;

  public static final byte[] FAMILY_NAME = TestHRegionFileSystem.FAMILY_NAME;
  protected static final byte[][] FAMILIES =
    { Bytes.add(FAMILY_NAME, Bytes.toBytes("-A")), Bytes.add(FAMILY_NAME, Bytes.toBytes("-B")) };
  protected static final TableName[] TABLE_NAMES = Stream
    .of("TestTable", "TestTable2", "TestTable3").map(TableName::valueOf).toArray(TableName[]::new);

  protected static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  /**
   * Simple mapper that makes KeyValue output.
   */
  protected static class RandomKVGeneratingMapper
    extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Cell> {

    private int keyLength;
    protected static final int KEYLEN_DEFAULT = 10;
    protected static final String KEYLEN_CONF = "randomkv.key.length";

    private int valLength;
    private static final int VALLEN_DEFAULT = 10;
    private static final String VALLEN_CONF = "randomkv.val.length";
    private static final byte[] QUALIFIER = Bytes.toBytes("data");
    private boolean multiTableMapper = false;
    private TableName[] tables = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
      multiTableMapper =
        conf.getBoolean(HFileOutputFormat2.MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY, false);
      if (multiTableMapper) {
        tables = TABLE_NAMES;
      } else {
        tables = new TableName[] { TABLE_NAMES[0] };
      }
    }

    @Override
    protected void map(NullWritable n1, NullWritable n2,
      Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Cell>.Context context)
      throws java.io.IOException, InterruptedException {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";
      byte[] key;
      for (int j = 0; j < tables.length; ++j) {
        for (int i = 0; i < ROWSPERSPLIT; i++) {
          Bytes.random(keyBytes);
          // Ensure that unique tasks generate unique keys
          keyBytes[keyLength - 1] = (byte) (taskId & 0xFF);
          Bytes.random(valBytes);
          key = keyBytes;
          if (multiTableMapper) {
            key = MultiTableHFileOutputFormat.createCompositeKey(tables[j].getName(), keyBytes);
          }

          for (byte[] family : FAMILIES) {
            Cell kv = new KeyValue(keyBytes, family, QUALIFIER, valBytes);
            context.write(new ImmutableBytesWritable(key), kv);
          }
        }
      }
    }
  }

  /**
   * Simple mapper that makes Put output.
   */
  protected static class RandomPutGeneratingMapper
    extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Put> {

    private int keyLength;
    protected static final int KEYLEN_DEFAULT = 10;
    protected static final String KEYLEN_CONF = "randomkv.key.length";

    private int valLength;
    protected static final int VALLEN_DEFAULT = 10;
    protected static final String VALLEN_CONF = "randomkv.val.length";
    protected static final byte[] QUALIFIER = Bytes.toBytes("data");
    private boolean multiTableMapper = false;
    private TableName[] tables = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
      multiTableMapper =
        conf.getBoolean(HFileOutputFormat2.MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY, false);
      if (multiTableMapper) {
        tables = TABLE_NAMES;
      } else {
        tables = new TableName[] { TABLE_NAMES[0] };
      }
    }

    @Override
    protected void map(NullWritable n1, NullWritable n2,
      Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

      byte[] key;
      for (int j = 0; j < tables.length; ++j) {
        for (int i = 0; i < ROWSPERSPLIT; i++) {
          Bytes.random(keyBytes);
          // Ensure that unique tasks generate unique keys
          keyBytes[keyLength - 1] = (byte) (taskId & 0xFF);
          Bytes.random(valBytes);
          key = keyBytes;
          if (multiTableMapper) {
            key = MultiTableHFileOutputFormat.createCompositeKey(tables[j].getName(), keyBytes);
          }

          for (byte[] family : FAMILIES) {
            Put p = new Put(keyBytes);
            p.addColumn(family, QUALIFIER, valBytes);
            // set TTL to very low so that the scan does not return any value
            p.setTTL(1l);
            context.write(new ImmutableBytesWritable(key), p);
          }
        }
      }
    }
  }

  protected static void setupRandomGeneratorMapper(Job job, boolean putSortReducer) {
    if (putSortReducer) {
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(RandomPutGeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
    } else {
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(RandomKVGeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
    }
  }

  protected static byte[][] generateRandomStartKeys(int numKeys) {
    Random random = ThreadLocalRandom.current();
    byte[][] ret = new byte[numKeys][];
    // first region start key is always empty
    ret[0] = HConstants.EMPTY_BYTE_ARRAY;
    for (int i = 1; i < numKeys; i++) {
      ret[i] = generateData(random, DEFAULT_VALUE_LENGTH);
    }
    return ret;
  }

  /**
   * This method takes some time and is done inline uploading data. For example, doing the mapfile
   * test, generation of the key and value consumes about 30% of CPU time.
   * @return Generated random value to insert into a table cell.
   */
  protected static byte[] generateData(final Random r, int length) {
    byte[] b = new byte[length];
    int i;

    for (i = 0; i < (length - 8); i += 8) {
      b[i] = (byte) (65 + r.nextInt(26));
      b[i + 1] = b[i];
      b[i + 2] = b[i];
      b[i + 3] = b[i];
      b[i + 4] = b[i];
      b[i + 5] = b[i];
      b[i + 6] = b[i];
      b[i + 7] = b[i];
    }

    byte a = (byte) (65 + r.nextInt(26));
    for (; i < length; i++) {
      b[i] = a;
    }
    return b;
  }

  protected static byte[][] generateRandomSplitKeys(int numKeys) {
    Random random = ThreadLocalRandom.current();
    byte[][] ret = new byte[numKeys][];
    for (int i = 0; i < numKeys; i++) {
      ret[i] = generateData(random, DEFAULT_VALUE_LENGTH);
    }
    return ret;
  }

  protected static void runIncrementalPELoad(Configuration conf,
    List<HFileOutputFormat2.TableInfo> tableInfo, Path outDir, boolean putSortReducer)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(conf, "testLocalMRIncrementalLoad");
    job.setWorkingDirectory(UTIL.getDataTestDirOnTestFS("runIncrementalPELoad"));
    job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
      MutationSerialization.class.getName(), ResultSerialization.class.getName(),
      CellSerialization.class.getName());
    setupRandomGeneratorMapper(job, putSortReducer);
    if (tableInfo.size() > 1) {
      MultiTableHFileOutputFormat.configureIncrementalLoad(job, tableInfo);
      int sum = 0;
      for (HFileOutputFormat2.TableInfo tableInfoSingle : tableInfo) {
        sum += tableInfoSingle.getRegionLocator().getAllRegionLocations().size();
      }
      assertEquals(sum, job.getNumReduceTasks());
    } else {
      RegionLocator regionLocator = tableInfo.get(0).getRegionLocator();
      HFileOutputFormat2.configureIncrementalLoad(job, tableInfo.get(0).getTableDescriptor(),
        regionLocator);
      assertEquals(regionLocator.getAllRegionLocations().size(), job.getNumReduceTasks());
    }

    FileOutputFormat.setOutputPath(job, outDir);

    assertFalse(UTIL.getTestFileSystem().exists(outDir));

    assertTrue(job.waitForCompletion(true));
  }
}
