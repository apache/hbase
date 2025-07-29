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

import java.io.IOException;
import javax.validation.constraints.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Simple Tests to check whether the durability of the Mutation is changed or not, for
 * {@link TableOutputFormat} if {@link TableOutputFormat#WAL_PROPERTY} is set to false.
 */
@Category(MediumTests.class)
public class TestTableOutputFormat {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableOutputFormat.class);

  private static final HBaseTestingUtil util = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("TEST_TABLE");
  private static final byte[] columnFamily = Bytes.toBytes("f");
  private static Configuration conf;
  private static RecordWriter<Null, Mutation> writer;
  private static TaskAttemptContext context;
  private static TableOutputFormat<Null> tableOutputFormat;
  private static final TableOutputCommitter CUSTOM_COMMITTER = new TableOutputCommitter() {};

  @BeforeClass
  public static void setUp() throws Exception {
    util.startMiniCluster();
    util.createTable(TABLE_NAME, columnFamily);

    conf = new Configuration(util.getConfiguration());
    context = Mockito.mock(TaskAttemptContext.class);
    tableOutputFormat = new TableOutputFormat<>();
    conf.set(TableOutputFormat.OUTPUT_TABLE, "TEST_TABLE");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @After
  public void close() throws IOException, InterruptedException {
    if (writer != null && context != null) {
      writer.close(context);
    }
  }

  @Test
  public void testTableOutputFormatWhenWalIsOFFForPut() throws IOException, InterruptedException {
    // setting up the configuration for the TableOutputFormat, with writing to the WAL off.
    conf.setBoolean(TableOutputFormat.WAL_PROPERTY, TableOutputFormat.WAL_OFF);
    tableOutputFormat.setConf(conf);

    writer = tableOutputFormat.getRecordWriter(context);

    // creating mutation of the type put
    Put put = new Put("row1".getBytes());
    put.addColumn(columnFamily, Bytes.toBytes("aa"), Bytes.toBytes("value"));

    // verifying whether durability of mutation is USE_DEFAULT or not, before commiting write.
    Assert.assertEquals("Durability of the mutation should be USE_DEFAULT", Durability.USE_DEFAULT,
      put.getDurability());

    writer.write(null, put);

    // verifying whether durability of mutation got changed to the SKIP_WAL or not.
    Assert.assertEquals("Durability of the mutation should be SKIP_WAL", Durability.SKIP_WAL,
      put.getDurability());
  }

  @Test
  public void testTableOutputFormatWhenWalIsOFFForDelete()
    throws IOException, InterruptedException {
    // setting up the configuration for the TableOutputFormat, with writing to the WAL off.
    conf.setBoolean(TableOutputFormat.WAL_PROPERTY, TableOutputFormat.WAL_OFF);
    tableOutputFormat.setConf(conf);

    writer = tableOutputFormat.getRecordWriter(context);

    // creating mutation of the type delete
    Delete delete = new Delete("row2".getBytes());
    delete.addColumn(columnFamily, Bytes.toBytes("aa"));

    // verifying whether durability of mutation is USE_DEFAULT or not, before commiting write.
    Assert.assertEquals("Durability of the mutation should be USE_DEFAULT", Durability.USE_DEFAULT,
      delete.getDurability());

    writer.write(null, delete);

    // verifying whether durability of mutation got changed from USE_DEFAULT to the SKIP_WAL or not.
    Assert.assertEquals("Durability of the mutation should be SKIP_WAL", Durability.SKIP_WAL,
      delete.getDurability());
  }

  @Test
  public void testOutputCommitterConfiguration() throws IOException, InterruptedException {
    // 1. Verify it returns the default committer when the property is not set.
    conf.unset(TableOutputFormat.OUTPUT_COMMITTER_CLASS);
    tableOutputFormat.setConf(conf);
    Assert.assertEquals("Should use default committer",
      TableOutputCommitter.class,
      tableOutputFormat.getOutputCommitter(context).getClass());

    // 2. Verify it returns the custom committer when the property is set.
    conf.set(TableOutputFormat.OUTPUT_COMMITTER_CLASS, CUSTOM_COMMITTER.getClass().getName());
    tableOutputFormat.setConf(conf);
    Assert.assertEquals("Should use custom committer",
      CUSTOM_COMMITTER.getClass(),
      tableOutputFormat.getOutputCommitter(context).getClass());
  }
}
