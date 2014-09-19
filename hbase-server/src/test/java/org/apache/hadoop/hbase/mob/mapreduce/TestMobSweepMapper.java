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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mob.MobZookeeper;
import org.apache.hadoop.hbase.mob.mapreduce.SweepMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestMobSweepMapper {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void TestMap() throws Exception {
    String prefix = "0000";
    final String fileName = "19691231f2cd014ea28f42788214560a21a44cef";
    final String mobFilePath = prefix + fileName;

    ImmutableBytesWritable r = new ImmutableBytesWritable(Bytes.toBytes("r"));
    final KeyValue[] kvList = new KeyValue[1];
    kvList[0] = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
            Bytes.toBytes("column"), Bytes.toBytes(mobFilePath));

    Result columns = mock(Result.class);
    when(columns.raw()).thenReturn(kvList);

    Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(SweepJob.SWEEP_JOB_ID, "1");
    configuration.set(SweepJob.SWEEPER_NODE, "/hbase/MOB/testSweepMapper:family-sweeper");

    MobZookeeper zk = MobZookeeper.newInstance(configuration, "1");
    zk.addSweeperZNode("testSweepMapper", "family", Bytes.toBytes("1"));

    Mapper<ImmutableBytesWritable, Result, Text, KeyValue>.Context ctx =
            mock(Mapper.Context.class);
    when(ctx.getConfiguration()).thenReturn(configuration);
    SweepMapper map = new SweepMapper();
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Text text = (Text) invocation.getArguments()[0];
        KeyValue kv = (KeyValue) invocation.getArguments()[1];

        assertEquals(Bytes.toString(text.getBytes(), 0, text.getLength()), fileName);
        assertEquals(0, Bytes.compareTo(kv.getKey(), kvList[0].getKey()));

        return null;
      }
    }).when(ctx).write(any(Text.class), any(KeyValue.class));

    map.map(r, columns, ctx);
  }
}
