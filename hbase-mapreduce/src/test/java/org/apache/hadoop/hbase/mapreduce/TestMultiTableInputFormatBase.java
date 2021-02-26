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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.client.Scan.SCAN_ATTRIBUTES_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests of MultiTableInputFormatBase.
 */
@Category({SmallTests.class})
public class TestMultiTableInputFormatBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiTableInputFormatBase.class);

  @Rule public final TestName name = new TestName();

  /**
   * Test getSplits only puts up one Connection.
   * In past it has put up many Connections. Each Connection setup comes with a fresh new cache
   * so we have to do fresh hit on hbase:meta. Should only do one Connection when doing getSplits
   * even if a MultiTableInputFormat.
   * @throws IOException
   */
  @Test
  public void testMRSplitsConnectionCount() throws IOException {
    // Make instance of MTIFB.
    MultiTableInputFormatBase mtif = new MultiTableInputFormatBase() {
      @Override
      public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split,
          TaskAttemptContext context)
      throws IOException, InterruptedException {
        return super.createRecordReader(split, context);
      }
    };
    // Pass it a mocked JobContext. Make the JC return our Configuration.
    // Load the Configuration so it returns our special Connection so we can interpolate
    // canned responses.
    JobContext mockedJobContext = Mockito.mock(JobContext.class);
    Configuration c = HBaseConfiguration.create();
    c.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, MRSplitsConnection.class.getName());
    Mockito.when(mockedJobContext.getConfiguration()).thenReturn(c);
    // Invent a bunch of scans. Have each Scan go against a different table so a good spread.
    List<Scan> scans = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Scan scan = new Scan();
      String tableName = this.name.getMethodName() + i;
      scan.setAttribute(SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
      scans.add(scan);
    }
    mtif.setScans(scans);
    // Get splits. Assert that that more than one.
    List<InputSplit> splits = mtif.getSplits(mockedJobContext);
    Assert.assertTrue(splits.size() > 0);
    // Assert only one Connection was made (see the static counter we have in the mocked
    // Connection MRSplitsConnection Constructor.
    Assert.assertEquals(1, MRSplitsConnection.creations.get());
  }

  /**
   * Connection to use above in Test.
   */
  public static class MRSplitsConnection implements Connection {
    private final Configuration configuration;
    static final AtomicInteger creations = new AtomicInteger(0);

    MRSplitsConnection (Configuration conf, ExecutorService pool, User user) throws IOException {
      this.configuration = conf;
      creations.incrementAndGet();
    }

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public Configuration getConfiguration() {
      return this.configuration;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
      return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
      return null;
    }

    @Override
    public RegionLocator getRegionLocator(final TableName tableName) throws IOException {
      // Make up array of start keys. We start off w/ empty byte array.
      final byte [][] startKeys = new byte [][] {HConstants.EMPTY_BYTE_ARRAY,
          Bytes.toBytes("aaaa"), Bytes.toBytes("bbb"),
          Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
          Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
          Bytes.toBytes("iii"), Bytes.toBytes("lll"), Bytes.toBytes("mmm"),
          Bytes.toBytes("nnn"), Bytes.toBytes("ooo"), Bytes.toBytes("ppp"),
          Bytes.toBytes("qqq"), Bytes.toBytes("rrr"), Bytes.toBytes("sss"),
          Bytes.toBytes("ttt"), Bytes.toBytes("uuu"), Bytes.toBytes("vvv"),
          Bytes.toBytes("zzz")};
      // Make an array of end keys. We end with the empty byte array.
      final byte [][] endKeys = new byte[][] {
          Bytes.toBytes("aaaa"), Bytes.toBytes("bbb"),
          Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
          Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
          Bytes.toBytes("iii"), Bytes.toBytes("lll"), Bytes.toBytes("mmm"),
          Bytes.toBytes("nnn"), Bytes.toBytes("ooo"), Bytes.toBytes("ppp"),
          Bytes.toBytes("qqq"), Bytes.toBytes("rrr"), Bytes.toBytes("sss"),
          Bytes.toBytes("ttt"), Bytes.toBytes("uuu"), Bytes.toBytes("vvv"),
          Bytes.toBytes("zzz"),
          HConstants.EMPTY_BYTE_ARRAY};
      // Now make a map of start keys to HRegionLocations. Let the server namber derive from
      // the start key.
      final Map<byte [], HRegionLocation> map =
          new TreeMap<byte [], HRegionLocation>(Bytes.BYTES_COMPARATOR);
      for (byte [] startKey: startKeys) {
        HRegionLocation hrl = new HRegionLocation(
            RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).build(),
            ServerName.valueOf(Bytes.toString(startKey), 0, 0));
        map.put(startKey, hrl);
      }
      // Get a list of the locations.
      final List<HRegionLocation> locations = new ArrayList<HRegionLocation>(map.values());
      // Now make a RegionLocator mock backed by the abpve map and list of locations.
      RegionLocator mockedRegionLocator = Mockito.mock(RegionLocator.class);
      Mockito.when(mockedRegionLocator.getRegionLocation(Mockito.any(byte [].class),
            Mockito.anyBoolean())).
          thenAnswer(new Answer<HRegionLocation>() {
            @Override
            public HRegionLocation answer(InvocationOnMock invocationOnMock) throws Throwable {
              Object [] args = invocationOnMock.getArguments();
              byte [] key = (byte [])args[0];
              return map.get(key);
            }
          });
      Mockito.when(mockedRegionLocator.getAllRegionLocations()).thenReturn(locations);
      Mockito.when(mockedRegionLocator.getStartEndKeys()).
          thenReturn(new Pair<byte [][], byte[][]>(startKeys, endKeys));
      Mockito.when(mockedRegionLocator.getName()).thenReturn(tableName);
      return mockedRegionLocator;
    }

    @Override
    public Admin getAdmin() throws IOException {
      Admin admin = Mockito.mock(Admin.class);
      Mockito.when(admin.getConfiguration()).thenReturn(getConfiguration());
      return admin;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
      Table table = Mockito.mock(Table.class);
      Mockito.when(table.getName()).thenReturn(tableName);
      return table;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
      return Mockito.mock(TableBuilder.class);
    }

    @Override
    public void clearRegionLocationCache() {
    }
  }
}
