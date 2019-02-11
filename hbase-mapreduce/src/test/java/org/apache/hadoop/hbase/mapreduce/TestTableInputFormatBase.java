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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({SmallTests.class})
public class TestTableInputFormatBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableInputFormatBase.class);

  @Test
  public void testTableInputFormatBaseReverseDNSForIPv6()
      throws UnknownHostException {
    String address = "ipv6.google.com";
    String localhost = null;
    InetAddress addr = null;
    TableInputFormat inputFormat = new TableInputFormat();
    try {
      localhost = InetAddress.getByName(address).getCanonicalHostName();
      addr = Inet6Address.getByName(address);
    } catch (UnknownHostException e) {
      // google.com is down, we can probably forgive this test.
      return;
    }
    System.out.println("Should retrun the hostname for this host " +
        localhost + " addr : " + addr);
    String actualHostName = inputFormat.reverseDNS(addr);
    assertEquals("Should retrun the hostname for this host. Expected : " +
        localhost + " Actual : " + actualHostName, localhost, actualHostName);
  }

  @Test
  public void testNonSuccessiveSplitsAreNotMerged() throws IOException {
    JobContext context = mock(JobContext.class);
    Configuration conf = HBaseConfiguration.create();
    conf.set(ConnectionUtils.HBASE_CLIENT_CONNECTION_IMPL,
        ConnectionForMergeTesting.class.getName());
    conf.set(TableInputFormat.INPUT_TABLE, "testTable");
    conf.setBoolean(TableInputFormatBase.MAPREDUCE_INPUT_AUTOBALANCE, true);
    when(context.getConfiguration()).thenReturn(conf);

    TableInputFormat tifExclude = new TableInputFormatForMergeTesting();
    tifExclude.setConf(conf);
    // split["b", "c"] is excluded, split["o", "p"] and split["p", "q"] are merged,
    // but split["a", "b"] and split["c", "d"] are not merged.
    assertEquals(ConnectionForMergeTesting.START_KEYS.length - 1 - 1,
        tifExclude.getSplits(context).size());
  }

  /**
   * Subclass of {@link TableInputFormat} to use in {@link #testNonSuccessiveSplitsAreNotMerged}.
   * This class overrides {@link TableInputFormatBase#includeRegionInSplit}
   * to exclude specific splits.
   */
  private static class TableInputFormatForMergeTesting extends TableInputFormat {
    private byte[] prefixStartKey = Bytes.toBytes("b");
    private byte[] prefixEndKey = Bytes.toBytes("c");
    private RegionSizeCalculator sizeCalculator;

    /**
     * Exclude regions which contain rows starting with "b".
     */
    @Override
    protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
      if (Bytes.compareTo(startKey, prefixEndKey) < 0
          && (Bytes.compareTo(prefixStartKey, endKey) < 0
              || Bytes.equals(endKey, HConstants.EMPTY_END_ROW))) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    protected void initializeTable(Connection connection, TableName tableName) throws IOException {
      super.initializeTable(connection, tableName);
      ConnectionForMergeTesting cft = (ConnectionForMergeTesting) connection;
      sizeCalculator = cft.getRegionSizeCalculator();
    }

    @Override
    protected RegionSizeCalculator createRegionSizeCalculator(RegionLocator locator, Admin admin)
      throws IOException {
      return sizeCalculator;
    }
  }

  /**
   * Connection class to use in {@link #testNonSuccessiveSplitsAreNotMerged}.
   * This class returns mocked {@link Table}, {@link RegionLocator}, {@link RegionSizeCalculator},
   * and {@link Admin}.
   */
  private static class ConnectionForMergeTesting implements Connection {
    public static final byte[][] SPLITS = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"), Bytes.toBytes("d"),
      Bytes.toBytes("e"), Bytes.toBytes("f"), Bytes.toBytes("g"), Bytes.toBytes("h"),
      Bytes.toBytes("i"), Bytes.toBytes("j"), Bytes.toBytes("k"), Bytes.toBytes("l"),
      Bytes.toBytes("m"), Bytes.toBytes("n"), Bytes.toBytes("o"), Bytes.toBytes("p"),
      Bytes.toBytes("q"), Bytes.toBytes("r"), Bytes.toBytes("s"), Bytes.toBytes("t"),
      Bytes.toBytes("u"), Bytes.toBytes("v"), Bytes.toBytes("w"), Bytes.toBytes("x"),
      Bytes.toBytes("y"), Bytes.toBytes("z")
    };

    public static final byte[][] START_KEYS;
    public static final byte[][] END_KEYS;
    static {
      START_KEYS = new byte[SPLITS.length + 1][];
      START_KEYS[0] = HConstants.EMPTY_BYTE_ARRAY;
      for (int i = 0; i < SPLITS.length; i++) {
        START_KEYS[i + 1] = SPLITS[i];
      }

      END_KEYS = new byte[SPLITS.length + 1][];
      for (int i = 0; i < SPLITS.length; i++) {
        END_KEYS[i] = SPLITS[i];
      }
      END_KEYS[SPLITS.length] = HConstants.EMPTY_BYTE_ARRAY;
    }

    public static final Map<byte[], Long> SIZE_MAP = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    static {
      for (byte[] startKey : START_KEYS) {
        SIZE_MAP.put(startKey, 1024L * 1024L * 1024L);
      }
      SIZE_MAP.put(Bytes.toBytes("a"), 200L * 1024L * 1024L);
      SIZE_MAP.put(Bytes.toBytes("b"), 200L * 1024L * 1024L);
      SIZE_MAP.put(Bytes.toBytes("c"), 200L * 1024L * 1024L);
      SIZE_MAP.put(Bytes.toBytes("o"), 200L * 1024L * 1024L);
      SIZE_MAP.put(Bytes.toBytes("p"), 200L * 1024L * 1024L);
    }

    ConnectionForMergeTesting(Configuration conf, ExecutorService pool, User user)
        throws IOException {
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
      throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
      Table table = mock(Table.class);
      when(table.getName()).thenReturn(tableName);
      return table;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
      final Map<byte[], HRegionLocation> locationMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (byte[] startKey : START_KEYS) {
        HRegionLocation hrl = new HRegionLocation(
            RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).build(),
            ServerName.valueOf("localhost", 0, 0));
        locationMap.put(startKey, hrl);
      }

      RegionLocator locator = mock(RegionLocator.class);
      when(locator.getRegionLocation(any(byte [].class), anyBoolean())).
        thenAnswer(new Answer<HRegionLocation>() {
          @Override
          public HRegionLocation answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object [] args = invocationOnMock.getArguments();
            byte [] key = (byte [])args[0];
            return locationMap.get(key);
          }
        });
      when(locator.getStartEndKeys()).
        thenReturn(new Pair<byte[][], byte[][]>(START_KEYS, END_KEYS));
      return locator;
    }

    public RegionSizeCalculator getRegionSizeCalculator() {
      RegionSizeCalculator sizeCalculator = mock(RegionSizeCalculator.class);
      when(sizeCalculator.getRegionSize(any(byte[].class))).
        thenAnswer(new Answer<Long>() {
          @Override
          public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object [] args = invocationOnMock.getArguments();
            byte [] regionId = (byte [])args[0];
            byte[] startKey = RegionInfo.getStartKey(regionId);
            return SIZE_MAP.get(startKey);
          }
        });
      return sizeCalculator;
    }

    @Override
    public Admin getAdmin() throws IOException {
      Admin admin = mock(Admin.class);
      // return non-null admin to pass null checks
      return admin;
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
      throw new UnsupportedOperationException();
    }

    @Override
    public void clearRegionLocationCache() {
    }
  }
}
