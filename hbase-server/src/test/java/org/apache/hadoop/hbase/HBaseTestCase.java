/**
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionAsTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract HBase test class.  Initializes a few things that can come in handly
 * like an HBaseConfiguration and filesystem.
 * @deprecated since 2.0.0 and will be removed in 3.0.0. Write junit4 unit tests using
 *   {@link HBaseTestingUtility}.
 * @see HBaseTestingUtility
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-11912">HBASE-11912</a>
 */
@Deprecated
public abstract class HBaseTestCase extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTestCase.class);

  protected final static byte [] fam1 = Bytes.toBytes("colfamily11");
  protected final static byte [] fam2 = Bytes.toBytes("colfamily21");
  protected final static byte [] fam3 = Bytes.toBytes("colfamily31");

  protected static final byte [][] COLUMNS = {fam1, fam2, fam3};

  private boolean localfs = false;
  protected static Path testDir = null;
  protected FileSystem fs = null;
  protected HRegion meta = null;
  protected static final char FIRST_CHAR = 'a';
  protected static final char LAST_CHAR = 'z';
  protected static final String PUNCTUATION = "~`@#$%^&*()-_+=:;',.<>/?[]{}|";
  protected static final byte [] START_KEY_BYTES = {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
  protected String START_KEY = new String(START_KEY_BYTES, HConstants.UTF8_CHARSET);
  protected static final int MAXVERSIONS = 3;

  protected final HBaseTestingUtility testUtil = new HBaseTestingUtility();

  public volatile Configuration conf = testUtil.getConfiguration();
  public final FSTableDescriptors fsTableDescriptors;
  {
    try {
      fsTableDescriptors = new FSTableDescriptors(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to init descriptors", e);
    }
  }

  /** constructor */
  public HBaseTestCase() {
    super();
  }

  /**
   * @param name
   */
  public HBaseTestCase(String name) {
    super(name);
  }

  /**
   * Note that this method must be called after the mini hdfs cluster has
   * started or we end up with a local file system.
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    localfs =
      (conf.get("fs.defaultFS", "file:///").compareTo("file:///") == 0);

    if (fs == null) {
      this.fs = FileSystem.get(conf);
    }
    try {
      if (localfs) {
        testDir = getUnitTestdir(getName());
        if (fs.exists(testDir)) {
          fs.delete(testDir, true);
        }
      } else {
        testDir = CommonFSUtils.getRootDir(conf);
      }
    } catch (Exception e) {
      LOG.error(HBaseMarkers.FATAL, "error during setup", e);
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      if (localfs) {
        if (this.fs.exists(testDir)) {
          this.fs.delete(testDir, true);
        }
      }
    } catch (Exception e) {
      LOG.error(HBaseMarkers.FATAL, "error during tear down", e);
    }
    super.tearDown();
  }

  /**
   * @see HBaseTestingUtility#getBaseTestDir
   * @param testName
   * @return directory to use for this test
   */
    protected Path getUnitTestdir(String testName) {
      return testUtil.getDataTestDir(testName);
    }

  /**
   * You must call close on the returned region and then close on the log file it created. Do
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} to close both the region and the WAL.
   * @param desc
   * @param startKey
   * @param endKey
   * @return An {@link HRegion}
   * @throws IOException
   */
  public HRegion createNewHRegion(HTableDescriptor desc, byte [] startKey,
      byte [] endKey)
  throws IOException {
    return createNewHRegion(desc, startKey, endKey, this.conf);
  }

  public HRegion createNewHRegion(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, Configuration conf)
  throws IOException {
    HRegionInfo hri = new HRegionInfo(desc.getTableName(), startKey, endKey);
    return HBaseTestingUtility.createRegionAndWAL(hri, testDir, conf, desc);
  }

  protected HRegion openClosedRegion(final HRegion closedRegion)
  throws IOException {
    return HRegion.openHRegion(closedRegion, null);
  }

  /**
   * Create a table of name {@code name} with {@link #COLUMNS} for
   * families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, MAXVERSIONS);
  }

  /**
   * Create a table of name {@code name} with {@link #COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name,
      final int versions) {
    return createTableDescriptor(name, HColumnDescriptor.DEFAULT_MIN_VERSIONS,
        versions, HConstants.FOREVER, HColumnDescriptor.DEFAULT_KEEP_DELETED);
  }

  /**
   * Create a table of name {@code name} with {@link #COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name,
      final int minVersions, final int versions, final int ttl, KeepDeletedCells keepDeleted) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    for (byte[] cfName : new byte[][]{ fam1, fam2, fam3 }) {
      htd.addFamily(new HColumnDescriptor(cfName)
          .setMinVersions(minVersions)
          .setMaxVersions(versions)
          .setKeepDeletedCells(keepDeleted)
          .setBlockCacheEnabled(false)
          .setTimeToLive(ttl)
      );
    }
    return htd;
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param r
   * @param columnFamily
   * @param column
   * @throws IOException
   * @return count of what we added.
   */
  public static long addContent(final Region r, final byte [] columnFamily, final byte[] column)
  throws IOException {
    byte [] startKey = r.getRegionInfo().getStartKey();
    byte [] endKey = r.getRegionInfo().getEndKey();
    byte [] startKeyBytes = startKey;
    if (startKeyBytes == null || startKeyBytes.length == 0) {
      startKeyBytes = START_KEY_BYTES;
    }
    return addContent(new RegionAsTable(r), Bytes.toString(columnFamily), Bytes.toString(column),
      startKeyBytes, endKey, -1);
  }

  public static long addContent(final Region r, final byte [] columnFamily) throws IOException {
    return addContent(r, columnFamily, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @throws IOException
   * @return count of what we added.
   */
  public static long addContent(final Table updater,
      final String columnFamily) throws IOException {
    return addContent(updater, columnFamily, START_KEY_BYTES, null);
  }

  public static long addContent(final Table updater, final String family,
      final String column) throws IOException {
    return addContent(updater, family, column, START_KEY_BYTES, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   * @throws IOException
   */
  public static long addContent(final Table updater, final String columnFamily,
      final byte [] startKeyBytes, final byte [] endKey)
  throws IOException {
    return addContent(updater, columnFamily, null, startKeyBytes, endKey, -1);
  }

  public static long addContent(final Table updater, final String family, String column,
      final byte [] startKeyBytes, final byte [] endKey) throws IOException {
    return addContent(updater, family, column, startKeyBytes, endKey, -1);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   * @throws IOException
   */
  public static long addContent(final Table updater,
                                   final String columnFamily,
                                   final String column,
      final byte [] startKeyBytes, final byte [] endKey, final long ts)
  throws IOException {
    long count = 0;
    // Add rows of three characters.  The first character starts with the
    // 'a' character and runs up to 'z'.  Per first character, we run the
    // second character over same range.  And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char)startKeyBytes[1];
    char thirdCharStart = (char)startKeyBytes[2];
    EXIT: for (char c = (char)startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte [] t = new byte [] {(byte)c, (byte)d, (byte)e};
          if (endKey != null && endKey.length > 0
              && Bytes.compareTo(endKey, t) <= 0) {
            break EXIT;
          }
          try {
            Put put;
            if(ts != -1) {
              put = new Put(t, ts);
            } else {
              put = new Put(t);
            }
            try {
              StringBuilder sb = new StringBuilder();
              if (column != null && column.contains(":")) {
                sb.append(column);
              } else {
                if (columnFamily != null) {
                  sb.append(columnFamily);
                  if (!columnFamily.endsWith(":")) {
                    sb.append(":");
                  }
                  if (column != null) {
                    sb.append(column);
                  }
                }
              }
              byte[][] split =
                CellUtil.parseColumn(Bytes.toBytes(sb.toString()));
              if(split.length == 1) {
                byte[] qualifier = new byte[0];
                put.addColumn(split[0], qualifier, t);
              } else {
                put.addColumn(split[0], split[1], t);
              }
              put.setDurability(Durability.SKIP_WAL);
              updater.put(put);
              count++;
            } catch (RuntimeException ex) {
              ex.printStackTrace();
              throw ex;
            } catch (IOException ex) {
              ex.printStackTrace();
              throw ex;
            }
          } catch (RuntimeException ex) {
            ex.printStackTrace();
            throw ex;
          } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
          }
        }
        // Set start character back to FIRST_CHAR after we've done first loop.
        thirdCharStart = FIRST_CHAR;
      }
      secondCharStart = FIRST_CHAR;
    }
    return count;
  }

  protected void assertResultEquals(final HRegion region, final byte [] row,
      final byte [] family, final byte [] qualifier, final long timestamp,
      final byte [] value) throws IOException {
    Get get = new Get(row);
    get.setTimestamp(timestamp);
    Result res = region.get(get);
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
      res.getMap();
    byte [] res_value = map.get(family).get(qualifier).get(timestamp);

    if (value == null) {
      assertEquals(Bytes.toString(family) + " " + Bytes.toString(qualifier) +
          " at timestamp " + timestamp, null, res_value);
    } else {
      if (res_value == null) {
        fail(Bytes.toString(family) + " " + Bytes.toString(qualifier) +
            " at timestamp " + timestamp + "\" was expected to be \"" +
            Bytes.toStringBinary(value) + " but was null");
      }
      if (res_value != null) {
        assertEquals(Bytes.toString(family) + " " + Bytes.toString(qualifier) +
            " at timestamp " +
            timestamp, value, new String(res_value, StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Common method to close down a MiniDFSCluster and the associated file system
   *
   * @param cluster
   */
  public static void shutdownDfs(MiniDFSCluster cluster) {
    if (cluster != null) {
      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        /// Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
        FileSystem.closeAll();
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }
    }
  }

  /**
   * You must call {@link #closeRootAndMeta()} when done after calling this
   * method. It does cleanup.
   * @throws IOException
   */
  protected void createMetaRegion() throws IOException {
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(conf);
    meta = HBaseTestingUtility.createRegionAndWAL(HRegionInfo.FIRST_META_REGIONINFO, testDir,
        conf, fsTableDescriptors.get(TableName.META_TABLE_NAME));
  }

  protected void closeRootAndMeta() throws IOException {
    HBaseTestingUtility.closeRegionAndWAL(meta);
  }

  public static void assertByteEquals(byte[] expected,
                               byte[] actual) {
    if (Bytes.compareTo(expected, actual) != 0) {
      throw new AssertionFailedError("expected:<" +
      Bytes.toString(expected) + "> but was:<" +
      Bytes.toString(actual) + ">");
    }
  }

  public static void assertEquals(byte[] expected,
                               byte[] actual) {
    if (Bytes.compareTo(expected, actual) != 0) {
      throw new AssertionFailedError("expected:<" +
      Bytes.toStringBinary(expected) + "> but was:<" +
      Bytes.toStringBinary(actual) + ">");
    }
  }
}
