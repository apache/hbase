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
package org.apache.hadoop.hbase.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.HadoopSecurityEnabledUserProviderForTesting;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Base class for TestLoadIncrementalHFiles.
 */
public class TestLoadIncrementalHFilesBase {
  @Rule
  public TestName tn = new TestName();

  protected static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  protected static final byte[] FAMILY = Bytes.toBytes("myfam");
  private static final String NAMESPACE = "bulkNS";

  static final String EXPECTED_MSG_FOR_NON_EXISTING_FAMILY = "Unmatched family names found";
  static final int MAX_FILES_PER_REGION_PER_FAMILY = 4;

  protected static final byte[][] SPLIT_KEYS =
      new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("ppp") };

  static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    util.getConfiguration().setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
        MAX_FILES_PER_REGION_PER_FAMILY);
    // change default behavior so that tag values are returned with normal rpcs
    util.getConfiguration().set(HConstants.RPC_CODEC_CONF_KEY,
        KeyValueCodecWithTags.class.getCanonicalName());
    util.startMiniCluster();

    setupNamespace();
  }

  protected static void setupNamespace() throws Exception {
    util.getAdmin().createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  public static void secureSetUpBeforeClass() throws Exception {
    // set the always on security provider
    UserProvider.setUserProviderForTesting(util.getConfiguration(),
        HadoopSecurityEnabledUserProviderForTesting.class);
    // setup configuration
    SecureTestUtil.enableSecurity(util.getConfiguration());
    util.getConfiguration().setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
        MAX_FILES_PER_REGION_PER_FAMILY);
    // change default behavior so that tag values are returned with normal rpcs
    util.getConfiguration().set(HConstants.RPC_CODEC_CONF_KEY,
        KeyValueCodecWithTags.class.getCanonicalName());

    util.startMiniCluster();

    // Wait for the ACL table to become available
    util.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME);

    setupNamespace();
  }

  protected void runTest(String testName, BloomType bloomType, byte[][][] hfileRanges)
      throws Exception {
    runTest(testName, bloomType, null, hfileRanges);
  }

  protected void runTest(String testName, BloomType bloomType, byte[][][] hfileRanges,
      boolean useMap) throws Exception {
    runTest(testName, bloomType, null, hfileRanges, useMap);
  }

  protected void runTest(String testName, BloomType bloomType, byte[][] tableSplitKeys,
      byte[][][] hfileRanges) throws Exception {
    runTest(testName, bloomType, tableSplitKeys, hfileRanges, false);
  }

  protected void runTest(String testName, BloomType bloomType, byte[][] tableSplitKeys,
      byte[][][] hfileRanges, boolean useMap) throws Exception {
    final byte[] TABLE_NAME = Bytes.toBytes("mytable_" + testName);
    final boolean preCreateTable = tableSplitKeys != null;

    // Run the test bulkloading the table to the default namespace
    final TableName TABLE_WITHOUT_NS = TableName.valueOf(TABLE_NAME);
    runTest(testName, TABLE_WITHOUT_NS, bloomType, preCreateTable, tableSplitKeys, hfileRanges,
        useMap, 2);


    /* Run the test bulkloading the table from a depth of 3
      directory structure is now
      baseDirectory
          -- regionDir
            -- familyDir
              -- storeFileDir
    */
    if (preCreateTable) {
      runTest(testName + 2, TABLE_WITHOUT_NS, bloomType, true, tableSplitKeys, hfileRanges,
          false, 3);
    }

    // Run the test bulkloading the table to the specified namespace
    final TableName TABLE_WITH_NS = TableName.valueOf(Bytes.toBytes(NAMESPACE), TABLE_NAME);
    runTest(testName, TABLE_WITH_NS, bloomType, preCreateTable, tableSplitKeys, hfileRanges,
        useMap, 2);
  }

  protected void runTest(String testName, TableName tableName, BloomType bloomType,
      boolean preCreateTable, byte[][] tableSplitKeys, byte[][][] hfileRanges,
      boolean useMap, int depth) throws Exception {
    TableDescriptor htd = buildHTD(tableName, bloomType);
    runTest(testName, htd, preCreateTable, tableSplitKeys, hfileRanges, useMap, false, depth);
  }

  protected void runTest(String testName, TableDescriptor htd,
      boolean preCreateTable, byte[][] tableSplitKeys, byte[][][] hfileRanges, boolean useMap,
      boolean copyFiles, int depth) throws Exception {
    loadHFiles(testName, htd, util, FAMILY, QUALIFIER, preCreateTable, tableSplitKeys, hfileRanges,
        useMap, true, copyFiles, 0, 1000, depth);

    final TableName tableName = htd.getTableName();
    // verify staging folder has been cleaned up
    Path stagingBasePath =
        new Path(FSUtils.getRootDir(util.getConfiguration()), HConstants.BULKLOAD_STAGING_DIR_NAME);
    FileSystem fs = util.getTestFileSystem();
    if (fs.exists(stagingBasePath)) {
      FileStatus[] files = fs.listStatus(stagingBasePath);
      for (FileStatus file : files) {
        assertTrue("Folder=" + file.getPath() + " is not cleaned up.",
            file.getPath().getName() != "DONOTERASE");
      }
    }

    util.deleteTable(tableName);
  }

  protected TableDescriptor buildHTD(TableName tableName, BloomType bloomType) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setBloomFilterType(bloomType).build())
        .build();
  }

  public static int loadHFiles(String testName, TableDescriptor htd, HBaseTestingUtility util,
      byte[] fam, byte[] qual, boolean preCreateTable, byte[][] tableSplitKeys,
      byte[][][] hfileRanges, boolean useMap, boolean deleteFile, boolean copyFiles,
      int initRowCount, int factor, int depth) throws Exception {
    Path baseDirectory = util.getDataTestDirOnTestFS(testName);
    FileSystem fs = util.getTestFileSystem();
    baseDirectory = baseDirectory.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path parentDir = baseDirectory;
    if (depth == 3) {
      assert !useMap;
      parentDir = new Path(baseDirectory, "someRegion");
    }
    Path familyDir = new Path(parentDir, Bytes.toString(fam));

    int hfileIdx = 0;
    Map<byte[], List<Path>> map = null;
    List<Path> list = null;
    if (useMap || copyFiles) {
      list = new ArrayList<>();
    }
    if (useMap) {
      map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      map.put(fam, list);
    }
    Path last = null;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      Path path = new Path(familyDir, "hfile_" + hfileIdx++);
      HFileTestUtil.createHFile(util.getConfiguration(), fs, path, fam, qual, from, to, factor);
      if (useMap) {
        last = path;
        list.add(path);
      }
    }
    int expectedRows = hfileIdx * factor;

    TableName tableName = htd.getTableName();
    if (!util.getAdmin().tableExists(tableName) && (preCreateTable || map != null)) {
      util.getAdmin().createTable(htd, tableSplitKeys);
    }

    Configuration conf = util.getConfiguration();
    if (copyFiles) {
      conf.setBoolean(LoadIncrementalHFiles.ALWAYS_COPY_FILES, true);
    }
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf);
    List<String> args = Lists.newArrayList(baseDirectory.toString(), tableName.toString());
    if (depth == 3) {
      args.add("-loadTable");
    }

    if (useMap) {
      if (deleteFile) {
        fs.delete(last, true);
      }
      Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> loaded = loader.bulkLoad(tableName, map);
      if (deleteFile) {
        expectedRows -= 1000;
        for (BulkLoadHFiles.LoadQueueItem item : loaded.keySet()) {
          if (item.getFilePath().getName().equals(last.getName())) {
            fail(last + " should be missing");
          }
        }
      }
    } else {
      loader.run(args.toArray(new String[] {}));
    }

    if (copyFiles) {
      for (Path p : list) {
        assertTrue(p + " should exist", fs.exists(p));
      }
    }

    Table table = util.getConnection().getTable(tableName);
    try {
      assertEquals(initRowCount + expectedRows, util.countRows(table));
    } finally {
      table.close();
    }

    return expectedRows;
  }
}