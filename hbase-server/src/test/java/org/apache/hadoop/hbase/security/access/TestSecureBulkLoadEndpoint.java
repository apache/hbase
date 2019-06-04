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

package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint.Consumer;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Multimap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the SecureBulkLoadEndpoint code.
 */
@Category(MediumTests.class)
public class TestSecureBulkLoadEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureBulkLoadEndpoint.class);

  private static TableName TABLE = TableName.valueOf(Bytes.toBytes("TestSecureBulkLoadManager"));
  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] COLUMN = Bytes.toBytes("column");
  private static byte[] key1 = Bytes.toBytes("row1");
  private static byte[] key2 = Bytes.toBytes("row2");
  private static byte[] key3 = Bytes.toBytes("row3");
  private static byte[] value1 = Bytes.toBytes("t1");
  private static byte[] value3 = Bytes.toBytes("t3");
  private static byte[] SPLIT_ROWKEY = key2;

  private Thread ealierBulkload;
  private Thread laterBulkload;

  protected final static HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private static Configuration conf = testUtil.getConfiguration();

  @BeforeClass
  public static void setUp() throws Exception {
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,SecureBulkLoadEndpoint.class.getName());
    testUtil.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
    testUtil.cleanupTestDir();
  }

  @Test
  public void testFileSystemsWithoutPermissionSupport() {
    final Configuration emptyConf = new Configuration(false);
    final Configuration defaultConf = HBaseConfiguration.create();

    final Set<String> expectedDefaultIgnoredSchemes = new HashSet<>(
        Arrays.asList(
          StringUtils.split(SecureBulkLoadEndpoint.FS_WITHOUT_SUPPORT_PERMISSION_DEFAULT, ',')));

    final SecureBulkLoadEndpoint endpoint = new SecureBulkLoadEndpoint();

    // Empty configuration should return the default list of schemes
    Set<String> defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(
        emptyConf);
    assertEquals(defaultIgnoredSchemes, expectedDefaultIgnoredSchemes);

    // Default configuration (unset) should be the default list of schemes
    defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(defaultConf);
    assertEquals(defaultIgnoredSchemes, expectedDefaultIgnoredSchemes);

    defaultConf.set(SecureBulkLoadEndpoint.FS_WITHOUT_SUPPORT_PERMISSION_KEY, "foo,bar");
    defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(defaultConf);
    assertEquals(defaultIgnoredSchemes, new HashSet<String>(Arrays.asList("foo", "bar")));
  }

  /**
   * After a secure bulkload finished , there is a clean-up for FileSystems used in the bulkload.
   * Sometimes, FileSystems used in the finished bulkload might also be used in other bulkload
   * calls, or there are other FileSystems created by the same user, they could be closed by a
   * FileSystem.closeAllForUGI call. So during the clean-up, those FileSystems need to be used
   * later can not get closed ,or else a race condition occurs.
   *
   * testForRaceCondition tests the case that two secure bulkload calls from the same UGI go
   * into two different regions and one bulkload finishes earlier when the other bulkload still
   * needs its FileSystems, checks that both bulkloads succeed.
   */
  @Test
  public void testForRaceCondition() throws Exception {
    /// create table
    testUtil.createTable(TABLE,FAMILY,Bytes.toByteArrays(SPLIT_ROWKEY));
    testUtil.waitUntilAllRegionsAssigned(TABLE);

    Consumer<Region> fsCreatedListener = new Consumer<Region>() {
      @Override
      public void accept(Region hRegion) {
        if (hRegion.getRegionInfo().containsRow(key3)) {
          Threads.shutdown(ealierBulkload);/// wait util the other bulkload finished
        }
      }
    };
    SecureBulkLoadEndpoint.setFsCreatedListener(fsCreatedListener);

    /// prepare files
    Path rootdir = testUtil.getMiniHBaseCluster().getRegionServerThreads().get(0)
        .getRegionServer().getFileSystem().getHomeDirectory();
    final Path dir1 = new Path(rootdir, "dir1");
    prepareHFile(dir1, key1, value1);
    final Path dir2 = new Path(rootdir, "dir2");
    prepareHFile(dir2, key3, value3);

    /// do bulkload
    final AtomicReference<Throwable> t1Exception = new AtomicReference<>();
    final AtomicReference<Throwable> t2Exception = new AtomicReference<>();
    ealierBulkload = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          doBulkloadWithoutRetry(dir1);
        } catch (Exception e) {
          LOG.error("bulk load failed .",e);
          t1Exception.set(e);
        }
      }
    });
    laterBulkload = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          doBulkloadWithoutRetry(dir2);
        } catch (Exception e) {
          LOG.error("bulk load failed .",e);
          t2Exception.set(e);
        }
      }
    });
    ealierBulkload.start();
    laterBulkload.start();
    Threads.shutdown(ealierBulkload);
    Threads.shutdown(laterBulkload);
    Assert.assertNull(t1Exception.get());
    Assert.assertNull(t2Exception.get());

    /// check bulkload ok
    Get get1 = new Get(key1);
    Get get3 = new Get(key3);
    Table t = testUtil.getConnection().getTable(TABLE);
    Result r = t.get(get1);
    Assert.assertArrayEquals(r.getValue(FAMILY, COLUMN), value1);
    r = t.get(get3);
    Assert.assertArrayEquals(r.getValue(FAMILY, COLUMN), value3);

  }

  /**
   * A trick is used to make sure server-side failures( if any ) not being covered up by a client
   * retry. Since LoadIncrementalHFiles.doBulkLoad keeps performing bulkload calls as long as the
   * HFile queue is not empty, while server-side exceptions in the doAs block do not lead
   * to a client exception, a bulkload will always succeed in this case by default, thus client
   * will never be aware that failures have ever happened . To avoid this kind of retry ,
   * a MyExceptionToAvoidRetry exception is thrown after bulkLoadPhase finished and caught
   * silently outside the doBulkLoad call, so that the bulkLoadPhase would be called exactly
   * once, and server-side failures, if any ,can be checked via data.
   */
  class MyExceptionToAvoidRetry extends DoNotRetryIOException {
  }

  private void doBulkloadWithoutRetry(Path dir) throws Exception {
    Connection connection = testUtil.getConnection();
    LoadIncrementalHFiles h = new LoadIncrementalHFiles(conf) {
      @Override
      protected void bulkLoadPhase(final Table table, final Connection conn,
          ExecutorService pool, Deque<LoadQueueItem> queue,
          final Multimap<ByteBuffer, LoadQueueItem> regionGroups) throws IOException {
        super.bulkLoadPhase(table, conn, pool, queue, regionGroups);
        throw new MyExceptionToAvoidRetry(); // throw exception to avoid retry
      }
    };
    try {
      h.doBulkLoad(dir, testUtil.getHBaseAdmin(), connection.getTable(TABLE),
          connection.getRegionLocator(TABLE));
      Assert.fail("MyExceptionToAvoidRetry is expected");
    } catch (MyExceptionToAvoidRetry e) { //expected
    }
  }

  private void prepareHFile(Path dir, byte[] key, byte[] value) throws Exception {
    HTableDescriptor desc = testUtil.getHBaseAdmin().getTableDescriptor(TABLE);
    HColumnDescriptor family = desc.getFamily(FAMILY);
    Compression.Algorithm compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;

    CacheConfig writerCacheConf = new CacheConfig(conf, family);
    writerCacheConf.setCacheDataOnWrite(false);
    HFileContext hFileContext = new HFileContextBuilder()
        .withIncludesMvcc(false)
        .withIncludesTags(true)
        .withCompression(compression)
        .withCompressTags(family.isCompressTags())
        .withChecksumType(HStore.getChecksumType(conf))
        .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
        .withBlockSize(family.getBlocksize())
        .withHBaseCheckSum(true)
        .withDataBlockEncoding(family.getDataBlockEncoding())
        .withEncryptionContext(Encryption.Context.NONE)
        .withCreateTime(EnvironmentEdgeManager.currentTime())
        .build();
    StoreFile.WriterBuilder builder =
        new StoreFile.WriterBuilder(conf, writerCacheConf, dir.getFileSystem(conf))
            .withOutputDir(new Path(dir, family.getNameAsString()))
            .withBloomType(family.getBloomFilterType())
            .withMaxKeyCount(Integer.MAX_VALUE)
            .withFileContext(hFileContext);
    StoreFile.Writer writer = builder.build();

    Put put = new Put(key);
    put.addColumn(FAMILY, COLUMN, value);
    for (Cell c : put.get(FAMILY, COLUMN)) {
      writer.append(c);
    }

    writer.close();
  }

}
