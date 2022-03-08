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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Tests for failedBulkLoad logic to make sure staged files are returned to their original location
 * if the bulkload have failed.
 */
@Category({MiscTests.class, LargeTests.class})
public class TestSecureBulkloadListener {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecureBulkloadListener.class);

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private Configuration conf;
  private MiniDFSCluster cluster;
  private HBaseTestingUtil htu;
  private DistributedFileSystem dfs;
  private final byte[] randomBytes = new byte[100];
  private static final String host1 = "host1";
  private static final String host2 = "host2";
  private static final String host3 = "host3";
  private static byte[] FAMILY = Bytes.toBytes("family");
  private static final String STAGING_DIR = "staging";
  private static final String CUSTOM_STAGING_DIR = "customStaging";

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    Bytes.random(randomBytes);
    htu = new HBaseTestingUtil();
    htu.getConfiguration().setInt("dfs.blocksize", 1024);// For the test with multiple blocks
    htu.getConfiguration().setInt("dfs.replication", 3);
    htu.startMiniDFSCluster(3,
        new String[]{"/r1", "/r2", "/r3"}, new String[]{host1, host2, host3});

    conf = htu.getConfiguration();
    cluster = htu.getDFSCluster();
    dfs = (DistributedFileSystem) FileSystem.get(conf);
  }

  @After
  public void tearDownAfterClass() throws Exception {
    htu.shutdownMiniCluster();
  }

  @Test
  public void testMovingStagedFile() throws Exception {
    Path stagingDirPath =
      new Path(dfs.getWorkingDirectory(), new Path(name.getMethodName(), STAGING_DIR));
    if (!dfs.exists(stagingDirPath)) {
      dfs.mkdirs(stagingDirPath);
    }
    SecureBulkLoadManager.SecureBulkLoadListener listener =
      new SecureBulkLoadManager.SecureBulkLoadListener(dfs, stagingDirPath.toString(), conf);

    //creating file to load
    String srcFile = createHFileForFamilies(FAMILY);
    Path srcPath = new Path(srcFile);
    Assert.assertTrue(dfs.exists(srcPath));

    Path stagedFamily = new Path(stagingDirPath, new Path(Bytes.toString(FAMILY)));
    if (!dfs.exists(stagedFamily)) {
      dfs.mkdirs(stagedFamily);
    }

    //moving file to staging
    String stagedFile = listener.prepareBulkLoad(FAMILY, srcFile, false, null);
    Path stagedPath = new Path(stagedFile);
    Assert.assertTrue(dfs.exists(stagedPath));
    Assert.assertFalse(dfs.exists(srcPath));

    //moving files back to original location after a failed bulkload
    listener.failedBulkLoad(FAMILY, stagedFile);
    Assert.assertFalse(dfs.exists(stagedPath));
    Assert.assertTrue(dfs.exists(srcPath));
  }

  @Test
  public void testMovingStagedFileWithCustomStageDir() throws Exception {
    Path stagingDirPath =
      new Path(dfs.getWorkingDirectory(), new Path(name.getMethodName(), STAGING_DIR));
    if (!dfs.exists(stagingDirPath)) {
      dfs.mkdirs(stagingDirPath);
    }
    SecureBulkLoadManager.SecureBulkLoadListener listener =
      new SecureBulkLoadManager.SecureBulkLoadListener(dfs, stagingDirPath.toString(), conf);

    //creating file to load
    String srcFile = createHFileForFamilies(FAMILY);
    Path srcPath = new Path(srcFile);
    Assert.assertTrue(dfs.exists(srcPath));

    Path stagedFamily = new Path(stagingDirPath, new Path(Bytes.toString(FAMILY)));
    if (!dfs.exists(stagedFamily)) {
      dfs.mkdirs(stagedFamily);
    }

    Path customStagingDirPath =
      new Path(dfs.getWorkingDirectory(), new Path(name.getMethodName(), CUSTOM_STAGING_DIR));
    Path customStagedFamily = new Path(customStagingDirPath, new Path(Bytes.toString(FAMILY)));
    if (!dfs.exists(customStagedFamily)) {
      dfs.mkdirs(customStagedFamily);
    }

    //moving file to staging using a custom staging dir
    String stagedFile =
      listener.prepareBulkLoad(FAMILY, srcFile, false, customStagingDirPath.toString());
    Path stagedPath = new Path(stagedFile);
    Assert.assertTrue(dfs.exists(stagedPath));
    Assert.assertFalse(dfs.exists(srcPath));

    //moving files back to original location after a failed bulkload
    listener.failedBulkLoad(FAMILY, stagedFile);
    Assert.assertFalse(dfs.exists(stagedPath));
    Assert.assertTrue(dfs.exists(srcPath));
  }

  @Test
  public void testCopiedStagedFile() throws Exception {
    Path stagingDirPath =
      new Path(dfs.getWorkingDirectory(), new Path(name.getMethodName(), STAGING_DIR));
    if (!dfs.exists(stagingDirPath)) {
      dfs.mkdirs(stagingDirPath);
    }
    SecureBulkLoadManager.SecureBulkLoadListener listener =
      new SecureBulkLoadManager.SecureBulkLoadListener(dfs, stagingDirPath.toString(), conf);

    //creating file to load
    String srcFile = createHFileForFamilies(FAMILY);
    Path srcPath = new Path(srcFile);
    Assert.assertTrue(dfs.exists(srcPath));

    Path stagedFamily = new Path(stagingDirPath, new Path(Bytes.toString(FAMILY)));
    if (!dfs.exists(stagedFamily)) {
      dfs.mkdirs(stagedFamily);
    }

    //copying file to staging
    String stagedFile = listener.prepareBulkLoad(FAMILY, srcFile, true, null);
    Path stagedPath = new Path(stagedFile);
    Assert.assertTrue(dfs.exists(stagedPath));
    Assert.assertTrue(dfs.exists(srcPath));

    //should do nothing because the original file was copied to staging
    listener.failedBulkLoad(FAMILY, stagedFile);
    Assert.assertTrue(dfs.exists(stagedPath));
    Assert.assertTrue(dfs.exists(srcPath));
  }

  @Test(expected = IOException.class)
  public void testDeletedStagedFile() throws Exception {
    Path stagingDirPath =
      new Path(dfs.getWorkingDirectory(), new Path(name.getMethodName(), STAGING_DIR));
    if (!dfs.exists(stagingDirPath)) {
      dfs.mkdirs(stagingDirPath);
    }
    SecureBulkLoadManager.SecureBulkLoadListener listener =
      new SecureBulkLoadManager.SecureBulkLoadListener(dfs, stagingDirPath.toString(), conf);

    //creating file to load
    String srcFile = createHFileForFamilies(FAMILY);
    Path srcPath = new Path(srcFile);
    Assert.assertTrue(dfs.exists(srcPath));

    Path stagedFamily = new Path(stagingDirPath, new Path(Bytes.toString(FAMILY)));
    if (!dfs.exists(stagedFamily)) {
      dfs.mkdirs(stagedFamily);
    }

    //moving file to staging
    String stagedFile = listener.prepareBulkLoad(FAMILY, srcFile, false, null);
    Path stagedPath = new Path(stagedFile);
    Assert.assertTrue(dfs.exists(stagedPath));
    Assert.assertFalse(dfs.exists(srcPath));

    dfs.delete(stagedPath, false);

    //moving files back to original location after a failed bulkload
    listener.failedBulkLoad(FAMILY, stagedFile);
  }

  private String createHFileForFamilies(byte[] family) throws IOException {
    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(conf);
    Path testDir = new Path(dfs.getWorkingDirectory() , new Path(name.getMethodName(), Bytes.toString(family)));
    if(!dfs.exists(testDir)){
      dfs.mkdirs(testDir);
    }
    Path hfilePath = new Path(testDir, generateUniqueName(null));
    FSDataOutputStream out = dfs.createFile(hfilePath).build();
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContextBuilder().build());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
          .setRow(randomBytes).setFamily(family).setQualifier(randomBytes).setTimestamp(0L)
          .setType(KeyValue.Type.Put.getCode()).setValue(randomBytes).build()));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return hfilePath.toString();
  }

  private static String generateUniqueName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (suffix != null) name += suffix;
    return name;
  }

}
