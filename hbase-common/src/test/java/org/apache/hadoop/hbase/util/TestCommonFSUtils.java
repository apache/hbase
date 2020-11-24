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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link CommonFSUtils}.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestCommonFSUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCommonFSUtils.class);

  private HBaseCommonTestingUtility htu;
  private Configuration conf;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    conf = htu.getConfiguration();
  }

  /**
   * Test path compare and prefix checking.
   */
  @Test
  public void testMatchingTail() throws IOException {
    Path rootdir = htu.getDataTestDir();
    final FileSystem fs = rootdir.getFileSystem(conf);
    assertTrue(rootdir.depth() > 1);
    Path partPath = new Path("a", "b");
    Path fullPath = new Path(rootdir, partPath);
    Path fullyQualifiedPath = fs.makeQualified(fullPath);
    assertFalse(CommonFSUtils.isMatchingTail(fullPath, partPath));
    assertFalse(CommonFSUtils.isMatchingTail(fullPath, partPath.toString()));
    assertTrue(CommonFSUtils.isStartingWithPath(rootdir, fullPath.toString()));
    assertTrue(CommonFSUtils.isStartingWithPath(fullyQualifiedPath, fullPath.toString()));
    assertFalse(CommonFSUtils.isStartingWithPath(rootdir, partPath.toString()));
    assertFalse(CommonFSUtils.isMatchingTail(fullyQualifiedPath, partPath));
    assertTrue(CommonFSUtils.isMatchingTail(fullyQualifiedPath, fullPath));
    assertTrue(CommonFSUtils.isMatchingTail(fullyQualifiedPath, fullPath.toString()));
    assertTrue(CommonFSUtils.isMatchingTail(fullyQualifiedPath, fs.makeQualified(fullPath)));
    assertTrue(CommonFSUtils.isStartingWithPath(rootdir, fullyQualifiedPath.toString()));
    assertFalse(CommonFSUtils.isMatchingTail(fullPath, new Path("x")));
    assertFalse(CommonFSUtils.isMatchingTail(new Path("x"), fullPath));
  }

  private void WriteDataToHDFS(FileSystem fs, Path file, int dataSize)
    throws Exception {
    FSDataOutputStream out = fs.create(file);
    byte [] data = new byte[dataSize];
    out.write(data, 0, dataSize);
    out.close();
  }

  @Test
  public void testSetWALRootDir() throws Exception {
    Path p = new Path("file:///hbase/root");
    CommonFSUtils.setWALRootDir(conf, p);
    assertEquals(p.toString(), conf.get(CommonFSUtils.HBASE_WAL_DIR));
  }

  @Test
  public void testGetWALRootDir() throws IOException {
    Path root = new Path("file:///hbase/root");
    Path walRoot = new Path("file:///hbase/logroot");
    CommonFSUtils.setRootDir(conf, root);
    assertEquals(root, CommonFSUtils.getRootDir(conf));
    assertEquals(root, CommonFSUtils.getWALRootDir(conf));
    CommonFSUtils.setWALRootDir(conf, walRoot);
    assertEquals(walRoot, CommonFSUtils.getWALRootDir(conf));
  }

  @Test
  public void testGetWALRootDirUsingUri() throws IOException {
    Path root = new Path("file:///hbase/root");
    conf.set(HConstants.HBASE_DIR, root.toString());
    Path walRoot = new Path("file:///hbase/logroot");
    conf.set(CommonFSUtils.HBASE_WAL_DIR, walRoot.toString());
    String walDirUri = CommonFSUtils.getDirUri(conf, walRoot);
    String rootDirUri = CommonFSUtils.getDirUri(conf, root);
    CommonFSUtils.setFsDefault(this.conf, rootDirUri);
    CommonFSUtils.setRootDir(conf, root);
    assertEquals(root, CommonFSUtils.getRootDir(conf));
    CommonFSUtils.setFsDefault(this.conf, walDirUri);
    CommonFSUtils.setWALRootDir(conf, walRoot);
    assertEquals(walRoot, CommonFSUtils.getWALRootDir(conf));
  }

  @Test(expected=IllegalStateException.class)
  public void testGetWALRootDirIllegalWALDir() throws IOException {
    Path root = new Path("file:///hbase/root");
    Path invalidWALDir = new Path("file:///hbase/root/logroot");
    CommonFSUtils.setRootDir(conf, root);
    CommonFSUtils.setWALRootDir(conf, invalidWALDir);
    CommonFSUtils.getWALRootDir(conf);
  }

  @Test
  public void testRemoveWALRootPath() throws Exception {
    CommonFSUtils.setRootDir(conf, new Path("file:///user/hbase"));
    Path testFile = new Path(CommonFSUtils.getRootDir(conf), "test/testfile");
    Path tmpFile = new Path("file:///test/testfile");
    assertEquals("test/testfile", CommonFSUtils.removeWALRootPath(testFile, conf));
    assertEquals(tmpFile.toString(), CommonFSUtils.removeWALRootPath(tmpFile, conf));
    CommonFSUtils.setWALRootDir(conf, new Path("file:///user/hbaseLogDir"));
    assertEquals(testFile.toString(), CommonFSUtils.removeWALRootPath(testFile, conf));
    Path logFile = new Path(CommonFSUtils.getWALRootDir(conf), "test/testlog");
    assertEquals("test/testlog", CommonFSUtils.removeWALRootPath(logFile, conf));
  }
}
