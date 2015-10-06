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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.legacy.LegacyTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link FSTableDescriptors}.
 */
// Do not support to be executed in he same JVM as other tests
@Category({MiscTests.class, MediumTests.class})
public class TestFSTableDescriptors {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestFSTableDescriptors.class);

  @Test (expected=IllegalArgumentException.class)
  public void testRegexAgainstOldStyleTableInfo() {
    Path p = new Path("/tmp", LegacyTableDescriptor.TABLEINFO_FILE_PREFIX);
    int i = LegacyTableDescriptor.getTableInfoSequenceId(p);
    assertEquals(0, i);
    // Assert it won't eat garbage -- that it fails
    p = new Path("/tmp", "abc");
    LegacyTableDescriptor.getTableInfoSequenceId(p);
  }

  @Test
  public void testCreateAndUpdate() throws IOException {
    Path testdir = UTIL.getDataTestDir("testCreateAndUpdate");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testCreate"));
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(UTIL.getConfiguration(), fs, testdir);
    assertTrue(fstd.createTableDescriptor(htd));
    assertFalse(fstd.createTableDescriptor(htd));
    FileStatus [] statuses = fs.listStatus(testdir);
    assertTrue("statuses.length="+statuses.length, statuses.length == 1);
    for (int i = 0; i < 10; i++) {
      fstd.updateTableDescriptor(htd);
    }
    statuses = fs.listStatus(testdir);
    assertTrue(statuses.length == 1);
    Path tmpTableDir = new Path(FSUtils.getTableDir(testdir, htd.getTableName()), ".tmp");
    statuses = fs.listStatus(tmpTableDir);
    assertTrue(statuses.length == 0);
  }

  @Test
  public void testSequenceIdAdvancesOnTableInfo() throws IOException {
    Path testdir = UTIL.getDataTestDir("testSequenceidAdvancesOnTableInfo");
    HTableDescriptor htd = new HTableDescriptor(
        TableName.valueOf("testSequenceidAdvancesOnTableInfo"));
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    Path tableDir = FSUtils.getTableDir(testdir, htd.getTableName());
    FSTableDescriptors fstd = new FSTableDescriptors(UTIL.getConfiguration(), fs, testdir);
    fstd.updateTableDescriptor(htd);
    Path p0 = LegacyTableDescriptor.getTableInfoPath(fs, tableDir).getPath();
    int i0 = LegacyTableDescriptor.getTableInfoSequenceId(p0);
    fstd.updateTableDescriptor(htd);
    Path p1 = LegacyTableDescriptor.getTableInfoPath(fs, tableDir).getPath();
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p0));
    int i1 = LegacyTableDescriptor.getTableInfoSequenceId(p1);
    assertTrue(i1 == i0 + 1);
    fstd.updateTableDescriptor(htd);
    Path p2 = LegacyTableDescriptor.getTableInfoPath(fs, tableDir).getPath();
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p1));
    int i2 = LegacyTableDescriptor.getTableInfoSequenceId(p2);
    assertTrue(i2 == i1 + 1);
    fstd.updateTableDescriptor(htd);
    Path p3 = LegacyTableDescriptor.getTableInfoPath(fs, tableDir).getPath();
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p2));
    int i3 = LegacyTableDescriptor.getTableInfoSequenceId(p3);
    assertTrue(i3 == i2 + 1);
    HTableDescriptor descriptor = fstd.get(htd.getTableName());
    assertEquals(descriptor, htd);
  }

  @Test
  public void testFormatTableInfoSequenceId() {
    Path p0 = assertWriteAndReadSequenceId(0);
    // Assert p0 has format we expect.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < LegacyTableDescriptor.WIDTH_OF_SEQUENCE_ID; i++) {
      sb.append("0");
    }
    assertEquals(LegacyTableDescriptor.TABLEINFO_FILE_PREFIX + "." + sb.toString(),
      p0.getName());
    // Check a few more.
    Path p2 = assertWriteAndReadSequenceId(2);
    Path p10000 = assertWriteAndReadSequenceId(10000);
    // Get a .tablinfo that has no sequenceid suffix.
    Path p = new Path(p0.getParent(), LegacyTableDescriptor.TABLEINFO_FILE_PREFIX);
    FileStatus fs = new FileStatus(0, false, 0, 0, 0, p);
    FileStatus fs0 = new FileStatus(0, false, 0, 0, 0, p0);
    FileStatus fs2 = new FileStatus(0, false, 0, 0, 0, p2);
    FileStatus fs10000 = new FileStatus(0, false, 0, 0, 0, p10000);
    Comparator<FileStatus> comparator = LegacyTableDescriptor.TABLEINFO_FILESTATUS_COMPARATOR;
    assertTrue(comparator.compare(fs, fs0) > 0);
    assertTrue(comparator.compare(fs0, fs2) > 0);
    assertTrue(comparator.compare(fs2, fs10000) > 0);
  }

  private Path assertWriteAndReadSequenceId(final int i) {
    Path p = new Path("/tmp", LegacyTableDescriptor.getTableInfoFileName(i));
    int ii = LegacyTableDescriptor.getTableInfoSequenceId(p);
    assertEquals(i, ii);
    return p;
  }

  @Test
  public void testRemoves() throws IOException {
    final String name = "testRemoves";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    htds.add(htd);
    assertNotNull(htds.remove(htd.getTableName()));
    assertNull(htds.remove(htd.getTableName()));
  }

  @Test public void testReadingHTDFromFS() throws IOException {
    final String name = "testReadingHTDFromFS";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    Path rootdir = UTIL.getDataTestDir(name);
    FSTableDescriptors fstd = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    fstd.createTableDescriptor(htd);
    HTableDescriptor td2 =
      LegacyTableDescriptor.getTableDescriptorFromFs(fs, rootdir, htd.getTableName());
    assertTrue(htd.equals(td2));
  }

  @Test public void testReadingOldHTDFromFS() throws IOException, DeserializationException {
    final String name = "testReadingOldHTDFromFS";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    Path rootdir = UTIL.getDataTestDir(name);
    FSTableDescriptors fstd = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    Path tableDir = FSUtils.getTableDir(rootdir, htd.getTableName());
    fstd.updateTableDescriptor(htd);
    Path descriptorFile = LegacyTableDescriptor.getTableInfoPath(fs, tableDir).getPath();
    FSUtils.writeFully(fs, descriptorFile, htd.toByteArray(), true);
    FSTableDescriptors fstd2 = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    HTableDescriptor td2 = fstd2.getDescriptor(htd.getTableName());
    assertEquals(htd, td2);
    FileStatus descriptorFile2 = LegacyTableDescriptor.getTableInfoPath(fs, tableDir);
    byte[] buffer = htd.toByteArray();
    FSUtils.readFully(fs, descriptorFile2.getPath(), buffer);
    TableDescriptor td3 = TableDescriptor.parseFrom(buffer);
    assertEquals(htd, td3);
  }

  @Test public void testHTableDescriptors()
  throws IOException, InterruptedException {
    final String name = "testHTableDescriptors";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors htds = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir) {
      @Override
      public HTableDescriptor get(TableName tablename)
          throws TableExistsException, FileNotFoundException, IOException {
        LOG.info(tablename + ", cachehits=" + this.cachehits);
        return super.get(tablename);
      }
    };
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(
          new HTableDescriptor(TableName.valueOf(name + i)));
      htds.createTableDescriptor(htd);
    }

    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name + i)) !=  null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name + i)) !=  null);
    }
    // Update the table infos
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name + i));
      htd.addFamily(new HColumnDescriptor("" + i));
      htds.updateTableDescriptor(htd);
    }
    // Wait a while so mod time we write is for sure different.
    Thread.sleep(100);
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name + i)) !=  null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name + i)) !=  null);
    }
    assertEquals(count * 4, htds.invocations);
    assertTrue("expected=" + (count * 2) + ", actual=" + htds.cachehits,
      htds.cachehits >= (count * 2));
  }

  @Test
  public void testHTableDescriptorsNoCache()
    throws IOException, InterruptedException {
    final String name = "testHTableDescriptorsNoCache";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors htds = new FSTableDescriptorsTest(UTIL.getConfiguration(), fs, rootdir,
      false, false);
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      htds.createTableDescriptor(htd);
    }

    for (int i = 0; i < 2 * count; i++) {
      assertNotNull("Expected HTD, got null instead", htds.get(TableName.valueOf(name + i % 2)));
    }
    // Update the table infos
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name + i));
      htd.addFamily(new HColumnDescriptor("" + i));
      htds.updateTableDescriptor(htd);
    }
    for (int i = 0; i < count; i++) {
      assertNotNull("Expected HTD, got null instead", htds.get(TableName.valueOf(name + i)));
      assertTrue("Column Family " + i + " missing",
                 htds.get(TableName.valueOf(name + i)).hasFamily(Bytes.toBytes("" + i)));
    }
    assertEquals(count * 4, htds.invocations);
    assertEquals("expected=0, actual=" + htds.cachehits, 0, htds.cachehits);
  }

  @Test
  public void testGetAll()
    throws IOException, InterruptedException {
    final String name = "testGetAll";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors htds = new FSTableDescriptorsTest(UTIL.getConfiguration(), fs, rootdir);
    final int count = 4;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      htds.createTableDescriptor(htd);
    }
    // add hbase:meta
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    htds.createTableDescriptor(htd);

    assertEquals("getAll() didn't return all TableDescriptors, expected: " +
                   (count + 1) + " got: " + htds.getAll().size(),
                 count + 1, htds.getAll().size());

  }

  @Test
  public void testCacheConsistency()
    throws IOException, InterruptedException {
    final String name = "testCacheConsistency";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors chtds = new FSTableDescriptorsTest(UTIL.getConfiguration(), fs, rootdir);
    FSTableDescriptors nonchtds = new FSTableDescriptorsTest(UTIL.getConfiguration(), fs,
      rootdir, false, false);

    final int count = 10;
    // Write out table infos via non-cached FSTableDescriptors
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      nonchtds.createTableDescriptor(htd);
    }

    // Calls to getAll() won't increase the cache counter, do per table.
    for (int i = 0; i < count; i++) {
      assertTrue(chtds.get(TableName.valueOf(name + i)) !=  null);
    }

    assertTrue(nonchtds.getAll().size() == chtds.getAll().size());

    // add a new entry for hbase:meta
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    nonchtds.createTableDescriptor(htd);

    // hbase:meta will only increase the cachehit by 1
    assertTrue(nonchtds.getAll().size() == chtds.getAll().size());

    for (Map.Entry entry: nonchtds.getAll().entrySet()) {
      String t = (String) entry.getKey();
      HTableDescriptor nchtd = (HTableDescriptor) entry.getValue();
      assertTrue("expected " + htd.toString() +
                   " got: " + chtds.get(TableName.valueOf(t)).toString(),
                 (nchtd.equals(chtds.get(TableName.valueOf(t)))));
    }
  }

  @Test
  public void testNoSuchTable() throws IOException {
    final String name = "testNoSuchTable";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    assertNull("There shouldn't be any HTD for this table",
      htds.get(TableName.valueOf("NoSuchTable")));
  }

  @Test
  public void testUpdates() throws IOException {
    final String name = "testUpdates";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(UTIL.getConfiguration(), fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    htds.add(htd);
    htds.add(htd);
    htds.add(htd);
  }

  @Test
  public void testTableInfoFileStatusComparator() {
    FileStatus bare =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp", LegacyTableDescriptor.TABLEINFO_FILE_PREFIX));
    FileStatus future =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis()));
    FileStatus farFuture =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis() + 1000));
    FileStatus [] alist = {bare, future, farFuture};
    FileStatus [] blist = {bare, farFuture, future};
    FileStatus [] clist = {farFuture, bare, future};
    Comparator<FileStatus> c = LegacyTableDescriptor.TABLEINFO_FILESTATUS_COMPARATOR;
    Arrays.sort(alist, c);
    Arrays.sort(blist, c);
    Arrays.sort(clist, c);
    // Now assert all sorted same in way we want.
    for (int i = 0; i < alist.length; i++) {
      assertTrue(alist[i].equals(blist[i]));
      assertTrue(blist[i].equals(clist[i]));
      assertTrue(clist[i].equals(i == 0? farFuture: i == 1? future: bare));
    }
  }

  @Test
  public void testReadingInvalidDirectoryFromFS() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    try {
      new FSTableDescriptors(UTIL.getConfiguration(), fs,
          FSUtils.getRootDir(UTIL.getConfiguration()))
          .get(TableName.valueOf(HConstants.HBASE_TEMP_DIRECTORY));
      fail("Shouldn't be able to read a table descriptor for the archive directory.");
    } catch (Exception e) {
      LOG.debug("Correctly got error when reading a table descriptor from the archive directory: "
          + e.getMessage());
    }
  }

  @Test
  public void testCreateTableDescriptorUpdatesIfExistsAlready() throws IOException {
    Path testdir = UTIL.getDataTestDir("testCreateTableDescriptorUpdatesIfThereExistsAlready");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(
        "testCreateTableDescriptorUpdatesIfThereExistsAlready"));
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(UTIL.getConfiguration(), fs, testdir);
    assertTrue(fstd.createTableDescriptor(htd));
    assertFalse(fstd.createTableDescriptor(htd));
    htd.setValue(Bytes.toBytes("mykey"), Bytes.toBytes("myValue"));
    assertTrue(fstd.createTableDescriptor(htd)); //this will re-create
    Path tableDir = FSUtils.getTableDir(testdir, htd.getTableName());
    Path tmpTableDir = new Path(tableDir, LegacyTableDescriptor.TMP_DIR);
    FileStatus[] statuses = fs.listStatus(tmpTableDir);
    assertTrue(statuses.length == 0);

    assertEquals(htd, LegacyTableDescriptor.getTableDescriptorFromFs(fs, tableDir));
  }

  private static class FSTableDescriptorsTest extends FSTableDescriptors {

    public FSTableDescriptorsTest(Configuration conf, FileSystem fs, Path rootdir)
      throws IOException {
      this(conf, fs, rootdir, false, true);
    }

    public FSTableDescriptorsTest(Configuration conf, FileSystem fs, Path rootdir,
      boolean fsreadonly, boolean usecache) throws IOException {
      super(conf, fs, rootdir, fsreadonly, usecache);
    }

    @Override
    public HTableDescriptor get(TableName tablename)
      throws TableExistsException, FileNotFoundException, IOException {
      LOG.info((super.isUsecache() ? "Cached" : "Non-Cached") +
                 " HTableDescriptor.get() on " + tablename + ", cachehits=" + this.cachehits);
      return super.get(tablename);
    }
  }
}

