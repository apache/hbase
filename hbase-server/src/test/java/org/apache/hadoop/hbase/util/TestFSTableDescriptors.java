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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Tests for {@link FSTableDescriptors}.
 */
// Do not support to be executed in he same JVM as other tests
@Category(MediumTests.class)
public class TestFSTableDescriptors {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestFSTableDescriptors.class);

  @Test (expected=IllegalArgumentException.class)
  public void testRegexAgainstOldStyleTableInfo() {
    Path p = new Path("/tmp", FSTableDescriptors.TABLEINFO_FILE_PREFIX);
    int i = FSTableDescriptors.getTableInfoSequenceId(p);
    assertEquals(0, i);
    // Assert it won't eat garbage -- that it fails
    p = new Path("/tmp", "abc");
    FSTableDescriptors.getTableInfoSequenceId(p);
  }

  @Test
  public void testCreateAndUpdate() throws IOException {
    Path testdir = UTIL.getDataTestDir("testCreateAndUpdate");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testCreate"));
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testdir);
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
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testdir);
    Path p0 = fstd.updateTableDescriptor(htd);
    int i0 = FSTableDescriptors.getTableInfoSequenceId(p0);
    Path p1 = fstd.updateTableDescriptor(htd);
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p0));
    int i1 = FSTableDescriptors.getTableInfoSequenceId(p1);
    assertTrue(i1 == i0 + 1);
    Path p2 = fstd.updateTableDescriptor(htd);
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p1));
    int i2 = FSTableDescriptors.getTableInfoSequenceId(p2);
    assertTrue(i2 == i1 + 1);
  }

  @Test
  public void testFormatTableInfoSequenceId() {
    Path p0 = assertWriteAndReadSequenceId(0);
    // Assert p0 has format we expect.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < FSTableDescriptors.WIDTH_OF_SEQUENCE_ID; i++) {
      sb.append("0");
    }
    assertEquals(FSTableDescriptors.TABLEINFO_FILE_PREFIX + "." + sb.toString(),
      p0.getName());
    // Check a few more.
    Path p2 = assertWriteAndReadSequenceId(2);
    Path p10000 = assertWriteAndReadSequenceId(10000);
    // Get a .tablinfo that has no sequenceid suffix.
    Path p = new Path(p0.getParent(), FSTableDescriptors.TABLEINFO_FILE_PREFIX);
    FileStatus fs = new FileStatus(0, false, 0, 0, 0, p);
    FileStatus fs0 = new FileStatus(0, false, 0, 0, 0, p0);
    FileStatus fs2 = new FileStatus(0, false, 0, 0, 0, p2);
    FileStatus fs10000 = new FileStatus(0, false, 0, 0, 0, p10000);
    Comparator<FileStatus> comparator = FSTableDescriptors.TABLEINFO_FILESTATUS_COMPARATOR;
    assertTrue(comparator.compare(fs, fs0) > 0);
    assertTrue(comparator.compare(fs0, fs2) > 0);
    assertTrue(comparator.compare(fs2, fs10000) > 0);
  }

  private Path assertWriteAndReadSequenceId(final int i) {
    Path p = new Path("/tmp", FSTableDescriptors.getTableInfoFileName(i));
    int ii = FSTableDescriptors.getTableInfoSequenceId(p);
    assertEquals(i, ii);
    return p;
  }

  @Test
  public void testRemoves() throws IOException {
    final String name = "testRemoves";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
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
    FSTableDescriptors fstd = new FSTableDescriptors(fs, rootdir);
    fstd.createTableDescriptor(htd);
    HTableDescriptor htd2 =
      FSTableDescriptors.getTableDescriptorFromFs(fs, rootdir, htd.getTableName());
    assertTrue(htd.equals(htd2));
  }

  @Test public void testHTableDescriptors()
  throws IOException, InterruptedException {
    final String name = "testHTableDescriptors";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors htds = new FSTableDescriptors(fs, rootdir) {
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
      HTableDescriptor htd = new HTableDescriptor(name + i);
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
  public void testNoSuchTable() throws IOException {
    final String name = "testNoSuchTable";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    assertNull("There shouldn't be any HTD for this table",
      htds.get(TableName.valueOf("NoSuchTable")));
  }

  @Test
  public void testUpdates() throws IOException {
    final String name = "testUpdates";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    htds.add(htd);
    htds.add(htd);
    htds.add(htd);
  }

  @Test
  public void testTableInfoFileStatusComparator() {
    FileStatus bare =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp", FSTableDescriptors.TABLEINFO_FILE_PREFIX));
    FileStatus future =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis()));
    FileStatus farFuture =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis() + 1000));
    FileStatus [] alist = {bare, future, farFuture};
    FileStatus [] blist = {bare, farFuture, future};
    FileStatus [] clist = {farFuture, bare, future};
    Comparator<FileStatus> c = FSTableDescriptors.TABLEINFO_FILESTATUS_COMPARATOR;
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
      // .tmp dir is an invalid table name
      new FSTableDescriptors(fs, FSUtils.getRootDir(UTIL.getConfiguration()))
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
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testdir);
    assertTrue(fstd.createTableDescriptor(htd));
    assertFalse(fstd.createTableDescriptor(htd));
    htd.setValue(Bytes.toBytes("mykey"), Bytes.toBytes("myValue"));
    assertTrue(fstd.createTableDescriptor(htd)); //this will re-create
    Path tableDir = fstd.getTableDir(htd.getTableName());
    Path tmpTableDir = new Path(tableDir, FSTableDescriptors.TMP_DIR);
    FileStatus[] statuses = fs.listStatus(tmpTableDir);
    assertTrue(statuses.length == 0);

    assertEquals(htd, FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir));
  }

}

