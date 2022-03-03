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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link FSTableDescriptors}.
 */
// Do not support to be executed in he same JVM as other tests
@Category({MiscTests.class, MediumTests.class})
public class TestFSTableDescriptors {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSTableDescriptors.class);

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();
  private static final Logger LOG = LoggerFactory.getLogger(TestFSTableDescriptors.class);

  @Rule
  public TestName name = new TestName();

  private Path testDir;

  @Before
  public void setUp() {
    testDir = UTIL.getDataTestDir(name.getMethodName());
  }

  @AfterClass
  public static void tearDownAfterClass() {
    UTIL.cleanupTestDir();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegexAgainstOldStyleTableInfo() {
    Path p = new Path(testDir, FSTableDescriptors.TABLEINFO_FILE_PREFIX);
    int i = FSTableDescriptors.getTableInfoSequenceIdAndFileLength(p).sequenceId;
    assertEquals(0, i);
    // Assert it won't eat garbage -- that it fails
    p = new Path(testDir, "abc");
    FSTableDescriptors.getTableInfoSequenceIdAndFileLength(p);
  }

  @Test
  public void testCreateAndUpdate() throws IOException {
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testDir);
    assertTrue(fstd.createTableDescriptor(htd));
    assertFalse(fstd.createTableDescriptor(htd));
    Path tableInfoDir = new Path(CommonFSUtils.getTableDir(testDir, htd.getTableName()),
      FSTableDescriptors.TABLEINFO_DIR);
    FileStatus[] statuses = fs.listStatus(tableInfoDir);
    assertEquals("statuses.length=" + statuses.length, 1, statuses.length);
    for (int i = 0; i < 10; i++) {
      fstd.update(htd);
    }
    statuses = fs.listStatus(tableInfoDir);
    assertEquals(1, statuses.length);
  }

  @Test
  public void testSequenceIdAdvancesOnTableInfo() throws IOException {
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testDir);
    Path previousPath = null;
    int previousSeqId = -1;
    for (int i = 0; i < 10; i++) {
      Path path = fstd.updateTableDescriptor(htd);
      int seqId =
        FSTableDescriptors.getTableInfoSequenceIdAndFileLength(path).sequenceId;
      if (previousPath != null) {
        // Assert we cleaned up the old file.
        assertTrue(!fs.exists(previousPath));
        assertEquals(previousSeqId + 1, seqId);
      }
      previousPath = path;
      previousSeqId = seqId;
    }
  }

  @Test
  public void testFormatTableInfoSequenceId() {
    Path p0 = assertWriteAndReadSequenceId(0);
    // Assert p0 has format we expect.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < FSTableDescriptors.WIDTH_OF_SEQUENCE_ID; i++) {
      sb.append("0");
    }
    assertEquals(FSTableDescriptors.TABLEINFO_FILE_PREFIX + "." + sb.toString() + ".0",
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
    Path p =
      new Path(testDir, FSTableDescriptors.getTableInfoFileName(i, HConstants.EMPTY_BYTE_ARRAY));
    int ii = FSTableDescriptors.getTableInfoSequenceIdAndFileLength(p).sequenceId;
    assertEquals(i, ii);
    return p;
  }

  @Test
  public void testRemoves() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    TableDescriptors htds = new FSTableDescriptors(fs, testDir);
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    htds.update(htd);
    assertNotNull(htds.remove(htd.getTableName()));
    assertNull(htds.remove(htd.getTableName()));
  }

  @Test
  public void testReadingHTDFromFS() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testDir);
    fstd.createTableDescriptor(htd);
    TableDescriptor td2 =
      FSTableDescriptors.getTableDescriptorFromFs(fs, testDir, htd.getTableName());
    assertTrue(htd.equals(td2));
  }

  @Test
  public void testTableDescriptors() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    FSTableDescriptors htds = new FSTableDescriptors(fs, testDir) {
      @Override
      public TableDescriptor get(TableName tablename) {
        LOG.info(tablename + ", cachehits=" + this.cachehits);
        return super.get(tablename);
      }
    };
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      htds.createTableDescriptor(
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName() + i)).build());
    }

    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name.getMethodName() + i)) != null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name.getMethodName() + i)) != null);
    }
    // Update the table infos
    for (int i = 0; i < count; i++) {
      TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName() + i));
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("" + i));
      htds.update(builder.build());
    }
    // Wait a while so mod time we write is for sure different.
    Thread.sleep(100);
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name.getMethodName() + i)) != null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(TableName.valueOf(name.getMethodName() + i)) != null);
    }
    assertEquals(count * 4, htds.invocations);
    assertTrue("expected=" + (count * 2) + ", actual=" + htds.cachehits,
      htds.cachehits >= (count * 2));
  }

  @Test
  public void testTableDescriptorsNoCache() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    FSTableDescriptors htds = new FSTableDescriptorsTest(fs, testDir, false);
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      htds.createTableDescriptor(
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName() + i)).build());
    }

    for (int i = 0; i < 2 * count; i++) {
      assertNotNull("Expected HTD, got null instead",
        htds.get(TableName.valueOf(name.getMethodName() + i % 2)));
    }
    // Update the table infos
    for (int i = 0; i < count; i++) {
      TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName() + i));
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("" + i));
      htds.update(builder.build());
    }
    for (int i = 0; i < count; i++) {
      assertNotNull("Expected HTD, got null instead",
        htds.get(TableName.valueOf(name.getMethodName() + i)));
      assertTrue("Column Family " + i + " missing", htds
        .get(TableName.valueOf(name.getMethodName() + i)).hasColumnFamily(Bytes.toBytes("" + i)));
    }
    assertEquals(count * 4, htds.invocations);
    assertEquals("expected=0, actual=" + htds.cachehits, 0, htds.cachehits);
  }

  @Test
  public void testGetAll() throws IOException, InterruptedException {
    final String name = "testGetAll";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    FSTableDescriptors htds = new FSTableDescriptorsTest(fs, testDir);
    final int count = 4;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      htds.createTableDescriptor(
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name + i)).build());
    }
    // add hbase:meta
    htds
      .createTableDescriptor(TableDescriptorBuilder.newBuilder(TableName.META_TABLE_NAME).build());
    assertEquals("getAll() didn't return all TableDescriptors, expected: " + (count + 1) +
      " got: " + htds.getAll().size(), count + 1, htds.getAll().size());
  }

  @Test
  public void testGetAllOrdering() throws Exception {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors tds = new FSTableDescriptorsTest(fs, testDir);

    String[] tableNames = new String[] { "foo", "bar", "foo:bar", "bar:foo" };
    for (String tableName : tableNames) {
      tds.createTableDescriptor(
        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).build());
    }

    Map<String, TableDescriptor> tables = tds.getAll();
    // Remove hbase:meta from list. It shows up now since we made it dynamic. The schema
    // is written into the fs by the FSTableDescriptors constructor now where before it
    // didn't.
    tables.remove(TableName.META_TABLE_NAME.getNameAsString());
    assertEquals(4, tables.size());

    String[] tableNamesOrdered =
      new String[] { "bar:foo", "default:bar", "default:foo", "foo:bar" };
    int i = 0;
    for (Map.Entry<String, TableDescriptor> entry : tables.entrySet()) {
      assertEquals(tableNamesOrdered[i], entry.getKey());
      assertEquals(tableNamesOrdered[i],
        entry.getValue().getTableName().getNameWithNamespaceInclAsString());
      i++;
    }
  }

  @Test
  public void testCacheConsistency() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    FSTableDescriptors chtds = new FSTableDescriptorsTest(fs, testDir);
    FSTableDescriptors nonchtds = new FSTableDescriptorsTest(fs, testDir, false);

    final int count = 10;
    // Write out table infos via non-cached FSTableDescriptors
    for (int i = 0; i < count; i++) {
      nonchtds.createTableDescriptor(
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName() + i)).build());
    }

    // Calls to getAll() won't increase the cache counter, do per table.
    for (int i = 0; i < count; i++) {
      assertTrue(chtds.get(TableName.valueOf(name.getMethodName() + i)) != null);
    }

    assertTrue(nonchtds.getAll().size() == chtds.getAll().size());

    // add a new entry for random table name.
    TableName random = TableName.valueOf("random");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(random).build();
    nonchtds.createTableDescriptor(htd);

    // random will only increase the cachehit by 1
    assertEquals(nonchtds.getAll().size(), chtds.getAll().size() + 1);

    for (Map.Entry<String, TableDescriptor> entry : chtds.getAll().entrySet()) {
      String t = (String) entry.getKey();
      TableDescriptor nchtd = entry.getValue();
      assertTrue(
        "expected " + htd.toString() + " got: " + chtds.get(TableName.valueOf(t)).toString(),
        (nchtd.equals(chtds.get(TableName.valueOf(t)))));
    }
    // this is by design, for FSTableDescriptor with cache enabled, once we have done a full scan
    // and load all the table descriptors to cache, we will not go to file system again, as the only
    // way to update table descriptor is to through us so we can cache it when updating.
    assertNotNull(nonchtds.get(random));
    assertNull(chtds.get(random));
  }

  @Test
  public void testNoSuchTable() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    TableDescriptors htds = new FSTableDescriptors(fs, testDir);
    assertNull("There shouldn't be any HTD for this table",
      htds.get(TableName.valueOf("NoSuchTable")));
  }

  @Test
  public void testUpdates() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    TableDescriptors htds = new FSTableDescriptors(fs, testDir);
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    htds.update(htd);
    htds.update(htd);
    htds.update(htd);
  }

  @Test
  public void testTableInfoFileStatusComparator() {
    FileStatus bare = new FileStatus(0, false, 0, 0, -1,
      new Path("/tmp", FSTableDescriptors.TABLEINFO_FILE_PREFIX));
    FileStatus future = new FileStatus(0, false, 0, 0, -1,
      new Path("/tmp/tablinfo." + EnvironmentEdgeManager.currentTime()));
    FileStatus farFuture = new FileStatus(0, false, 0, 0, -1,
      new Path("/tmp/tablinfo." + EnvironmentEdgeManager.currentTime() + 1000));
    FileStatus[] alist = { bare, future, farFuture };
    FileStatus[] blist = { bare, farFuture, future };
    FileStatus[] clist = { farFuture, bare, future };
    Comparator<FileStatus> c = FSTableDescriptors.TABLEINFO_FILESTATUS_COMPARATOR;
    Arrays.sort(alist, c);
    Arrays.sort(blist, c);
    Arrays.sort(clist, c);
    // Now assert all sorted same in way we want.
    for (int i = 0; i < alist.length; i++) {
      assertTrue(alist[i].equals(blist[i]));
      assertTrue(blist[i].equals(clist[i]));
      assertTrue(clist[i].equals(i == 0 ? farFuture : i == 1 ? future : bare));
    }
  }

  @Test
  public void testReadingInvalidDirectoryFromFS() throws IOException {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    try {
      new FSTableDescriptors(fs, CommonFSUtils.getRootDir(UTIL.getConfiguration()))
        .get(TableName.valueOf(HConstants.HBASE_TEMP_DIRECTORY));
      fail("Shouldn't be able to read a table descriptor for the archive directory.");
    } catch (Exception e) {
      LOG.debug("Correctly got error when reading a table descriptor from the archive directory: " +
        e.getMessage());
    }
  }

  @Test
  public void testCreateTableDescriptorUpdatesIfExistsAlready() throws IOException {
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testDir);
    assertTrue(fstd.createTableDescriptor(htd));
    assertFalse(fstd.createTableDescriptor(htd));
    htd = TableDescriptorBuilder.newBuilder(htd)
      .setValue(Bytes.toBytes("mykey"), Bytes.toBytes("myValue")).build();
    assertTrue(fstd.createTableDescriptor(htd)); // this will re-create
    Path tableDir = CommonFSUtils.getTableDir(testDir, htd.getTableName());
    assertEquals(htd, FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir));
  }

  @Test
  public void testIgnoreBrokenTableDescriptorFiles() throws IOException {
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    TableDescriptor newHtd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf2")).build();
    assertNotEquals(newHtd, htd);
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FSTableDescriptors fstd = new FSTableDescriptors(fs, testDir, false, false);
    fstd.update(htd);
    byte[] bytes = TableDescriptorBuilder.toByteArray(newHtd);
    Path tableDir = CommonFSUtils.getTableDir(testDir, htd.getTableName());
    Path tableInfoDir = new Path(tableDir, FSTableDescriptors.TABLEINFO_DIR);
    FileStatus[] statuses = fs.listStatus(tableInfoDir);
    assertEquals(1, statuses.length);
    int seqId =
      FSTableDescriptors.getTableInfoSequenceIdAndFileLength(statuses[0].getPath()).sequenceId + 1;
    Path brokenFile = new Path(tableInfoDir, FSTableDescriptors.getTableInfoFileName(seqId, bytes));
    try (FSDataOutputStream out = fs.create(brokenFile)) {
      out.write(bytes, 0, bytes.length / 2);
    }
    assertTrue(fs.exists(brokenFile));
    TableDescriptor getTd = fstd.get(htd.getTableName());
    assertEquals(htd, getTd);
    assertFalse(fs.exists(brokenFile));
  }

  private static class FSTableDescriptorsTest extends FSTableDescriptors {

    public FSTableDescriptorsTest(FileSystem fs, Path rootdir) {
      this(fs, rootdir, true);
    }

    public FSTableDescriptorsTest(FileSystem fs, Path rootdir, boolean usecache) {
      super(fs, rootdir, false, usecache);
    }

    @Override
    public TableDescriptor get(TableName tablename) {
      LOG.info((super.isUsecache() ? "Cached" : "Non-Cached") +
                 " TableDescriptor.get() on " + tablename + ", cachehits=" + this.cachehits);
      return super.get(tablename);
    }
  }
}

