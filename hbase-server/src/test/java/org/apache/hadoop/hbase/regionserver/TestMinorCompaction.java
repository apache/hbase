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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


/**
 * Test minor compactions
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestMinorCompaction {
  @Rule public TestName name = new TestName();
  static final Log LOG = LogFactory.getLog(TestMinorCompaction.class.getName());
  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();
  protected Configuration conf = UTIL.getConfiguration();
  
  private Region r = null;
  private HTableDescriptor htd = null;
  private int compactionThreshold;
  private byte[] firstRowBytes, secondRowBytes, thirdRowBytes;
  final private byte[] col1, col2;

  /** constructor */
  public TestMinorCompaction() {
    super();

    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    conf.setInt("hbase.hregion.memstore.block.multiplier", 100);
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    firstRowBytes = START_KEY_BYTES;
    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] += 2;
    col1 = Bytes.toBytes("column1");
    col2 = Bytes.toBytes("column2");
  }

  @Before
  public void setUp() throws Exception {
    this.htd = UTIL.createTableDescriptor(name.getMethodName());
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws Exception {
    WAL wal = ((HRegion)r).getWAL();
    ((HRegion)r).close();
    wal.close();
  }

  @Test
  public void testMinorCompactionWithDeleteRow() throws Exception {
    Delete deleteRow = new Delete(secondRowBytes);
    testMinorCompactionWithDelete(deleteRow);
  }

  @Test
  public void testMinorCompactionWithDeleteColumn1() throws Exception {
    Delete dc = new Delete(secondRowBytes);
    /* delete all timestamps in the column */
    dc.deleteColumns(fam2, col2);
    testMinorCompactionWithDelete(dc);
  }

  @Test
  public void testMinorCompactionWithDeleteColumn2() throws Exception {
    Delete dc = new Delete(secondRowBytes);
    dc.deleteColumn(fam2, col2);
    /* compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3.
     * we only delete the latest version. One might expect to see only
     * versions 1 and 2. HBase differs, and gives us 0, 1 and 2.
     * This is okay as well. Since there was no compaction done before the
     * delete, version 0 seems to stay on.
     */
    testMinorCompactionWithDelete(dc, 3);
  }

  @Test
  public void testMinorCompactionWithDeleteColumnFamily() throws Exception {
    Delete deleteCF = new Delete(secondRowBytes);
    deleteCF.deleteFamily(fam2);
    testMinorCompactionWithDelete(deleteCF);
  }

  @Test
  public void testMinorCompactionWithDeleteVersion1() throws Exception {
    Delete deleteVersion = new Delete(secondRowBytes);
    deleteVersion.deleteColumns(fam2, col2, 2);
    /* compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3.
     * We delete versions 0 ... 2. So, we still have one remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 1);
  }

  @Test
  public void testMinorCompactionWithDeleteVersion2() throws Exception {
    Delete deleteVersion = new Delete(secondRowBytes);
    deleteVersion.deleteColumn(fam2, col2, 1);
    /*
     * the table has 4 versions: 0, 1, 2, and 3.
     * We delete 1.
     * Should have 3 remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 3);
  }

  /*
   * A helper function to test the minor compaction algorithm. We check that
   * the delete markers are left behind. Takes delete as an argument, which
   * can be any delete (row, column, columnfamliy etc), that essentially
   * deletes row2 and column2. row1 and column1 should be undeleted
   */
  private void testMinorCompactionWithDelete(Delete delete) throws Exception {
    testMinorCompactionWithDelete(delete, 0);
  }

  private void testMinorCompactionWithDelete(Delete delete, int expectedResultsAfterDelete) throws Exception {
    HRegionIncommon loader = new HRegionIncommon(r);
    for (int i = 0; i < compactionThreshold + 1; i++) {
      HBaseTestCase.addContent(loader, Bytes.toString(fam1), Bytes.toString(col1), firstRowBytes,
        thirdRowBytes, i);
      HBaseTestCase.addContent(loader, Bytes.toString(fam1), Bytes.toString(col2), firstRowBytes,
        thirdRowBytes, i);
      HBaseTestCase.addContent(loader, Bytes.toString(fam2), Bytes.toString(col1), firstRowBytes,
        thirdRowBytes, i);
      HBaseTestCase.addContent(loader, Bytes.toString(fam2), Bytes.toString(col2), firstRowBytes,
        thirdRowBytes, i);
      r.flush(true);
    }

    Result result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());

    // Now add deletes to memstore and then flush it.  That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    r.delete(delete);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());

    r.flush(true);
    // should not change anything.
    // Let us check again

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());

    // do a compaction
    Store store2 = r.getStore(fam2);
    int numFiles1 = store2.getStorefiles().size();
    assertTrue("Was expecting to see 4 store files", numFiles1 > compactionThreshold); // > 3
    ((HStore)store2).compactRecentForTestingAssumingDefaultPolicy(compactionThreshold);   // = 3
    int numFiles2 = store2.getStorefiles().size();
    // Check that we did compact
    assertTrue("Number of store files should go down", numFiles1 > numFiles2);
    // Check that it was a minor compaction.
    assertTrue("Was not supposed to be a major compaction", numFiles2 > 1);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());
  }
}
