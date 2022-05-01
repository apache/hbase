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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test minor compactions
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestMinorCompaction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMinorCompaction.class);

  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF = UTIL.getConfiguration();

  private HRegion r = null;
  private TableDescriptor htd = null;
  private static int COMPACTION_THRESHOLD;
  private static byte[] FIRST_ROW_BYTES, SECOND_ROW_BYTES, THIRD_ROW_BYTES;
  private static byte[] COL1, COL2;

  public static final class MyCompactionPolicy extends RatioBasedCompactionPolicy {

    public MyCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
      super(conf, storeConfigInfo);
    }

    @Override
    public CompactionRequestImpl selectCompaction(Collection<HStoreFile> candidateFiles,
      List<HStoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak,
      boolean forceMajor) throws IOException {
      return new CompactionRequestImpl(
        candidateFiles.stream().filter(f -> !filesCompacting.contains(f))
          .limit(COMPACTION_THRESHOLD).collect(Collectors.toList()));
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    // Set cache flush size to 1MB
    CONF.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    CONF.setInt(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 100);
    COMPACTION_THRESHOLD = CONF.getInt("hbase.hstore.compactionThreshold", 3);
    CONF.setClass(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, MyCompactionPolicy.class,
      RatioBasedCompactionPolicy.class);

    FIRST_ROW_BYTES = START_KEY_BYTES;
    SECOND_ROW_BYTES = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    SECOND_ROW_BYTES[START_KEY_BYTES.length - 1]++;
    THIRD_ROW_BYTES = START_KEY_BYTES.clone();
    THIRD_ROW_BYTES[START_KEY_BYTES.length - 1] =
      (byte) (THIRD_ROW_BYTES[START_KEY_BYTES.length - 1] + 2);
    COL1 = Bytes.toBytes("column1");
    COL2 = Bytes.toBytes("column2");
  }

  @Before
  public void setUp() throws Exception {
    this.htd = UTIL.createTableDescriptor(name.getMethodName());
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws Exception {
    WAL wal = ((HRegion) r).getWAL();
    ((HRegion) r).close();
    wal.close();
  }

  @Test
  public void testMinorCompactionWithDeleteRow() throws Exception {
    Delete deleteRow = new Delete(SECOND_ROW_BYTES);
    testMinorCompactionWithDelete(deleteRow);
  }

  @Test
  public void testMinorCompactionWithDeleteColumn1() throws Exception {
    Delete dc = new Delete(SECOND_ROW_BYTES);
    /* delete all timestamps in the column */
    dc.addColumns(fam2, COL2);
    testMinorCompactionWithDelete(dc);
  }

  @Test
  public void testMinorCompactionWithDeleteColumn2() throws Exception {
    Delete dc = new Delete(SECOND_ROW_BYTES);
    dc.addColumn(fam2, COL2);
    /*
     * compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3. we only delete the latest
     * version. One might expect to see only versions 1 and 2. HBase differs, and gives us 0, 1 and
     * 2. This is okay as well. Since there was no compaction done before the delete, version 0
     * seems to stay on.
     */
    testMinorCompactionWithDelete(dc, 3);
  }

  @Test
  public void testMinorCompactionWithDeleteColumnFamily() throws Exception {
    Delete deleteCF = new Delete(SECOND_ROW_BYTES);
    deleteCF.addFamily(fam2);
    testMinorCompactionWithDelete(deleteCF);
  }

  @Test
  public void testMinorCompactionWithDeleteVersion1() throws Exception {
    Delete deleteVersion = new Delete(SECOND_ROW_BYTES);
    deleteVersion.addColumns(fam2, COL2, 2);
    /*
     * compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3. We delete versions 0 ...
     * 2. So, we still have one remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 1);
  }

  @Test
  public void testMinorCompactionWithDeleteVersion2() throws Exception {
    Delete deleteVersion = new Delete(SECOND_ROW_BYTES);
    deleteVersion.addColumn(fam2, COL2, 1);
    /*
     * the table has 4 versions: 0, 1, 2, and 3. We delete 1. Should have 3 remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 3);
  }

  /*
   * A helper function to test the minor compaction algorithm. We check that the delete markers are
   * left behind. Takes delete as an argument, which can be any delete (row, column, columnfamliy
   * etc), that essentially deletes row2 and column2. row1 and column1 should be undeleted
   */
  private void testMinorCompactionWithDelete(Delete delete) throws Exception {
    testMinorCompactionWithDelete(delete, 0);
  }

  private void testMinorCompactionWithDelete(Delete delete, int expectedResultsAfterDelete)
    throws Exception {
    Table loader = new RegionAsTable(r);
    for (int i = 0; i < COMPACTION_THRESHOLD + 1; i++) {
      HTestConst.addContent(loader, Bytes.toString(fam1), Bytes.toString(COL1), FIRST_ROW_BYTES,
        THIRD_ROW_BYTES, i);
      HTestConst.addContent(loader, Bytes.toString(fam1), Bytes.toString(COL2), FIRST_ROW_BYTES,
        THIRD_ROW_BYTES, i);
      HTestConst.addContent(loader, Bytes.toString(fam2), Bytes.toString(COL1), FIRST_ROW_BYTES,
        THIRD_ROW_BYTES, i);
      HTestConst.addContent(loader, Bytes.toString(fam2), Bytes.toString(COL2), FIRST_ROW_BYTES,
        THIRD_ROW_BYTES, i);
      r.flush(true);
    }

    Result result = r.get(new Get(FIRST_ROW_BYTES).addColumn(fam1, COL1).readVersions(100));
    assertEquals(COMPACTION_THRESHOLD, result.size());
    result = r.get(new Get(SECOND_ROW_BYTES).addColumn(fam2, COL2).readVersions(100));
    assertEquals(COMPACTION_THRESHOLD, result.size());

    // Now add deletes to memstore and then flush it. That will put us over
    // the compaction threshold of 3 store files. Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    r.delete(delete);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(SECOND_ROW_BYTES).addColumn(fam2, COL2).readVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(FIRST_ROW_BYTES).addColumn(fam1, COL1).readVersions(100));
    assertEquals(COMPACTION_THRESHOLD, result.size());

    r.flush(true);
    // should not change anything.
    // Let us check again

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(SECOND_ROW_BYTES).addColumn(fam2, COL2).readVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(FIRST_ROW_BYTES).addColumn(fam1, COL1).readVersions(100));
    assertEquals(COMPACTION_THRESHOLD, result.size());

    // do a compaction
    HStore store2 = r.getStore(fam2);
    int numFiles1 = store2.getStorefiles().size();
    assertTrue("Was expecting to see 4 store files", numFiles1 > COMPACTION_THRESHOLD); // > 3
    Optional<CompactionContext> compaction = store2.requestCompaction();
    assertTrue(compaction.isPresent());
    store2.compact(compaction.get(), NoLimitThroughputController.INSTANCE, null); // = 3
    int numFiles2 = store2.getStorefiles().size();
    // Check that we did compact
    assertTrue("Number of store files should go down", numFiles1 > numFiles2);
    // Check that it was a minor compaction.
    assertTrue("Was not supposed to be a major compaction", numFiles2 > 1);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(SECOND_ROW_BYTES).addColumn(fam2, COL2).readVersions(100));
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(FIRST_ROW_BYTES).addColumn(fam1, COL1).readVersions(100));
    assertEquals(COMPACTION_THRESHOLD, result.size());
  }
}
