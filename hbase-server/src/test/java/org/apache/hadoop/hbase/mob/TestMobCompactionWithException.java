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
package org.apache.hadoop.hbase.mob;

import static org.apache.hadoop.hbase.HBaseTestingUtil.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionAsTable;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestMobCompactionWithException {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobCompactionWithException.class);

  @Rule
  public TestName name = new TestName();
  static final Logger LOG = LoggerFactory.getLogger(TestMobCompactionWithException.class.getName());
  private final static HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static Configuration conf = null;

  private HRegion region = null;
  private TableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor columnFamilyDescriptor;
  private FileSystem fs;

  private static final byte[] COLUMN_FAMILY = fam1;
  private final byte[] STARTROW = Bytes.toBytes(START_KEY);
  private static volatile boolean testException = false;
  private static int rowCount = 100;
  private Table table;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = HTU.getConfiguration();
    conf.set(MobConstants.MOB_COMPACTION_TYPE_KEY, MobConstants.OPTIMIZED_MOB_COMPACTION_TYPE);
    conf.set(MobStoreEngine.MOB_COMPACTOR_CLASS_KEY, MyMobStoreCompactor.class.getName());

  }

  @After
  public void tearDown() throws Exception {
    region.close();
    this.table.close();
    fs.delete(HTU.getDataTestDir(), true);
  }

  private void createTable(long mobThreshold) throws IOException {

    this.columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).setMobEnabled(true)
        .setMobThreshold(mobThreshold).setMaxVersions(1).setBlocksize(500).build();
    this.tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(TestMobUtils.getTableName(name)))
        .setColumnFamily(columnFamilyDescriptor).build();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    region = HBaseTestingUtil.createRegionAndWAL(regionInfo, HTU.getDataTestDir(), conf,
      tableDescriptor, new MobFileCache(conf));
    this.table = new RegionAsTable(region);
    fs = FileSystem.get(conf);
  }

  /**
   * This test is for HBASE-27433.
   */
  @Test
  public void testMobStoreFileDeletedWhenCompactException() throws Exception {
    this.createTable(200);
    byte[] dummyData = makeDummyData(1000); // larger than mob threshold
    for (int i = 0; i < rowCount; i++) {
      Put p = createPut(i, dummyData);
      table.put(p);
      region.flush(true);
    }

    int storeFileCountBeforeCompact = countStoreFiles();
    int mobFileCountBeforeCompact = countMobFiles();
    long mobFileByteSize = getMobFileByteSize();

    List<HStore> stores = region.getStores();
    assertTrue(stores.size() == 1);
    HMobStore mobStore = (HMobStore) stores.get(0);
    Compactor<?> compactor = mobStore.getStoreEngine().getCompactor();
    MyMobStoreCompactor myMobStoreCompactor = (MyMobStoreCompactor) compactor;
    myMobStoreCompactor.setMobFileMaxByteSize(mobFileByteSize + 100);
    testException = true;
    try {
      try {

        // Force major compaction
        mobStore.triggerMajorCompaction();
        Optional<CompactionContext> context = mobStore.requestCompaction(HStore.PRIORITY_USER,
          CompactionLifeCycleTracker.DUMMY, User.getCurrent());
        assertTrue(context.isPresent());
        region.compact(context.get(), mobStore, NoLimitThroughputController.INSTANCE,
          User.getCurrent());

        fail();
      } catch (IOException e) {
        assertTrue(e != null);
      }
    } finally {
      testException = false;
    }

    // When compaction is failed,the count of StoreFile and MobStoreFile should be the same as
    // before compaction.
    assertEquals("After compaction: store files", storeFileCountBeforeCompact, countStoreFiles());
    assertEquals("After compaction: mob file count", mobFileCountBeforeCompact, countMobFiles());
  }

  private int countStoreFiles() throws IOException {
    HStore store = region.getStore(COLUMN_FAMILY);
    return store.getStorefilesCount();
  }

  private int countMobFiles() throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableDescriptor.getTableName(),
      columnFamilyDescriptor.getNameAsString());
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = HTU.getTestFileSystem().listStatus(mobDirPath);
      return files.length;
    }
    return 0;
  }

  private long getMobFileByteSize() throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableDescriptor.getTableName(),
      columnFamilyDescriptor.getNameAsString());
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = HTU.getTestFileSystem().listStatus(mobDirPath);
      if (files.length > 0) {
        return files[0].getLen();
      }
    }
    return 0;
  }

  private Put createPut(int rowIdx, byte[] dummyData) throws IOException {
    Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(rowIdx)));
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(COLUMN_FAMILY, Bytes.toBytes("colX"), dummyData);
    return p;
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }

  public static class MyMobStoreCompactor extends DefaultMobStoreCompactor {
    public MyMobStoreCompactor(Configuration conf, HStore store) {
      super(conf, store);

    }

    public void setMobFileMaxByteSize(long maxByteSize) {
      this.conf.setLong(MobConstants.MOB_COMPACTION_MAX_FILE_SIZE_KEY, maxByteSize);
    }

    @Override
    protected boolean performCompaction(FileDetails fd, final InternalScanner scanner,
      CellSink writer, long smallestReadPoint, boolean cleanSeqId,
      ThroughputController throughputController, CompactionRequestImpl request,
      CompactionProgress progress) throws IOException {

      InternalScanner wrappedScanner = new InternalScanner() {

        private int count = -1;

        @Override
        public boolean next(List<? super ExtendedCell> result, ScannerContext scannerContext)
          throws IOException {
          count++;
          if (count == rowCount - 1 && testException) {
            count = 0;
            throw new IOException("Inject Error");
          }
          return scanner.next(result, scannerContext);
        }

        @Override
        public void close() throws IOException {
          scanner.close();
        }
      };
      return super.performCompaction(fd, wrappedScanner, writer, smallestReadPoint, cleanSeqId,
        throughputController, request, progress);
    }
  }
}
