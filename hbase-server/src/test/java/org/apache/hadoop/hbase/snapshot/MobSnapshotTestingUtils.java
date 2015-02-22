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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;

public class MobSnapshotTestingUtils {

  /**
   * Create the Mob Table.
   */
  public static void createMobTable(final HBaseTestingUtility util,
      final TableName tableName, int regionReplication,
      final byte[]... families) throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      hcd.setMobEnabled(true);
      hcd.setMobThreshold(0L);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = SnapshotTestingUtils.getSplitKeys();
    util.getHBaseAdmin().createTable(htd, splitKeys);
    SnapshotTestingUtils.waitForTableToBeOnline(util, tableName);
    assertEquals((splitKeys.length + 1) * regionReplication, util
        .getHBaseAdmin().getTableRegions(tableName).size());
  }

  /**
   * Create a Mob table.
   *
   * @param util
   * @param tableName
   * @param families
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public static HTable createMobTable(final HBaseTestingUtility util,
      final TableName tableName, final byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // Disable blooms (they are on by default as of 0.95) but we disable them
      // here because
      // tests have hard coded counts of what to expect in block cache, etc.,
      // and blooms being
      // on is interfering.
      hcd.setBloomFilterType(BloomType.NONE);
      hcd.setMobEnabled(true);
      hcd.setMobThreshold(0L);
      htd.addFamily(hcd);
    }
    util.getHBaseAdmin().createTable(htd);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait
    // until they are assigned
    util.waitUntilAllRegionsAssigned(htd.getTableName());
    return new HTable(util.getConfiguration(), htd.getTableName());
  }

  /**
   * Return the number of rows in the given table.
   */
  public static int countMobRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count++;
      List<Cell> cells = res.listCells();
      for (Cell cell : cells) {
        // Verify the value
        Assert.assertTrue(CellUtil.cloneValue(cell).length > 0);
      }
    }
    results.close();
    return count;
  }

  /**
   * Return the number of rows in the given table.
   */
  public static int countMobRows(final HTable table, final byte[]... families)
      throws IOException {
    Scan scan = new Scan();
    for (byte[] family : families) {
      scan.addFamily(family);
    }
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count++;
      List<Cell> cells = res.listCells();
      for (Cell cell : cells) {
        // Verify the value
        Assert.assertTrue(CellUtil.cloneValue(cell).length > 0);
      }
    }
    results.close();
    return count;
  }

  public static void verifyMobRowCount(final HBaseTestingUtility util,
      final TableName tableName, long expectedRows) throws IOException {
    HTable table = new HTable(util.getConfiguration(), tableName);
    try {
      assertEquals(expectedRows, countMobRows(table));
    } finally {
      table.close();
    }
  }

  // ==========================================================================
  // Snapshot Mock
  // ==========================================================================
  public static class SnapshotMock {
    private final static String TEST_FAMILY = "cf";
    public final static int TEST_NUM_REGIONS = 4;

    private final Configuration conf;
    private final FileSystem fs;
    private final Path rootDir;

    static class RegionData {
      public HRegionInfo hri;
      public Path tableDir;
      public Path[] files;

      public RegionData(final Path tableDir, final HRegionInfo hri,
          final int nfiles) {
        this.tableDir = tableDir;
        this.hri = hri;
        this.files = new Path[nfiles];
      }
    }

    public static class SnapshotBuilder {
      private final RegionData[] tableRegions;
      private final SnapshotDescription desc;
      private final HTableDescriptor htd;
      private final Configuration conf;
      private final FileSystem fs;
      private final Path rootDir;
      private Path snapshotDir;
      private int snapshotted = 0;

      public SnapshotBuilder(final Configuration conf, final FileSystem fs,
          final Path rootDir, final HTableDescriptor htd,
          final SnapshotDescription desc, final RegionData[] tableRegions)
          throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.rootDir = rootDir;
        this.htd = htd;
        this.desc = desc;
        this.tableRegions = tableRegions;
        this.snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc,
            rootDir);
        new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(
            snapshotDir, new TableDescriptor(htd), false);
      }

      public HTableDescriptor getTableDescriptor() {
        return this.htd;
      }

      public SnapshotDescription getSnapshotDescription() {
        return this.desc;
      }

      public Path getSnapshotsDir() {
        return this.snapshotDir;
      }

      public Path[] addRegion() throws IOException {
        return addRegion(desc);
      }

      public Path[] addRegionV1() throws IOException {
        return addRegion(desc.toBuilder()
            .setVersion(SnapshotManifestV1.DESCRIPTOR_VERSION).build());
      }

      public Path[] addRegionV2() throws IOException {
        return addRegion(desc.toBuilder()
            .setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION).build());
      }

      private Path[] addRegion(final SnapshotDescription desc)
          throws IOException {
        if (this.snapshotted == tableRegions.length) {
          throw new UnsupportedOperationException(
              "No more regions in the table");
        }

        RegionData regionData = tableRegions[this.snapshotted++];
        ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(
            desc.getName());
        SnapshotManifest manifest = SnapshotManifest.create(conf, fs,
            snapshotDir, desc, monitor);
        manifest.addRegion(regionData.tableDir, regionData.hri);
        return regionData.files;
      }

      public Path commit() throws IOException {
        ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(
            desc.getName());
        SnapshotManifest manifest = SnapshotManifest.create(conf, fs,
            snapshotDir, desc, monitor);
        manifest.addTableDescriptor(htd);
        manifest.consolidate();
        SnapshotDescriptionUtils.completeSnapshot(desc, rootDir, snapshotDir,
            fs);
        snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(desc,
            rootDir);
        return snapshotDir;
      }
    }

    public SnapshotMock(final Configuration conf, final FileSystem fs,
        final Path rootDir) {
      this.fs = fs;
      this.conf = conf;
      this.rootDir = rootDir;
    }

    public SnapshotBuilder createSnapshotV1(final String snapshotName)
        throws IOException {
      return createSnapshot(snapshotName, SnapshotManifestV1.DESCRIPTOR_VERSION);
    }

    public SnapshotBuilder createSnapshotV2(final String snapshotName)
        throws IOException {
      return createSnapshot(snapshotName, SnapshotManifestV2.DESCRIPTOR_VERSION);
    }

    private SnapshotBuilder createSnapshot(final String snapshotName,
        final int version) throws IOException {
      HTableDescriptor htd = createHtd(snapshotName);

      RegionData[] regions = createTable(htd, TEST_NUM_REGIONS);

      SnapshotDescription desc = SnapshotDescription.newBuilder()
          .setTable(htd.getNameAsString()).setName(snapshotName)
          .setVersion(version).build();

      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc,
          rootDir);
      SnapshotDescriptionUtils.writeSnapshotInfo(desc, workingDir, fs);
      return new SnapshotBuilder(conf, fs, rootDir, htd, desc, regions);
    }

    public HTableDescriptor createHtd(final String tableName) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
      hcd.setMobEnabled(true);
      hcd.setMobThreshold(0L);
      htd.addFamily(hcd);
      return htd;
    }

    private RegionData[] createTable(final HTableDescriptor htd,
        final int nregions) throws IOException {
      Path tableDir = FSUtils.getTableDir(rootDir, htd.getTableName());
      new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(
          tableDir, new TableDescriptor(htd), false);

      assertTrue(nregions % 2 == 0);
      RegionData[] regions = new RegionData[nregions];
      for (int i = 0; i < regions.length; i += 2) {
        byte[] startKey = Bytes.toBytes(0 + i * 2);
        byte[] endKey = Bytes.toBytes(1 + i * 2);

        // First region, simple with one plain hfile.
        HRegionInfo hri = new HRegionInfo(htd.getTableName(), startKey, endKey);
        HRegionFileSystem rfs = HRegionFileSystem.createRegionOnFileSystem(
            conf, fs, tableDir, hri);
        regions[i] = new RegionData(tableDir, hri, 3);
        for (int j = 0; j < regions[i].files.length; ++j) {
          Path storeFile = createStoreFile(rfs.createTempName());
          regions[i].files[j] = rfs.commitStoreFile(TEST_FAMILY, storeFile);
        }

        // Second region, used to test the split case.
        // This region contains a reference to the hfile in the first region.
        startKey = Bytes.toBytes(2 + i * 2);
        endKey = Bytes.toBytes(3 + i * 2);
        hri = new HRegionInfo(htd.getTableName());
        rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir,
            hri);
        regions[i + 1] = new RegionData(tableDir, hri, regions[i].files.length);
        for (int j = 0; j < regions[i].files.length; ++j) {
          String refName = regions[i].files[j].getName() + '.'
              + regions[i].hri.getEncodedName();
          Path refFile = createStoreFile(new Path(rootDir, refName));
          regions[i + 1].files[j] = rfs.commitStoreFile(TEST_FAMILY, refFile);
        }
      }
      return regions;
    }

    private Path createStoreFile(final Path storeFile) throws IOException {
      FSDataOutputStream out = fs.create(storeFile);
      try {
        out.write(Bytes.toBytes(storeFile.toString()));
      } finally {
        out.close();
      }
      return storeFile;
    }
  }
}
