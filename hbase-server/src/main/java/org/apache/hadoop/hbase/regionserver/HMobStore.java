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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobCacheConfig;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFile;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobStoreEngine;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.MobZookeeper;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.zookeeper.KeeperException;

/**
 * The store implementation to save MOBs (medium objects), it extends the HStore.
 * When a descriptor of a column family has the value "IS_MOB", it means this column family
 * is a mob one. When a HRegion instantiate a store for this column family, the HMobStore is
 * created.
 * HMobStore is almost the same with the HStore except using different types of scanners.
 * In the method of getScanner, the MobStoreScanner and MobReversedStoreScanner are returned.
 * In these scanners, a additional seeks in the mob files should be performed after the seek
 * to HBase is done.
 * The store implements how we save MOBs by extending HStore. When a descriptor
 * of a column family has the value "IS_MOB", it means this column family is a mob one. When a
 * HRegion instantiate a store for this column family, the HMobStore is created. HMobStore is
 * almost the same with the HStore except using different types of scanners. In the method of
 * getScanner, the MobStoreScanner and MobReversedStoreScanner are returned. In these scanners, a
 * additional seeks in the mob files should be performed after the seek in HBase is done.
 */
@InterfaceAudience.Private
public class HMobStore extends HStore {

  private MobCacheConfig mobCacheConfig;
  private Path homePath;
  private Path mobFamilyPath;
  private volatile long mobCompactedIntoMobCellsCount = 0;
  private volatile long mobCompactedFromMobCellsCount = 0;
  private volatile long mobCompactedIntoMobCellsSize = 0;
  private volatile long mobCompactedFromMobCellsSize = 0;
  private volatile long mobFlushCount = 0;
  private volatile long mobFlushedCellsCount = 0;
  private volatile long mobFlushedCellsSize = 0;
  private volatile long mobScanCellsCount = 0;
  private volatile long mobScanCellsSize = 0;
  private List<Path> mobDirLocations;
  private HColumnDescriptor family;

  public HMobStore(final HRegion region, final HColumnDescriptor family,
      final Configuration confParam) throws IOException {
    super(region, family, confParam);
    this.family = family;
    this.mobCacheConfig = (MobCacheConfig) cacheConf;
    this.homePath = MobUtils.getMobHome(conf);
    this.mobFamilyPath = MobUtils.getMobFamilyPath(conf, this.getTableName(),
        family.getNameAsString());
    mobDirLocations = new ArrayList<Path>();
    mobDirLocations.add(mobFamilyPath);
    TableName tn = region.getTableDesc().getTableName();
    mobDirLocations.add(HFileArchiveUtil.getStoreArchivePath(conf, tn, MobUtils
        .getMobRegionInfo(tn).getEncodedName(), family.getNameAsString()));
  }

  /**
   * Creates the mob cache config.
   */
  @Override
  protected void createCacheConf(HColumnDescriptor family) {
    cacheConf = new MobCacheConfig(conf, family);
  }

  /**
   * Gets current config.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Gets the MobStoreScanner or MobReversedStoreScanner. In these scanners, a additional seeks in
   * the mob files should be performed after the seek in HBase is done.
   */
  @Override
  protected KeyValueScanner createScanner(Scan scan, final NavigableSet<byte[]> targetCols,
      long readPt, KeyValueScanner scanner) throws IOException {
    if (scanner == null) {
      if (MobUtils.isRefOnlyScan(scan)) {
        Filter refOnlyFilter = new MobReferenceOnlyFilter();
        Filter filter = scan.getFilter();
        if (filter != null) {
          scan.setFilter(new FilterList(filter, refOnlyFilter));
        } else {
          scan.setFilter(refOnlyFilter);
        }
      }
      scanner = scan.isReversed() ? new ReversedMobStoreScanner(this, getScanInfo(), scan,
          targetCols, readPt) : new MobStoreScanner(this, getScanInfo(), scan, targetCols, readPt);
    }
    return scanner;
  }

  /**
   * Creates the mob store engine.
   */
  @Override
  protected StoreEngine<?, ?, ?, ?> createStoreEngine(Store store, Configuration conf,
      KVComparator kvComparator) throws IOException {
    MobStoreEngine engine = new MobStoreEngine();
    engine.createComponents(conf, store, kvComparator);
    return engine;
  }

  /**
   * Gets the temp directory.
   * @return The temp directory.
   */
  private Path getTempDir() {
    return new Path(homePath, MobConstants.TEMP_DIR_NAME);
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param date The latest date of cells in the flushing.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(Date date, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    if (startKey == null) {
      startKey = HConstants.EMPTY_START_ROW;
    }
    Path path = getTempDir();
    return createWriterInTmp(MobUtils.formatDate(date), path, maxKeyCount, compression, startKey);
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID()
        .toString().replaceAll("-", ""));
    final CacheConfig writerCacheConf = mobCacheConfig;
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withIncludesMvcc(false).withIncludesTags(true)
        .withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
        .withBlockSize(getFamily().getBlocksize())
        .withHBaseCheckSum(true).withDataBlockEncoding(getFamily().getDataBlockEncoding()).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf, region.getFilesystem())
        .withFilePath(new Path(basePath, mobFileName.getFileName()))
        .withComparator(KeyValue.COMPARATOR).withBloomType(BloomType.NONE)
        .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param sourceFile The source file.
   * @param targetPath The directory path where the source file is renamed to.
   * @throws IOException
   */
  public void commitFile(final Path sourceFile, Path targetPath) throws IOException {
    if (sourceFile == null) {
      return;
    }
    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateMobFile(sourceFile);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);
    Path parent = dstPath.getParent();
    if (!region.getFilesystem().exists(parent)) {
      region.getFilesystem().mkdirs(parent);
    }
    if (!region.getFilesystem().rename(sourceFile, dstPath)) {
      throw new IOException("Failed rename of " + sourceFile + " to " + dstPath);
    }
  }

  /**
   * Validates a mob file by opening and closing it.
   *
   * @param path the path to the mob file
   */
  private void validateMobFile(Path path) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile =
          new StoreFile(region.getFilesystem(), path, conf, this.mobCacheConfig, BloomType.NONE);
      storeFile.createReader();
    } catch (IOException e) {
      LOG.error("Fail to open mob file[" + path + "], keep it in temp directory.", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }

  /**
   * Reads the cell from the mob file.
   * @param reference The cell found in the HBase, its value is a path to a mob file.
   * @param cacheBlocks Whether the scanner should cache blocks.
   * @return The cell found in the mob file.
   * @throws IOException
   */
  public Cell resolve(Cell reference, boolean cacheBlocks) throws IOException {
    Cell result = null;
    if (MobUtils.hasValidMobRefCellValue(reference)) {
      String fileName = MobUtils.getMobFileName(reference);
      result = readCell(mobDirLocations, fileName, reference, cacheBlocks);
      if (result == null) {
        result = readClonedCell(fileName, reference, cacheBlocks);
      }
    }
    if (result == null) {
      LOG.warn("The KeyValue result is null, assemble a new KeyValue with the same row,family,"
          + "qualifier,timestamp,type and tags but with an empty value to return.");
      result = new KeyValue(reference.getRowArray(), reference.getRowOffset(),
          reference.getRowLength(), reference.getFamilyArray(), reference.getFamilyOffset(),
          reference.getFamilyLength(), reference.getQualifierArray(),
          reference.getQualifierOffset(), reference.getQualifierLength(), reference.getTimestamp(),
          Type.codeToType(reference.getTypeByte()), HConstants.EMPTY_BYTE_ARRAY,
          0, 0, reference.getTagsArray(), reference.getTagsOffset(),
          reference.getTagsLength());
    }
    return result;
  }

  /**
   * Reads the cell from a mob file.
   * The mob file might be located in different directories.
   * 1. The working directory.
   * 2. The archive directory.
   * Reads the cell from the files located in both of the above directories.
   * @param locations The possible locations where the mob files are saved.
   * @param fileName The file to be read.
   * @param search The cell to be searched.
   * @param cacheMobBlocks Whether the scanner should cache blocks.
   * @return The found cell. Null if there's no such a cell.
   * @throws IOException
   */
  private Cell readCell(List<Path> locations, String fileName, Cell search, boolean cacheMobBlocks)
      throws IOException {
    FileSystem fs = getFileSystem();
    for (Path location : locations) {
      MobFile file = null;
      Path path = new Path(location, fileName);
      try {
        file = mobCacheConfig.getMobFileCache().openFile(fs, path, mobCacheConfig);
        return file.readCell(search, cacheMobBlocks);
      } catch (IOException e) {
        mobCacheConfig.getMobFileCache().evictFile(fileName);
        if (e instanceof FileNotFoundException) {
          LOG.warn("Fail to read the cell, the mob file " + path + " doesn't exist", e);
        } else {
          throw e;
        }
      } finally {
        if (file != null) {
          mobCacheConfig.getMobFileCache().closeFile(file);
        }
      }
    }
    LOG.error("The mob file " + fileName + " could not be found in the locations "
        + mobDirLocations);
    return null;
  }

  /**
   * Reads the cell from a mob file of source table.
   * The table might be cloned, in this case only hfile link is created in the new table,
   * and the mob file is located in the source table directories.
   * 1. The working directory of the source table.
   * 2. The archive directory of the source table.
   * Reads the cell from the files located in both of the above directories.
   * @param fileName The file to be read.
   * @param search The cell to be searched.
   * @param cacheMobBlocks Whether the scanner should cache blocks.
   * @return The found cell. Null if there's no such a cell.
   * @throws IOException
   */
  private Cell readClonedCell(String fileName, Cell search, boolean cacheMobBlocks)
      throws IOException {
    Tag tableNameTag = MobUtils.getTableNameTag(search);
    if (tableNameTag == null) {
      return null;
    }
    byte[] tableName = tableNameTag.getValue();
    if (Bytes.equals(this.getTableName().getName(), tableName)) {
      return null;
    }
    // the possible locations in the source table.
    List<Path> locations = new ArrayList<Path>();
    TableName tn = TableName.valueOf(tableName);
    locations.add(MobUtils.getMobFamilyPath(conf, tn, family.getNameAsString()));
    locations.add(HFileArchiveUtil.getStoreArchivePath(conf, tn, MobUtils.getMobRegionInfo(tn)
        .getEncodedName(), family.getNameAsString()));
    // read the cell from the source table.
    return readCell(locations, fileName, search, cacheMobBlocks);
  }

  /**
   * Gets the mob file path.
   * @return The mob file path.
   */
  public Path getPath() {
    return mobFamilyPath;
  }

  /**
   * The compaction in the store of mob.
   * The cells in this store contains the path of the mob files. There might be race
   * condition between the major compaction and the sweeping in mob files.
   * In order to avoid this, we need mutually exclude the running of the major compaction and
   * sweeping in mob files.
   * The minor compaction is not affected.
   * The major compaction is marked as retainDeleteMarkers when a sweeping is in progress.
   */
  @Override
  public List<StoreFile> compact(CompactionContext compaction) throws IOException {
    // If it's major compaction, try to find whether there's a sweeper is running
    // If yes, mark the major compaction as retainDeleteMarkers
    if (compaction.getRequest().isAllFiles()) {
      // Use the Zookeeper to coordinate.
      // 1. Acquire a operation lock.
      //   1.1. If no, mark the major compaction as retainDeleteMarkers and continue the compaction.
      //   1.2. If the lock is obtained, search the node of sweeping.
      //      1.2.1. If the node is there, the sweeping is in progress, mark the major
      //             compaction as retainDeleteMarkers and continue the compaction.
      //      1.2.2. If the node is not there, add a child to the major compaction node, and
      //             run the compaction directly.
      String compactionName = UUID.randomUUID().toString().replaceAll("-", "");
      MobZookeeper zk = null;
      try {
        zk = MobZookeeper.newInstance(region.getBaseConf(), compactionName);
      } catch (KeeperException e) {
        LOG.error("Cannot connect to the zookeeper, forcing the delete markers to be retained", e);
        compaction.getRequest().forceRetainDeleteMarkers();
        return super.compact(compaction);
      }
      boolean keepDeleteMarkers = true;
      boolean majorCompactNodeAdded = false;
      try {
        // try to acquire the operation lock.
        if (zk.lockColumnFamily(getTableName().getNameAsString(), getFamily().getNameAsString())) {
          try {
            LOG.info("Obtain the lock for the store[" + this
                + "], ready to perform the major compaction");
            // check the sweeping node to find out whether the sweeping is in progress.
            boolean hasSweeper = zk.isSweeperZNodeExist(getTableName().getNameAsString(),
                getFamily().getNameAsString());
            if (!hasSweeper) {
              // if not, add a child to the major compaction node of this store.
              majorCompactNodeAdded = zk.addMajorCompactionZNode(getTableName().getNameAsString(),
                  getFamily().getNameAsString(), compactionName);
              // If we failed to add the major compact node, go with keep delete markers mode.
              keepDeleteMarkers = !majorCompactNodeAdded;
            }
          } catch (Exception e) {
            LOG.error("Fail to handle the Zookeeper", e);
          } finally {
            // release the operation lock
            zk.unlockColumnFamily(getTableName().getNameAsString(), getFamily().getNameAsString());
          }
        }
        try {
          if (keepDeleteMarkers) {
            LOG.warn("Cannot obtain the lock or a sweep tool is running on this store[" + this
                + "], forcing the delete markers to be retained");
            compaction.getRequest().forceRetainDeleteMarkers();
          }
          return super.compact(compaction);
        } finally {
          if (majorCompactNodeAdded) {
            try {
              zk.deleteMajorCompactionZNode(getTableName().getNameAsString(), getFamily()
                  .getNameAsString(), compactionName);
            } catch (KeeperException e) {
              LOG.error("Fail to delete the compaction znode" + compactionName, e);
            }
          }
        }
      } finally {
        zk.close();
      }
    } else {
      // If it's not a major compaction, continue the compaction.
      return super.compact(compaction);
    }
  }

  public void updateMobCompactedIntoMobCellsCount(long count) {
    mobCompactedIntoMobCellsCount += count;
  }

  public long getMobCompactedIntoMobCellsCount() {
    return mobCompactedIntoMobCellsCount;
  }

  public void updateMobCompactedFromMobCellsCount(long count) {
    mobCompactedFromMobCellsCount += count;
  }

  public long getMobCompactedFromMobCellsCount() {
    return mobCompactedFromMobCellsCount;
  }

  public void updateMobCompactedIntoMobCellsSize(long size) {
    mobCompactedIntoMobCellsSize += size;
  }

  public long getMobCompactedIntoMobCellsSize() {
    return mobCompactedIntoMobCellsSize;
  }

  public void updateMobCompactedFromMobCellsSize(long size) {
    mobCompactedFromMobCellsSize += size;
  }

  public long getMobCompactedFromMobCellsSize() {
    return mobCompactedFromMobCellsSize;
  }

  public void updateMobFlushCount() {
    mobFlushCount++;
  }

  public long getMobFlushCount() {
    return mobFlushCount;
  }

  public void updateMobFlushedCellsCount(long count) {
    mobFlushedCellsCount += count;
  }

  public long getMobFlushedCellsCount() {
    return mobFlushedCellsCount;
  }

  public void updateMobFlushedCellsSize(long size) {
    mobFlushedCellsSize += size;
  }

  public long getMobFlushedCellsSize() {
    return mobFlushedCellsSize;
  }

  public void updateMobScanCellsCount(long count) {
    mobScanCellsCount += count;
  }

  public long getMobScanCellsCount() {
    return mobScanCellsCount;
  }

  public void updateMobScanCellsSize(long size) {
    mobScanCellsSize += size;
  }

  public long getMobScanCellsSize() {
    return mobScanCellsSize;
  }
}
