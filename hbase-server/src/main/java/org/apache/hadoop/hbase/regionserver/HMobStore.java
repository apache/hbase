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

import java.io.IOException;
import java.util.Date;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
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
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The store implementation to save MOBs (medium objects), it extends the HStore.
 * When a descriptor of a column family has the value "is_mob", it means this column family
 * is a mob one. When a HRegion instantiate a store for this column family, the HMobStore is
 * created.
 * HMobStore is almost the same with the HStore except using different types of scanners.
 * In the method of getScanner, the MobStoreScanner and MobReversedStoreScanner are returned.
 * In these scanners, a additional seeks in the mob files should be performed after the seek
 * to HBase is done.
 * The store implements how we save MOBs by extending HStore. When a descriptor
 * of a column family has the value "isMob", it means this column family is a mob one. When a
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

  public HMobStore(final HRegion region, final HColumnDescriptor family,
      final Configuration confParam) throws IOException {
    super(region, family, confParam);
    this.mobCacheConfig = (MobCacheConfig) cacheConf;
    this.homePath = MobUtils.getMobHome(conf);
    this.mobFamilyPath = MobUtils.getMobFamilyPath(conf, this.getTableName(),
        family.getNameAsString());
  }

  /**
   * Creates the mob cache config.
   */
  @Override
  protected void createCacheConf(HColumnDescriptor family) {
    cacheConf = new MobCacheConfig(conf, family);
  }

  /**
   * Gets the MobStoreScanner or MobReversedStoreScanner. In these scanners, a additional seeks in
   * the mob files should be performed after the seek in HBase is done.
   */
  @Override
  protected KeyValueScanner createScanner(Scan scan, final NavigableSet<byte[]> targetCols,
      long readPt, KeyValueScanner scanner) throws IOException {
    if (scanner == null) {
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
    if (reference.getValueLength() > Bytes.SIZEOF_INT) {
      String fileName = Bytes.toString(reference.getValueArray(), reference.getValueOffset()
          + Bytes.SIZEOF_INT, reference.getValueLength() - Bytes.SIZEOF_INT);
      Path targetPath = new Path(mobFamilyPath, fileName);
      MobFile file = null;
      try {
        file = mobCacheConfig.getMobFileCache().openFile(region.getFilesystem(), targetPath,
            mobCacheConfig);
        result = file.readCell(reference, cacheBlocks);
      } catch (IOException e) {
        LOG.error("Fail to open/read the mob file " + targetPath.toString(), e);
      } catch (NullPointerException e) {
        // When delete the file during the scan, the hdfs getBlockRange will
        // throw NullPointerException, catch it and manage it.
        LOG.error("Fail to read the mob file " + targetPath.toString()
            + " since it's already deleted", e);
      } finally {
        if (file != null) {
          mobCacheConfig.getMobFileCache().closeFile(file);
        }
      }
    } else {
      LOG.warn("Invalid reference to mob, " + reference.getValueLength() + " bytes is too short");
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
   * Gets the mob file path.
   * @return The mob file path.
   */
  public Path getPath() {
    return mobFamilyPath;
  }
}
