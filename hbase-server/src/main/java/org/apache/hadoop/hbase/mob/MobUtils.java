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
package org.apache.hadoop.hbase.mob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public final class MobUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MobUtils.class);
  public static final String SEP = "_";

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat("yyyyMMdd");
        }
      };

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private MobUtils() {
  }

  /**
   * Formats a date to a string.
   * @param date The date.
   * @return The string format of the date, it's yyyymmdd.
   */
  public static String formatDate(Date date) {
    return LOCAL_FORMAT.get().format(date);
  }

  /**
   * Parses the string to a date.
   * @param dateString The string format of a date, it's yyyymmdd.
   * @return A date.
   * @throws ParseException
   */
  public static Date parseDate(String dateString) throws ParseException {
    return LOCAL_FORMAT.get().parse(dateString);
  }

  /**
   * Whether the current cell is a mob reference cell.
   * @param cell The current cell.
   * @return True if the cell has a mob reference tag, false if it doesn't.
   */
  public static boolean isMobReferenceCell(Cell cell) {
    if (cell.getTagsLength() > 0) {
      Optional<Tag> tag = PrivateCellUtil.getTag(cell, TagType.MOB_REFERENCE_TAG_TYPE);
      if (tag.isPresent()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the table name tag.
   * @param cell The current cell.
   * @return The table name tag.
   */
  public static Tag getTableNameTag(Cell cell) {
    if (cell.getTagsLength() > 0) {
      Optional<Tag> tag = PrivateCellUtil.getTag(cell, TagType.MOB_TABLE_NAME_TAG_TYPE);
      if (tag.isPresent()) {
        return tag.get();
      }
    }
    return null;
  }

  /**
   * Whether the tag list has a mob reference tag.
   * @param tags The tag list.
   * @return True if the list has a mob reference tag, false if it doesn't.
   */
  public static boolean hasMobReferenceTag(List<Tag> tags) {
    if (!tags.isEmpty()) {
      for (Tag tag : tags) {
        if (tag.getType() == TagType.MOB_REFERENCE_TAG_TYPE) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Indicates whether it's a raw scan. The information is set in the attribute "hbase.mob.scan.raw"
   * of scan. For a mob cell, in a normal scan the scanners retrieves the mob cell from the mob
   * file. In a raw scan, the scanner directly returns cell in HBase without retrieve the one in the
   * mob file.
   * @param scan The current scan.
   * @return True if it's a raw scan.
   */
  public static boolean isRawMobScan(Scan scan) {
    byte[] raw = scan.getAttribute(MobConstants.MOB_SCAN_RAW);
    try {
      return raw != null && Bytes.toBoolean(raw);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Indicates whether it's a reference only scan. The information is set in the attribute
   * "hbase.mob.scan.ref.only" of scan. If it's a ref only scan, only the cells with ref tag are
   * returned.
   * @param scan The current scan.
   * @return True if it's a ref only scan.
   */
  public static boolean isRefOnlyScan(Scan scan) {
    byte[] refOnly = scan.getAttribute(MobConstants.MOB_SCAN_REF_ONLY);
    try {
      return refOnly != null && Bytes.toBoolean(refOnly);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Indicates whether the scan contains the information of caching blocks. The information is set
   * in the attribute "hbase.mob.cache.blocks" of scan.
   * @param scan The current scan.
   * @return True when the Scan attribute specifies to cache the MOB blocks.
   */
  public static boolean isCacheMobBlocks(Scan scan) {
    byte[] cache = scan.getAttribute(MobConstants.MOB_CACHE_BLOCKS);
    try {
      return cache != null && Bytes.toBoolean(cache);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Sets the attribute of caching blocks in the scan.
   * @param scan The current scan.
   * @param cacheBlocks True, set the attribute of caching blocks into the scan, the scanner with
   *          this scan caches blocks. False, the scanner doesn't cache blocks for this scan.
   */
  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  /**
   * Cleans the expired mob files. Cleans the files whose creation date is older than (current -
   * columnFamily.ttl), and the minVersions of that column family is 0.
   * @param fs The current file system.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param columnDescriptor The descriptor of the current column family.
   * @param cacheConfig The cacheConfig that disables the block cache.
   * @param current The current time.
   * @throws IOException
   */
  public static void cleanExpiredMobFiles(FileSystem fs, Configuration conf, TableName tableName,
      ColumnFamilyDescriptor columnDescriptor, CacheConfig cacheConfig, long current)
      throws IOException {
    long timeToLive = columnDescriptor.getTimeToLive();
    if (Integer.MAX_VALUE == timeToLive) {
      // no need to clean, because the TTL is not set.
      return;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(current - timeToLive * 1000);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);

    Date expireDate = calendar.getTime();

    LOG.info("MOB HFiles older than " + expireDate.toGMTString() + " will be deleted!");

    FileStatus[] stats = null;
    Path mobTableDir = FSUtils.getTableDir(getMobHome(conf), tableName);
    Path path = getMobFamilyPath(conf, tableName, columnDescriptor.getNameAsString());
    try {
      stats = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      LOG.warn("Failed to find the mob file " + path, e);
    }
    if (null == stats) {
      // no file found
      return;
    }
    List<HStoreFile> filesToClean = new ArrayList<>();
    int deletedFileCount = 0;
    for (FileStatus file : stats) {
      String fileName = file.getPath().getName();
      try {
        if (HFileLink.isHFileLink(file.getPath())) {
          HFileLink hfileLink = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
          fileName = hfileLink.getOriginPath().getName();
        }

        Date fileDate = parseDate(MobFileName.getDateFromName(fileName));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking file {}", fileName);
        }
        if (fileDate.getTime() < expireDate.getTime()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("{} is an expired file", fileName);
          }
          filesToClean
              .add(new HStoreFile(fs, file.getPath(), conf, cacheConfig, BloomType.NONE, true));
        }
      } catch (Exception e) {
        LOG.error("Cannot parse the fileName " + fileName, e);
      }
    }
    if (!filesToClean.isEmpty()) {
      try {
        removeMobFiles(conf, fs, tableName, mobTableDir, columnDescriptor.getName(), filesToClean);
        deletedFileCount = filesToClean.size();
      } catch (IOException e) {
        LOG.error("Failed to delete the mob files " + filesToClean, e);
      }
    }
    LOG.info("{} expired mob files are deleted", deletedFileCount);
  }

  /**
   * Gets the root dir of the mob files. It's {HBASE_DIR}/mobdir.
   * @param conf The current configuration.
   * @return the root dir of the mob file.
   */
  public static Path getMobHome(Configuration conf) {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    return getMobHome(hbaseDir);
  }

  /**
   * Gets the root dir of the mob files under the qualified HBase root dir. It's {rootDir}/mobdir.
   * @param rootDir The qualified path of HBase root directory.
   * @return The root dir of the mob file.
   */
  public static Path getMobHome(Path rootDir) {
    return new Path(rootDir, MobConstants.MOB_DIR_NAME);
  }

  /**
   * Gets the qualified root dir of the mob files.
   * @param conf The current configuration.
   * @return The qualified root dir.
   * @throws IOException
   */
  public static Path getQualifiedMobRootDir(Configuration conf) throws IOException {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    Path mobRootDir = new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
    FileSystem fs = mobRootDir.getFileSystem(conf);
    return mobRootDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * Gets the table dir of the mob files under the qualified HBase root dir. It's
   * {rootDir}/mobdir/data/${namespace}/${tableName}
   * @param rootDir The qualified path of HBase root directory.
   * @param tableName The name of table.
   * @return The table dir of the mob file.
   */
  public static Path getMobTableDir(Path rootDir, TableName tableName) {
    return FSUtils.getTableDir(getMobHome(rootDir), tableName);
  }

  /**
   * Gets the region dir of the mob files. It's
   * {HBASE_DIR}/mobdir/data/{namespace}/{tableName}/{regionEncodedName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    return getMobRegionPath(new Path(conf.get(HConstants.HBASE_DIR)), tableName);
  }

  /**
   * Gets the region dir of the mob files under the specified root dir. It's
   * {rootDir}/mobdir/data/{namespace}/{tableName}/{regionEncodedName}.
   * @param rootDir The qualified path of HBase root directory.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Path rootDir, TableName tableName) {
    Path tablePath = FSUtils.getTableDir(getMobHome(rootDir), tableName);
    RegionInfo regionInfo = getMobRegionInfo(tableName);
    return new Path(tablePath, regionInfo.getEncodedName());
  }

  /**
   * Gets the family dir of the mob files. It's
   * {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    return new Path(getMobRegionPath(conf, tableName), familyName);
  }

  /**
   * Gets the family dir of the mob files. It's
   * {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param regionPath The path of mob region which is a dummy one.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Path regionPath, String familyName) {
    return new Path(regionPath, familyName);
  }

  /**
   * Gets the RegionInfo of the mob files. This is a dummy region. The mob files are not saved in a
   * region in HBase. This is only used in mob snapshot. It's internally used only.
   * @param tableName
   * @return A dummy mob region info.
   */
  public static RegionInfo getMobRegionInfo(TableName tableName) {
    return RegionInfoBuilder.newBuilder(tableName).setStartKey(MobConstants.MOB_REGION_NAME_BYTES)
        .setEndKey(HConstants.EMPTY_END_ROW).setSplit(false).setRegionId(0).build();
  }

  /**
   * Gets whether the current RegionInfo is a mob one.
   * @param regionInfo The current RegionInfo.
   * @return If true, the current RegionInfo is a mob one.
   */
  public static boolean isMobRegionInfo(RegionInfo regionInfo) {
    return regionInfo == null ? false
        : getMobRegionInfo(regionInfo.getTable()).getEncodedName()
            .equals(regionInfo.getEncodedName());
  }

  /**
   * Gets whether the current region name follows the pattern of a mob region name.
   * @param tableName The current table name.
   * @param regionName The current region name.
   * @return True if the current region name follows the pattern of a mob region name.
   */
  public static boolean isMobRegionName(TableName tableName, byte[] regionName) {
    return Bytes.equals(regionName, getMobRegionInfo(tableName).getRegionName());
  }

  /**
   * Archives the mob files.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param tableName The table name.
   * @param tableDir The table directory.
   * @param family The name of the column family.
   * @param storeFiles The files to be deleted.
   * @throws IOException
   */
  public static void removeMobFiles(Configuration conf, FileSystem fs, TableName tableName,
      Path tableDir, byte[] family, Collection<HStoreFile> storeFiles) throws IOException {
    HFileArchiver.archiveStoreFiles(conf, fs, getMobRegionInfo(tableName), tableDir, family,
      storeFiles);
  }



  /**
   * Creates a mob reference KeyValue. The value of the mob reference KeyValue is mobCellValueSize +
   * mobFileName.
   * @param cell The original Cell.
   * @param fileName The mob file name where the mob reference KeyValue is written.
   * @param tableNameTag The tag of the current table name. It's very important in cloning the
   *          snapshot.
   * @return The mob reference KeyValue.
   */
  public static Cell createMobRefCell(Cell cell, byte[] fileName, Tag tableNameTag) {
    // Append the tags to the KeyValue.
    // The key is same, the value is the filename of the mob file
    List<Tag> tags = new ArrayList<>();
    // Add the ref tag as the 1st one.
    tags.add(MobConstants.MOB_REF_TAG);
    // Add the tag of the source table name, this table is where this mob file is flushed
    // from.
    // It's very useful in cloning the snapshot. When reading from the cloning table, we need to
    // find the original mob files by this table name. For details please see cloning
    // snapshot for mob files.
    tags.add(tableNameTag);
    return createMobRefCell(cell, fileName, TagUtil.fromList(tags));
  }

  public static Cell createMobRefCell(Cell cell, byte[] fileName, byte[] refCellTags) {
    byte[] refValue = Bytes.add(Bytes.toBytes(cell.getValueLength()), fileName);
    return PrivateCellUtil.createCell(cell, refValue, TagUtil.concatTags(refCellTags, cell));
  }

  /**
   * Creates a writer for the mob file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The hex string of the start key.
   * @param cacheConfig The current cache config.
   * @param cryptoContext The encryption context.
   * @param isCompaction If the writer is used in compaction.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFileWriter createWriter(Configuration conf, FileSystem fs,
      ColumnFamilyDescriptor family, String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, String startKey, CacheConfig cacheConfig,
      Encryption.Context cryptoContext, boolean isCompaction, String regionName)
      throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date,
      UUID.randomUUID().toString().replaceAll("-", ""), regionName);
    return createWriter(conf, fs, family, mobFileName, basePath, maxKeyCount, compression,
      cacheConfig, cryptoContext, isCompaction);
  }

  /**
   * Creates a writer for the mob file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param mobFileName The mob file name.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param cacheConfig The current cache config.
   * @param cryptoContext The encryption context.
   * @param isCompaction If the writer is used in compaction.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFileWriter createWriter(Configuration conf, FileSystem fs,
      ColumnFamilyDescriptor family, MobFileName mobFileName, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, CacheConfig cacheConfig, Encryption.Context cryptoContext,
      boolean isCompaction) throws IOException {
    return createWriter(conf, fs, family, new Path(basePath, mobFileName.getFileName()),
      maxKeyCount, compression, cacheConfig, cryptoContext, HStore.getChecksumType(conf),
      HStore.getBytesPerChecksum(conf), family.getBlocksize(), BloomType.NONE, isCompaction);
  }

  /**
   * Creates a writer for the mob file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param path The path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param cacheConfig The current cache config.
   * @param cryptoContext The encryption context.
   * @param checksumType The checksum type.
   * @param bytesPerChecksum The bytes per checksum.
   * @param blocksize The HFile block size.
   * @param bloomType The bloom filter type.
   * @param isCompaction If the writer is used in compaction.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFileWriter createWriter(Configuration conf, FileSystem fs,
      ColumnFamilyDescriptor family, Path path, long maxKeyCount, Compression.Algorithm compression,
      CacheConfig cacheConfig, Encryption.Context cryptoContext, ChecksumType checksumType,
      int bytesPerChecksum, int blocksize, BloomType bloomType, boolean isCompaction)
      throws IOException {
    if (compression == null) {
      compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    final CacheConfig writerCacheConf;
    if (isCompaction) {
      writerCacheConf = new CacheConfig(cacheConfig);
      writerCacheConf.setCacheDataOnWrite(false);
    } else {
      writerCacheConf = cacheConfig;
    }
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withIncludesMvcc(true).withIncludesTags(true).withCompressTags(family.isCompressTags())
        .withChecksumType(checksumType).withBytesPerCheckSum(bytesPerChecksum)
        .withBlockSize(blocksize).withHBaseCheckSum(true)
        .withDataBlockEncoding(family.getDataBlockEncoding()).withEncryptionContext(cryptoContext)
        .withCreateTime(EnvironmentEdgeManager.currentTime()).build();

    StoreFileWriter w = new StoreFileWriter.Builder(conf, writerCacheConf, fs).withFilePath(path)
        .withComparator(CellComparator.getInstance()).withBloomType(bloomType)
        .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param sourceFile The path where the mob file is saved.
   * @param targetPath The directory path where the source file is renamed to.
   * @param cacheConfig The current cache config.
   * @return The target file path the source file is renamed to.
   * @throws IOException
   */
  public static Path commitFile(Configuration conf, FileSystem fs, final Path sourceFile,
      Path targetPath, CacheConfig cacheConfig) throws IOException {
    if (sourceFile == null) {
      return null;
    }
    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateMobFile(conf, fs, sourceFile, cacheConfig, true);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);
    Path parent = dstPath.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }
    if (!fs.rename(sourceFile, dstPath)) {
      throw new IOException("Failed rename of " + sourceFile + " to " + dstPath);
    }
    return dstPath;
  }

  /**
   * Validates a mob file by opening and closing it.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param path The path where the mob file is saved.
   * @param cacheConfig The current cache config.
   */
  private static void validateMobFile(Configuration conf, FileSystem fs, Path path,
      CacheConfig cacheConfig, boolean primaryReplica) throws IOException {
    HStoreFile storeFile = null;
    try {
      storeFile = new HStoreFile(fs, path, conf, cacheConfig, BloomType.NONE, primaryReplica);
      storeFile.initReader();
    } catch (IOException e) {
      LOG.error("Failed to open mob file[" + path + "], keep it in temp directory.", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeStoreFile(false);
      }
    }
  }

  /**
   * Indicates whether the current mob ref cell has a valid value. A mob ref cell has a mob
   * reference tag. The value of a mob ref cell consists of two parts, real mob value length and mob
   * file name. The real mob value length takes 4 bytes. The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return True if the cell has a valid value.
   */
  public static boolean hasValidMobRefCellValue(Cell cell) {
    return cell.getValueLength() > Bytes.SIZEOF_INT;
  }

  /**
   * Gets the mob value length from the mob ref cell. A mob ref cell has a mob reference tag. The
   * value of a mob ref cell consists of two parts, real mob value length and mob file name. The
   * real mob value length takes 4 bytes. The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The real mob value length.
   */
  public static int getMobValueLength(Cell cell) {
    return PrivateCellUtil.getValueAsInt(cell);
  }

  /**
   * Gets the mob file name from the mob ref cell. A mob ref cell has a mob reference tag. The value
   * of a mob ref cell consists of two parts, real mob value length and mob file name. The real mob
   * value length takes 4 bytes. The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The mob file name.
   */
  public static String getMobFileName(Cell cell) {
    return Bytes.toString(cell.getValueArray(), cell.getValueOffset() + Bytes.SIZEOF_INT,
      cell.getValueLength() - Bytes.SIZEOF_INT);
  }

  /**
   * Gets the table name used in the table lock. The table lock name is a dummy one, it's not a
   * table name. It's tableName + ".mobLock".
   * @param tn The table name.
   * @return The table name used in table lock.
   */
  public static TableName getTableLockName(TableName tn) {
    byte[] tableName = tn.getName();
    return TableName.valueOf(Bytes.add(tableName, MobConstants.MOB_TABLE_LOCK_SUFFIX));
  }

  /**
   * Checks whether this table has mob-enabled columns.
   * @param htd The current table descriptor.
   * @return Whether this table has mob-enabled columns.
   */
  public static boolean hasMobColumns(TableDescriptor htd) {
    ColumnFamilyDescriptor[] hcds = htd.getColumnFamilies();
    for (ColumnFamilyDescriptor hcd : hcds) {
      if (hcd.isMobEnabled()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get list of Mob column families (if any exists)
   * @param htd table descriptor
   * @return list of Mob column families
   */
  public static List<ColumnFamilyDescriptor> getMobColumnFamilies(TableDescriptor htd) {

    List<ColumnFamilyDescriptor> fams = new ArrayList<ColumnFamilyDescriptor>();
    ColumnFamilyDescriptor[] hcds = htd.getColumnFamilies();
    for (ColumnFamilyDescriptor hcd : hcds) {
      if (hcd.isMobEnabled()) {
        fams.add(hcd);
      }
    }
    return fams;
  }

  /**
   * Indicates whether return null value when the mob file is missing or corrupt.
   * The information is set in the attribute "empty.value.on.mobcell.miss" of scan.
   * @param scan The current scan.
   * @return True if the readEmptyValueOnMobCellMiss is enabled.
   */
  public static boolean isReadEmptyValueOnMobCellMiss(Scan scan) {
    byte[] readEmptyValueOnMobCellMiss =
        scan.getAttribute(MobConstants.EMPTY_VALUE_ON_MOBCELL_MISS);
    try {
      return readEmptyValueOnMobCellMiss != null && Bytes.toBoolean(readEmptyValueOnMobCellMiss);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Checks if the mob file is expired.
   * @param column The descriptor of the current column family.
   * @param current The current time.
   * @param fileDate The date string parsed from the mob file name.
   * @return True if the mob file is expired.
   */
  public static boolean isMobFileExpired(ColumnFamilyDescriptor column, long current,
      String fileDate) {
    if (column.getMinVersions() > 0) {
      return false;
    }
    long timeToLive = column.getTimeToLive();
    if (Integer.MAX_VALUE == timeToLive) {
      return false;
    }

    Date expireDate = new Date(current - timeToLive * 1000);
    expireDate = new Date(expireDate.getYear(), expireDate.getMonth(), expireDate.getDate());
    try {
      Date date = parseDate(fileDate);
      if (date.getTime() < expireDate.getTime()) {
        return true;
      }
    } catch (ParseException e) {
      LOG.warn("Failed to parse the date " + fileDate, e);
      return false;
    }
    return false;
  }
  
  /**
   * Gets encoded region name from a MOB file name
   * @param mobFileName MOB file name
   * @return encoded region name or null
   */
  public static String getEncodedRegionName(String mobFileName) {
    int index = mobFileName.lastIndexOf(MobFileName.REGION_SEP);
    if (index < 0) {
      return null;
    }
    return mobFileName.substring(index+1);
  }

  /**
   * Get list of referenced MOB files from a given collection
   * of store files
   * @param storeFiles store files
   * @param mobDir MOB file directory
   * @return list of MOB file paths
   */
  
  public static List<Path> getReferencedMobFiles(Collection<HStoreFile> storeFiles, Path mobDir) {

    Set<String> mobSet = new HashSet<String>();
    for (HStoreFile sf : storeFiles) {
      byte[] value = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
      if (value != null && value.length > 1) {
        String s = Bytes.toString(value);
        String[] all = s.split(",");
        Collections.addAll(mobSet, all);
      }
    }
    List<Path> retList = new ArrayList<Path>();
    for (String name : mobSet) {
      retList.add(new Path(mobDir, name));
    }
    return retList;
  }
}