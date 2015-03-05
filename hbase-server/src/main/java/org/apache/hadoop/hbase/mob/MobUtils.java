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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactor;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionedMobFileCompactor;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public class MobUtils {

  private static final Log LOG = LogFactory.getLog(MobUtils.class);

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };

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
      Tag tag = Tag.getTag(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(),
          TagType.MOB_REFERENCE_TAG_TYPE);
      return tag != null;
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
      Tag tag = Tag.getTag(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(),
          TagType.MOB_TABLE_NAME_TAG_TYPE);
      return tag;
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
   * Indicates whether it's a raw scan.
   * The information is set in the attribute "hbase.mob.scan.raw" of scan.
   * For a mob cell, in a normal scan the scanners retrieves the mob cell from the mob file.
   * In a raw scan, the scanner directly returns cell in HBase without retrieve the one in
   * the mob file.
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
   * Indicates whether it's a reference only scan.
   * The information is set in the attribute "hbase.mob.scan.ref.only" of scan.
   * If it's a ref only scan, only the cells with ref tag are returned.
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
   * Indicates whether the scan contains the information of caching blocks.
   * The information is set in the attribute "hbase.mob.cache.blocks" of scan.
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
   *
   * @param scan
   *          The current scan.
   * @param cacheBlocks
   *          True, set the attribute of caching blocks into the scan, the scanner with this scan
   *          caches blocks.
   *          False, the scanner doesn't cache blocks for this scan.
   */
  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  /**
   * Cleans the expired mob files.
   * Cleans the files whose creation date is older than (current - columnFamily.ttl), and
   * the minVersions of that column family is 0.
   * @param fs The current file system.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param columnDescriptor The descriptor of the current column family.
   * @param cacheConfig The cacheConfig that disables the block cache.
   * @param current The current time.
   * @throws IOException
   */
  public static void cleanExpiredMobFiles(FileSystem fs, Configuration conf, TableName tableName,
      HColumnDescriptor columnDescriptor, CacheConfig cacheConfig, long current)
      throws IOException {
    long timeToLive = columnDescriptor.getTimeToLive();
    if (Integer.MAX_VALUE == timeToLive) {
      // no need to clean, because the TTL is not set.
      return;
    }

    Date expireDate = new Date(current - timeToLive * 1000);
    expireDate = new Date(expireDate.getYear(), expireDate.getMonth(), expireDate.getDate());
    LOG.info("MOB HFiles older than " + expireDate.toGMTString() + " will be deleted!");

    FileStatus[] stats = null;
    Path mobTableDir = FSUtils.getTableDir(getMobHome(conf), tableName);
    Path path = getMobFamilyPath(conf, tableName, columnDescriptor.getNameAsString());
    try {
      stats = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      LOG.warn("Fail to find the mob file " + path, e);
    }
    if (null == stats) {
      // no file found
      return;
    }
    List<StoreFile> filesToClean = new ArrayList<StoreFile>();
    int deletedFileCount = 0;
    for (FileStatus file : stats) {
      String fileName = file.getPath().getName();
      try {
        MobFileName mobFileName = null;
        if (!HFileLink.isHFileLink(file.getPath())) {
          mobFileName = MobFileName.create(fileName);
        } else {
          HFileLink hfileLink = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
          mobFileName = MobFileName.create(hfileLink.getOriginPath().getName());
        }
        Date fileDate = parseDate(mobFileName.getDate());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking file " + fileName);
        }
        if (fileDate.getTime() < expireDate.getTime()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(fileName + " is an expired file");
          }
          filesToClean.add(new StoreFile(fs, file.getPath(), conf, cacheConfig, BloomType.NONE));
        }
      } catch (Exception e) {
        LOG.error("Cannot parse the fileName " + fileName, e);
      }
    }
    if (!filesToClean.isEmpty()) {
      try {
        removeMobFiles(conf, fs, tableName, mobTableDir, columnDescriptor.getName(),
            filesToClean);
        deletedFileCount = filesToClean.size();
      } catch (IOException e) {
        LOG.error("Fail to delete the mob files " + filesToClean, e);
      }
    }
    LOG.info(deletedFileCount + " expired mob files are deleted");
  }

  /**
   * Gets the root dir of the mob files.
   * It's {HBASE_DIR}/mobdir.
   * @param conf The current configuration.
   * @return the root dir of the mob file.
   */
  public static Path getMobHome(Configuration conf) {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    return new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
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
    return mobRootDir.makeQualified(fs);
  }

  /**
   * Gets the region dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    Path tablePath = FSUtils.getTableDir(getMobHome(conf), tableName);
    HRegionInfo regionInfo = getMobRegionInfo(tableName);
    return new Path(tablePath, regionInfo.getEncodedName());
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    return new Path(getMobRegionPath(conf, tableName), familyName);
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param regionPath The path of mob region which is a dummy one.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Path regionPath, String familyName) {
    return new Path(regionPath, familyName);
  }

  /**
   * Gets the HRegionInfo of the mob files.
   * This is a dummy region. The mob files are not saved in a region in HBase.
   * This is only used in mob snapshot. It's internally used only.
   * @param tableName
   * @return A dummy mob region info.
   */
  public static HRegionInfo getMobRegionInfo(TableName tableName) {
    HRegionInfo info = new HRegionInfo(tableName, MobConstants.MOB_REGION_NAME_BYTES,
        HConstants.EMPTY_END_ROW, false, 0);
    return info;
  }

  /**
   * Gets whether the current HRegionInfo is a mob one.
   * @param regionInfo The current HRegionInfo.
   * @return If true, the current HRegionInfo is a mob one.
   */
  public static boolean isMobRegionInfo(HRegionInfo regionInfo) {
    return regionInfo == null ? false : getMobRegionInfo(regionInfo.getTable()).getEncodedName()
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
   * Gets the working directory of the mob compaction.
   * @param root The root directory of the mob compaction.
   * @param jobName The current job name.
   * @return The directory of the mob compaction for the current job.
   */
  public static Path getCompactionWorkingPath(Path root, String jobName) {
    return new Path(root, jobName);
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
      Path tableDir, byte[] family, Collection<StoreFile> storeFiles) throws IOException {
    HFileArchiver.archiveStoreFiles(conf, fs, getMobRegionInfo(tableName), tableDir, family,
        storeFiles);
  }

  /**
   * Creates a mob reference KeyValue.
   * The value of the mob reference KeyValue is mobCellValueSize + mobFileName.
   * @param cell The original Cell.
   * @param fileName The mob file name where the mob reference KeyValue is written.
   * @param tableNameTag The tag of the current table name. It's very important in
   *                        cloning the snapshot.
   * @return The mob reference KeyValue.
   */
  public static KeyValue createMobRefKeyValue(Cell cell, byte[] fileName, Tag tableNameTag) {
    // Append the tags to the KeyValue.
    // The key is same, the value is the filename of the mob file
    List<Tag> tags = new ArrayList<Tag>();
    // Add the ref tag as the 1st one.
    tags.add(MobConstants.MOB_REF_TAG);
    // Add the tag of the source table name, this table is where this mob file is flushed
    // from.
    // It's very useful in cloning the snapshot. When reading from the cloning table, we need to
    // find the original mob files by this table name. For details please see cloning
    // snapshot for mob files.
    tags.add(tableNameTag);
    // Add the existing tags.
    tags.addAll(Tag.asList(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength()));
    int valueLength = cell.getValueLength();
    byte[] refValue = Bytes.add(Bytes.toBytes(valueLength), fileName);
    KeyValue reference = new KeyValue(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
        cell.getTimestamp(), KeyValue.Type.Put, refValue, 0, refValue.length, tags);
    reference.setSequenceId(cell.getSequenceId());
    return reference;
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
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(Configuration conf, FileSystem fs,
      HColumnDescriptor family, String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, String startKey, CacheConfig cacheConfig)
      throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID().toString()
        .replaceAll("-", ""));
    return createWriter(conf, fs, family, mobFileName, basePath, maxKeyCount, compression,
      cacheConfig);
  }

  /**
   * Creates a writer for the ref file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param cacheConfig The current cache config.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFile.Writer createRefFileWriter(Configuration conf, FileSystem fs,
    HColumnDescriptor family, Path basePath, long maxKeyCount, CacheConfig cacheConfig)
    throws IOException {
    HFileContext hFileContext = new HFileContextBuilder().withIncludesMvcc(true)
      .withIncludesTags(true).withCompression(family.getCompactionCompression())
      .withCompressTags(family.shouldCompressTags()).withChecksumType(HStore.getChecksumType(conf))
      .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf)).withBlockSize(family.getBlocksize())
      .withHBaseCheckSum(true).withDataBlockEncoding(family.getDataBlockEncoding()).build();
    Path tempPath = new Path(basePath, UUID.randomUUID().toString().replaceAll("-", ""));
    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, cacheConfig, fs).withFilePath(tempPath)
      .withComparator(KeyValue.COMPARATOR).withBloomType(family.getBloomFilterType())
      .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
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
   * @param startKey The start key.
   * @param cacheConfig The current cache config.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFile.Writer createWriter(Configuration conf, FileSystem fs,
      HColumnDescriptor family, String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey, CacheConfig cacheConfig)
      throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID().toString()
        .replaceAll("-", ""));
    return createWriter(conf, fs, family, mobFileName, basePath, maxKeyCount, compression,
      cacheConfig);
  }

  /**
   * Creates a writer for the del file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @param cacheConfig The current cache config.
   * @return The writer for the del file.
   * @throws IOException
   */
  public static StoreFile.Writer createDelFileWriter(Configuration conf, FileSystem fs,
      HColumnDescriptor family, String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey, CacheConfig cacheConfig)
      throws IOException {
    String suffix = UUID
      .randomUUID().toString().replaceAll("-", "") + "_del";
    MobFileName mobFileName = MobFileName.create(startKey, date, suffix);
    return createWriter(conf, fs, family, mobFileName, basePath, maxKeyCount, compression,
      cacheConfig);
  }

  /**
   * Creates a writer for the del file in temp directory.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param mobFileName The mob file name.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param cacheConfig The current cache config.
   * @return The writer for the mob file.
   * @throws IOException
   */
  private static StoreFile.Writer createWriter(Configuration conf, FileSystem fs,
    HColumnDescriptor family, MobFileName mobFileName, Path basePath, long maxKeyCount,
    Compression.Algorithm compression, CacheConfig cacheConfig) throws IOException {
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
      .withIncludesMvcc(false).withIncludesTags(true).withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
      .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM).withBlockSize(family.getBlocksize())
      .withHBaseCheckSum(true).withDataBlockEncoding(family.getDataBlockEncoding()).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, cacheConfig, fs)
      .withFilePath(new Path(basePath, mobFileName.getFileName()))
      .withComparator(KeyValue.COMPARATOR).withBloomType(BloomType.NONE)
      .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param @param conf The current configuration.
   * @param fs The current file system.
   * @param path The path where the mob file is saved.
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
    validateMobFile(conf, fs, sourceFile, cacheConfig);
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
      CacheConfig cacheConfig) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(fs, path, conf, cacheConfig, BloomType.NONE);
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
   * Indicates whether the current mob ref cell has a valid value.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return True if the cell has a valid value.
   */
  public static boolean hasValidMobRefCellValue(Cell cell) {
    return cell.getValueLength() > Bytes.SIZEOF_INT;
  }

  /**
   * Gets the mob value length from the mob ref cell.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The real mob value length.
   */
  public static int getMobValueLength(Cell cell) {
    return Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), Bytes.SIZEOF_INT);
  }

  /**
   * Gets the mob file name from the mob ref cell.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The mob file name.
   */
  public static String getMobFileName(Cell cell) {
    return Bytes.toString(cell.getValueArray(), cell.getValueOffset() + Bytes.SIZEOF_INT,
        cell.getValueLength() - Bytes.SIZEOF_INT);
  }

  /**
   * Gets the table name used in the table lock.
   * The table lock name is a dummy one, it's not a table name. It's tableName + ".mobLock".
   * @param tn The table name.
   * @return The table name used in table lock.
   */
  public static TableName getTableLockName(TableName tn) {
    byte[] tableName = tn.getName();
    return TableName.valueOf(Bytes.add(tableName, MobConstants.MOB_TABLE_LOCK_SUFFIX));
  }

  /**
   * Performs the mob file compaction.
   * @param conf the Configuration
   * @param fs the file system
   * @param tableName the table the compact
   * @param hcd the column descriptor
   * @param pool the thread pool
   * @param tableLockManager the tableLock manager
   * @param isForceAllFiles Whether add all mob files into the compaction.
   */
  public static void doMobFileCompaction(Configuration conf, FileSystem fs, TableName tableName,
    HColumnDescriptor hcd, ExecutorService pool, TableLockManager tableLockManager,
    boolean isForceAllFiles) throws IOException {
    String className = conf.get(MobConstants.MOB_FILE_COMPACTOR_CLASS_KEY,
      PartitionedMobFileCompactor.class.getName());
    // instantiate the mob file compactor.
    MobFileCompactor compactor = null;
    try {
      compactor = ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {
        Configuration.class, FileSystem.class, TableName.class, HColumnDescriptor.class,
        ExecutorService.class }, new Object[] { conf, fs, tableName, hcd, pool });
    } catch (Exception e) {
      throw new IOException("Unable to load configured mob file compactor '" + className + "'", e);
    }
    // compact only for mob-enabled column.
    // obtain a write table lock before performing compaction to avoid race condition
    // with major compaction in mob-enabled column.
    boolean tableLocked = false;
    TableLock lock = null;
    try {
      // the tableLockManager might be null in testing. In that case, it is lock-free.
      if (tableLockManager != null) {
        lock = tableLockManager.writeLock(MobUtils.getTableLockName(tableName),
          "Run MobFileCompaction");
        lock.acquire();
      }
      tableLocked = true;
      compactor.compact(isForceAllFiles);
    } catch (Exception e) {
      LOG.error("Fail to compact the mob files for the column " + hcd.getNameAsString()
        + " in the table " + tableName.getNameAsString(), e);
    } finally {
      if (lock != null && tableLocked) {
        try {
          lock.release();
        } catch (IOException e) {
          LOG.error("Fail to release the write lock for the table " + tableName.getNameAsString(),
            e);
        }
      }
    }
  }

  /**
   * Creates a thread pool.
   * @param conf the Configuration
   * @return A thread pool.
   */
  public static ExecutorService createMobFileCompactorThreadPool(Configuration conf) {
    int maxThreads = conf.getInt(MobConstants.MOB_FILE_COMPACTION_THREADS_MAX,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_THREADS_MAX);
    if (maxThreads == 0) {
      maxThreads = 1;
    }
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, 60, TimeUnit.SECONDS, queue,
      Threads.newDaemonThreadFactory("MobFileCompactor"), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
          try {
            // waiting for a thread to pick up instead of throwing exceptions.
            queue.put(r);
          } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
          }
        }
      });
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }
}
