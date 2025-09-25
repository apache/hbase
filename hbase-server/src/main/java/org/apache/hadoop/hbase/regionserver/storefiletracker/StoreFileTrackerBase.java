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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.TRACKER_IMPL;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Base class for all store file tracker.
 * <p/>
 * Mainly used to place the common logic to skip persistent for secondary replicas.
 */
@InterfaceAudience.Private
abstract class StoreFileTrackerBase implements StoreFileTracker {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerBase.class);

  protected final Configuration conf;

  protected final boolean isPrimaryReplica;

  protected final StoreContext ctx;

  private volatile boolean cacheOnWriteLogged;

  protected StoreFileTrackerBase(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    this.conf = conf;
    this.isPrimaryReplica = isPrimaryReplica;
    this.ctx = ctx;
  }

  @Override
  public final List<StoreFileInfo> load() throws IOException {
    return doLoadStoreFiles(!isPrimaryReplica);
  }

  @Override
  public final void add(Collection<StoreFileInfo> newFiles) throws IOException {
    if (isPrimaryReplica) {
      doAddNewStoreFiles(newFiles);
    }
  }

  @Override
  public final void replace(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException {
    if (isPrimaryReplica) {
      doAddCompactionResults(compactedFiles, newFiles);
    }
  }

  @Override
  public final void set(List<StoreFileInfo> files) throws IOException {
    if (isPrimaryReplica) {
      doSetStoreFiles(files);
    }
  }

  @Override
  public TableDescriptorBuilder updateWithTrackerConfigs(TableDescriptorBuilder builder) {
    builder.setValue(TRACKER_IMPL, getTrackerName());
    return builder;
  }

  protected final String getTrackerName() {
    return StoreFileTrackerFactory.getStoreFileTrackerName(getClass());
  }

  private HFileContext createFileContext(Compression.Algorithm compression,
    boolean includeMVCCReadpoint, boolean includesTag, Encryption.Context encryptionContext) {
    if (compression == null) {
      compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    ColumnFamilyDescriptor family = ctx.getFamily();
    HFileContext hFileContext = new HFileContextBuilder().withIncludesMvcc(includeMVCCReadpoint)
      .withIncludesTags(includesTag).withCompression(compression)
      .withCompressTags(family.isCompressTags()).withChecksumType(StoreUtils.getChecksumType(conf))
      .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf))
      .withBlockSize(StoreUtils.getBlockSize(conf, family.getBlocksize())).withHBaseCheckSum(true)
      .withDataBlockEncoding(family.getDataBlockEncoding()).withEncryptionContext(encryptionContext)
      .withCreateTime(EnvironmentEdgeManager.currentTime()).withColumnFamily(family.getName())
      .withTableName(ctx.getTableName().getName()).withCellComparator(ctx.getComparator())
      .withIndexBlockEncoding(family.getIndexBlockEncoding()).build();
    return hFileContext;
  }

  @Override
  public final StoreFileWriter createWriter(CreateStoreFileWriterParams params) throws IOException {
    if (!isPrimaryReplica) {
      throw new IllegalStateException("Should not call create writer on secondary replicas");
    }
    // creating new cache config for each new writer
    final CacheConfig cacheConf = ctx.getCacheConf();
    final CacheConfig writerCacheConf = new CacheConfig(cacheConf);
    long totalCompactedFilesSize = params.totalCompactedFilesSize();
    if (params.isCompaction()) {
      // Don't cache data on write on compactions, unless specifically configured to do so
      // Cache only when total file size remains lower than configured threshold
      final boolean cacheCompactedBlocksOnWrite = cacheConf.shouldCacheCompactedBlocksOnWrite();
      // if data blocks are to be cached on write
      // during compaction, we should forcefully
      // cache index and bloom blocks as well
      if (
        cacheCompactedBlocksOnWrite
          && totalCompactedFilesSize <= cacheConf.getCacheCompactedBlocksOnWriteThreshold()
      ) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheCompactedBlocksOnWrite is true, hence enabled "
            + "cacheOnWrite for Data blocks, Index blocks and Bloom filter blocks", this);
          cacheOnWriteLogged = true;
        }
      } else {
        writerCacheConf.setCacheDataOnWrite(false);
        if (totalCompactedFilesSize > cacheConf.getCacheCompactedBlocksOnWriteThreshold()) {
          // checking condition once again for logging
          LOG.debug(
            "For {}, setting cacheCompactedBlocksOnWrite as false as total size of compacted "
              + "files - {}, is greater than cacheCompactedBlocksOnWriteThreshold - {}",
            this, totalCompactedFilesSize, cacheConf.getCacheCompactedBlocksOnWriteThreshold());
        }
      }
    } else {
      final boolean shouldCacheDataOnWrite = cacheConf.shouldCacheDataOnWrite();
      if (shouldCacheDataOnWrite) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheDataOnWrite is true, hence enabled cacheOnWrite for "
            + "Index blocks and Bloom filter blocks", this);
          cacheOnWriteLogged = true;
        }
      }
    }
    Encryption.Context encryptionContext = ctx.getEncryptionContext();
    HFileContext hFileContext = createFileContext(params.compression(),
      params.includeMVCCReadpoint(), params.includesTag(), encryptionContext);
    Path outputDir;
    if (requireWritingToTmpDirFirst()) {
      outputDir =
        new Path(ctx.getRegionFileSystem().getTempDir(), ctx.getFamily().getNameAsString());
    } else {
      outputDir = ctx.getFamilyStoreDirectoryPath();
    }
    StoreFileWriter.Builder builder =
      new StoreFileWriter.Builder(conf, writerCacheConf, ctx.getRegionFileSystem().getFileSystem())
        .withOutputDir(outputDir).withBloomType(ctx.getBloomFilterType())
        .withMaxKeyCount(params.maxKeyCount()).withFavoredNodes(ctx.getFavoredNodes())
        .withFileContext(hFileContext).withShouldDropCacheBehind(params.shouldDropBehind())
        .withCompactedFilesSupplier(ctx.getCompactedFilesSupplier())
        .withFileStoragePolicy(params.fileStoragePolicy())
        .withWriterCreationTracker(params.writerCreationTracker())
        .withMaxVersions(ctx.getMaxVersions()).withNewVersionBehavior(ctx.getNewVersionBehavior())
        .withCellComparator(ctx.getComparator()).withIsCompaction(params.isCompaction());
    return builder.build();
  }

  @Override
  public Reference createReference(Reference reference, Path path) throws IOException {
    FSDataOutputStream out = ctx.getRegionFileSystem().getFileSystem().create(path, false);
    try {
      out.write(reference.toByteArray());
    } finally {
      out.close();
    }
    return reference;
  }

  /**
   * Returns true if the specified family has reference files
   * @param familyName Column Family Name
   * @return true if family contains reference files
   */
  public boolean hasReferences() throws IOException {
    Path storeDir = ctx.getRegionFileSystem().getStoreDir(ctx.getFamily().getNameAsString());
    FileStatus[] files =
      CommonFSUtils.listStatus(ctx.getRegionFileSystem().getFileSystem(), storeDir);
    if (files != null) {
      for (FileStatus stat : files) {
        if (stat.isDirectory()) {
          continue;
        }
        if (StoreFileInfo.isReference(stat.getPath())) {
          LOG.trace("Reference {}", stat.getPath());
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Reference readReference(final Path p) throws IOException {
    InputStream in = ctx.getRegionFileSystem().getFileSystem().open(p);
    try {
      // I need to be able to move back in the stream if this is not a pb serialization so I can
      // do the Writable decoding instead.
      in = in.markSupported() ? in : new BufferedInputStream(in);
      int pblen = ProtobufUtil.lengthOfPBMagic();
      in.mark(pblen);
      byte[] pbuf = new byte[pblen];
      IOUtils.readFully(in, pbuf, 0, pblen);
      // WATCHOUT! Return in middle of function!!!
      if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
        return Reference.convert(
          org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos.Reference.parseFrom(in));
      }
      // Else presume Writables. Need to reset the stream since it didn't start w/ pb.
      // We won't bother rewriting thie Reference as a pb since Reference is transitory.
      in.reset();
      Reference r = new Reference();
      DataInputStream dis = new DataInputStream(in);
      // Set in = dis so it gets the close below in the finally on our way out.
      in = dis;
      r.readFields(dis);
      return r;
    } finally {
      in.close();
    }
  }

  @Override
  public StoreFileInfo getStoreFileInfo(Path initialPath, boolean primaryReplica)
    throws IOException {
    return getStoreFileInfo(null, initialPath, primaryReplica);
  }

  @Override
  public StoreFileInfo getStoreFileInfo(FileStatus fileStatus, Path initialPath,
    boolean primaryReplica) throws IOException {
    FileSystem fs = this.ctx.getRegionFileSystem().getFileSystem();
    assert fs != null;
    assert initialPath != null;
    assert conf != null;
    Reference reference = null;
    HFileLink link = null;
    long createdTimestamp = 0;
    long size = 0;
    Path p = initialPath;
    if (HFileLink.isHFileLink(p)) {
      // HFileLink
      reference = null;
      link = HFileLink.buildFromHFileLinkPattern(conf, p);
      LOG.trace("{} is a link", p);
    } else if (StoreFileInfo.isReference(p)) {
      reference = readReference(p);
      Path referencePath = StoreFileInfo.getReferredToFile(p);
      if (HFileLink.isHFileLink(referencePath)) {
        // HFileLink Reference
        link = HFileLink.buildFromHFileLinkPattern(conf, referencePath);
      } else {
        // Reference
        link = null;
      }
      LOG.trace("{} is a {} reference to {}", p, reference.getFileRegion(), referencePath);
    } else
      if (StoreFileInfo.isHFile(p) || StoreFileInfo.isMobFile(p) || StoreFileInfo.isMobRefFile(p)) {
        // HFile
        if (fileStatus != null) {
          createdTimestamp = fileStatus.getModificationTime();
          size = fileStatus.getLen();
        } else {
          FileStatus fStatus = fs.getFileStatus(initialPath);
          createdTimestamp = fStatus.getModificationTime();
          size = fStatus.getLen();
        }
      } else {
        throw new IOException("path=" + p + " doesn't look like a valid StoreFile");
      }
    return new StoreFileInfo(conf, fs, createdTimestamp, initialPath, size, reference, link,
      isPrimaryReplica);
  }

  public String createHFileLink(final TableName linkedTable, final String linkedRegion,
    final String hfileName, final boolean createBackRef) throws IOException {
    String name = HFileLink.createHFileLinkName(linkedTable, linkedRegion, hfileName);
    String refName = HFileLink.createBackReferenceName(ctx.getTableName().toString(),
      ctx.getRegionInfo().getEncodedName());

    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    // Make sure the destination directory exists
    fs.mkdirs(ctx.getFamilyStoreDirectoryPath());

    // Make sure the FileLink reference directory exists
    Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(conf, linkedTable, linkedRegion,
      ctx.getFamily().getNameAsString());
    Path backRefPath = null;
    if (createBackRef) {
      Path backRefssDir = HFileLink.getBackReferencesDir(archiveStoreDir, hfileName);
      fs.mkdirs(backRefssDir);

      // Create the reference for the link
      backRefPath = new Path(backRefssDir, refName);
      fs.createNewFile(backRefPath);
    }
    try {
      // Create the link
      if (fs.createNewFile(new Path(ctx.getFamilyStoreDirectoryPath(), name))) {
        return name;
      }
    } catch (IOException e) {
      LOG.error("couldn't create the link=" + name + " for " + ctx.getFamilyStoreDirectoryPath(),
        e);
      // Revert the reference if the link creation failed
      if (createBackRef) {
        fs.delete(backRefPath, false);
      }
      throw e;
    }
    throw new IOException("File link=" + name + " already exists under "
      + ctx.getFamilyStoreDirectoryPath() + " folder.");

  }

  public String createFromHFileLink(final String hfileLinkName, final boolean createBackRef)
    throws IOException {
    Matcher m = HFileLink.LINK_NAME_PATTERN.matcher(hfileLinkName);
    if (!m.matches()) {
      throw new IllegalArgumentException(hfileLinkName + " is not a valid HFileLink name!");
    }
    return createHFileLink(TableName.valueOf(m.group(1), m.group(2)), m.group(3), m.group(4),
      createBackRef);
  }

  public void removeStoreFiles(List<HStoreFile> storeFiles) throws IOException {
    archiveStoreFiles(storeFiles);
  }

  protected void archiveStoreFiles(List<HStoreFile> storeFiles) throws IOException {
    HFileArchiver.archiveStoreFiles(this.conf, ctx.getRegionFileSystem().getFileSystem(),
      ctx.getRegionInfo(), ctx.getRegionFileSystem().getTableDir(), ctx.getFamily().getName(),
      storeFiles);
  }

  /**
   * For primary replica, we will call load once when opening a region, and the implementation could
   * choose to do some cleanup work. So here we use {@code readOnly} to indicate that whether you
   * are allowed to do the cleanup work. For secondary replicas, we will set {@code readOnly} to
   * {@code true}.
   */
  protected abstract List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException;

  protected abstract void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException;

  protected abstract void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException;

  protected abstract void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException;

}
