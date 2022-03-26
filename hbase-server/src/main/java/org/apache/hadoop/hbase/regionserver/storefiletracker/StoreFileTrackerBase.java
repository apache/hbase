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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all store file tracker.
 * <p/>
 * Mainly used to place the common logic to skip persistent for secondary replicas.
 */
@InterfaceAudience.Private
abstract class StoreFileTrackerBase implements StoreFileTracker {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerBase.class);

  protected final Configuration conf;

  protected final TableName tableName;

  protected final boolean isPrimaryReplica;

  protected final StoreContext ctx;

  private volatile boolean cacheOnWriteLogged;

  protected StoreFileTrackerBase(Configuration conf, TableName tableName, boolean isPrimaryReplica,
    StoreContext ctx) {
    this.conf = conf;
    this.tableName = tableName;
    this.isPrimaryReplica = isPrimaryReplica;
    this.ctx = ctx;
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
      .withBlockSize(family.getBlocksize()).withHBaseCheckSum(true)
      .withDataBlockEncoding(family.getDataBlockEncoding()).withEncryptionContext(encryptionContext)
      .withCreateTime(EnvironmentEdgeManager.currentTime()).withColumnFamily(family.getName())
      .withTableName(tableName.getName()).withCellComparator(ctx.getComparator()).build();
    return hFileContext;
  }

  @Override
  public final StoreFileWriter createWriter(CreateStoreFileWriterParams params)
    throws IOException {
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
      if (cacheCompactedBlocksOnWrite &&
        totalCompactedFilesSize <= cacheConf.getCacheCompactedBlocksOnWriteThreshold()) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheCompactedBlocksOnWrite is true, hence enabled " +
            "cacheOnWrite for Data blocks, Index blocks and Bloom filter blocks", this);
          cacheOnWriteLogged = true;
        }
      } else {
        writerCacheConf.setCacheDataOnWrite(false);
        if (totalCompactedFilesSize > cacheConf.getCacheCompactedBlocksOnWriteThreshold()) {
          // checking condition once again for logging
          LOG.debug(
            "For {}, setting cacheCompactedBlocksOnWrite as false as total size of compacted " +
              "files - {}, is greater than cacheCompactedBlocksOnWriteThreshold - {}",
            this, totalCompactedFilesSize, cacheConf.getCacheCompactedBlocksOnWriteThreshold());
        }
      }
    } else {
      final boolean shouldCacheDataOnWrite = cacheConf.shouldCacheDataOnWrite();
      if (shouldCacheDataOnWrite) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheDataOnWrite is true, hence enabled cacheOnWrite for " +
            "Index blocks and Bloom filter blocks", this);
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
      throw new UnsupportedOperationException("not supported yet");
    }
    StoreFileWriter.Builder builder =
      new StoreFileWriter.Builder(conf, writerCacheConf, ctx.getRegionFileSystem().getFileSystem())
        .withOutputDir(outputDir).withBloomType(ctx.getBloomFilterType())
        .withMaxKeyCount(params.maxKeyCount()).withFavoredNodes(ctx.getFavoredNodes())
        .withFileContext(hFileContext).withShouldDropCacheBehind(params.shouldDropBehind())
        .withCompactedFilesSupplier(ctx.getCompactedFilesSupplier())
        .withFileStoragePolicy(params.fileStoragePolicy());
    return builder.build();
  }

  /**
   * Whether the implementation of this tracker requires you to write to temp directory first, i.e,
   * does not allow broken store files under the actual data directory.
   */
  protected abstract boolean requireWritingToTmpDirFirst();

  protected abstract void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException;

  protected abstract void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException;
}
