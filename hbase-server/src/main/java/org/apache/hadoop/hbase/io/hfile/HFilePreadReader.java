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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.util.Optional;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketEntry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link HFile.Reader} to deal with pread.
 */
@InterfaceAudience.Private
public class HFilePreadReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(HFileReaderImpl.class);

  public HFilePreadReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    final MutableBoolean fileAlreadyCached = new MutableBoolean(false);
    Optional<BucketCache> bucketCacheOptional =
      BucketCache.getBucketCacheFromCacheConfig(cacheConf);
    bucketCacheOptional.ifPresent(bc -> fileAlreadyCached
      .setValue(bc.getFullyCachedFiles().get(path.getName()) == null ? false : true));
    // Prefetch file blocks upon open if requested
    if (
      cacheConf.shouldPrefetchOnOpen() && cacheIfCompactionsOff()
        && !fileAlreadyCached.booleanValue()
    ) {
      PrefetchExecutor.request(path, new Runnable() {
        @Override
        public void run() {
          long offset = 0;
          long end = 0;
          HFile.Reader prefetchStreamReader = null;
          try {
            ReaderContext streamReaderContext = ReaderContextBuilder.newBuilder(context)
              .withReaderType(ReaderContext.ReaderType.STREAM)
              .withInputStreamWrapper(new FSDataInputStreamWrapper(context.getFileSystem(),
                context.getInputStreamWrapper().getReaderPath()))
              .build();
            prefetchStreamReader =
              new HFileStreamReader(streamReaderContext, fileInfo, cacheConf, conf);
            end = getTrailer().getLoadOnOpenDataOffset();
            if (LOG.isTraceEnabled()) {
              LOG.trace("Prefetch start " + getPathOffsetEndStr(path, offset, end));
            }
            // Don't use BlockIterator here, because it's designed to read load-on-open section.
            long onDiskSizeOfNextBlock = -1;
            while (offset < end) {
              if (Thread.interrupted()) {
                break;
              }
              // BucketCache can be persistent and resilient to restarts, so we check first if the
              // block exists on its in-memory index, if so, we just update the offset and move on
              // to the next block without actually going read all the way to the cache.
              if (bucketCacheOptional.isPresent()) {
                BucketCache cache = bucketCacheOptional.get();
                BlockCacheKey cacheKey = new BlockCacheKey(name, offset);
                BucketEntry entry = cache.getBackingMap().get(cacheKey);
                if (entry != null) {
                  cacheKey = new BlockCacheKey(name, offset);
                  entry = cache.getBackingMap().get(cacheKey);
                  if (entry == null) {
                    LOG.debug("No cache key {}, we'll read and cache it", cacheKey);
                  } else {
                    offset += entry.getOnDiskSizeWithHeader();
                    LOG.debug("Found cache key {}. Skipping prefetch, the block is already cached.",
                      cacheKey);
                    continue;
                  }
                } else {
                  LOG.debug("No entry in the backing map for cache key {}", cacheKey);
                }
              }
              // Perhaps we got our block from cache? Unlikely as this may be, if it happens, then
              // the internal-to-hfileblock thread local which holds the overread that gets the
              // next header, will not have happened...so, pass in the onDiskSize gotten from the
              // cached block. This 'optimization' triggers extremely rarely I'd say.
              HFileBlock block = prefetchStreamReader.readBlock(offset, onDiskSizeOfNextBlock,
                /* cacheBlock= */true, /* pread= */false, false, false, null, null, true);
              try {
                onDiskSizeOfNextBlock = block.getNextBlockOnDiskSize();
                offset += block.getOnDiskSizeWithHeader();
              } finally {
                // Ideally here the readBlock won't find the block in cache. We call this
                // readBlock so that block data is read from FS and cached in BC. we must call
                // returnBlock here to decrease the reference count of block.
                block.release();
              }
            }
            bucketCacheOptional.ifPresent(bc -> bc.fileCacheCompleted(path.getName()));
          } catch (IOException e) {
            // IOExceptions are probably due to region closes (relocation, etc.)
            if (LOG.isTraceEnabled()) {
              LOG.trace("Prefetch " + getPathOffsetEndStr(path, offset, end), e);
            }
          } catch (Throwable e) {
            // Other exceptions are interesting
            LOG.warn("Prefetch " + getPathOffsetEndStr(path, offset, end), e);
          } finally {
            if (prefetchStreamReader != null) {
              try {
                prefetchStreamReader.close(false);
              } catch (IOException e) {
                LOG.warn("Close prefetch stream reader failed, path: " + path, e);
              }
            }
            PrefetchExecutor.complete(path);
          }
        }
      });
    }
  }

  private static String getPathOffsetEndStr(final Path path, final long offset, final long end) {
    return "path=" + path.toString() + ", offset=" + offset + ", end=" + end;
  }

  public void close(boolean evictOnClose) throws IOException {
    PrefetchExecutor.cancel(path);
    // Deallocate blocks in load-on-open section
    this.fileInfo.close();
    // Deallocate data blocks
    cacheConf.getBlockCache().ifPresent(cache -> {
      if (evictOnClose) {
        int numEvicted = cache.evictBlocksByHfileName(name);
        if (LOG.isTraceEnabled()) {
          LOG.trace("On close, file= {} evicted= {} block(s)", name, numEvicted);
        }
      }
    });
    fsBlockReader.closeStreams();
  }
}
