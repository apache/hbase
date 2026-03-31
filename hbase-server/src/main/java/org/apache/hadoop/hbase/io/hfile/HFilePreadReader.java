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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link HFile.Reader} to deal with pread.
 */
@InterfaceAudience.Private
public class HFilePreadReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(HFileReaderImpl.class);

  private static final int WAIT_TIME_FOR_CACHE_INITIALIZATION = 10 * 60 * 1000;

  public HFilePreadReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    // Initialize HFileInfo object with metadata for caching decisions
    fileInfo.initMetaAndIndex(this);
    // master hosted regions, like the master procedures store wouldn't have a block cache
    // Prefetch file blocks upon open if requested
    if (cacheConf.getBlockCache().isPresent() && cacheConf.shouldPrefetchOnOpen()) {
      PrefetchExecutor.request(path, new Runnable() {
        @Override
        public void run() {
          long offset = 0;
          long end = 0;
          HFile.Reader prefetchStreamReader = null;
          try {
            cacheConf.getBlockCache().ifPresent(
              cache -> cache.waitForCacheInitialization(WAIT_TIME_FOR_CACHE_INITIALIZATION));
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
            // if we are here, block cache is present anyways
            BlockCache cache = cacheConf.getBlockCache().get();
            boolean interrupted = false;
            int blockCount = 0;
            int dataBlockCount = 0;
            while (offset < end) {
              if (Thread.interrupted()) {
                break;
              }
              // Some cache implementations can be persistent and resilient to restarts,
              // so we check first if the block exists on its in-memory index, if so, we just
              // update the offset and move on to the next block without actually going read all
              // the way to the cache.
              BlockCacheKey cacheKey = new BlockCacheKey(name, offset);
              if (cache.isAlreadyCached(cacheKey).orElse(false)) {
                // Right now, isAlreadyCached is only supported by BucketCache, which should
                // always cache data blocks.
                int size = cache.getBlockSize(cacheKey).orElse(0);
                if (size > 0) {
                  offset += size;
                  LOG.debug("Found block of size {} for cache key {}. "
                    + "Skipping prefetch, the block is already cached.", size, cacheKey);
                  blockCount++;
                  dataBlockCount++;
                  // We need to reset this here, because we don't know the size of next block, since
                  // we never recovered the current block.
                  onDiskSizeOfNextBlock = -1;
                  continue;
                } else {
                  LOG.debug("Found block for cache key {}, but couldn't get its size. "
                    + "Maybe the cache implementation doesn't support it? "
                    + "We'll need to read the block from cache or file system. ", cacheKey);
                }
              } else {
                LOG.debug("No entry in the backing map for cache key {}. ", cacheKey);
              }
              // Perhaps we got our block from cache? Unlikely as this may be, if it happens, then
              // the internal-to-hfileblock thread local which holds the overread that gets the
              // next header, will not have happened...so, pass in the onDiskSize gotten from the
              // cached block. This 'optimization' triggers extremely rarely I'd say.
              HFileBlock block = prefetchStreamReader.readBlock(offset, onDiskSizeOfNextBlock,
                /* cacheBlock= */true, /* pread= */false, false, false, null, null, true);
              try {
                if (!cacheConf.isInMemory()) {
                  if (!cache.blockFitsIntoTheCache(block).orElse(true)) {
                    LOG.warn(
                      "Interrupting prefetch for file {} because block {} of size {} "
                        + "doesn't fit in the available cache space. isCacheEnabled: {}",
                      path, cacheKey, block.getOnDiskSizeWithHeader(), cache.isCacheEnabled());
                    interrupted = true;
                    break;
                  }
                  if (!cacheConf.isHeapUsageBelowThreshold()) {
                    LOG.warn(
                      "Interrupting prefetch because heap usage is above the threshold: {} "
                        + "configured via {}",
                      cacheConf.getHeapUsageThreshold(), CacheConfig.PREFETCH_HEAP_USAGE_THRESHOLD);
                    interrupted = true;
                    break;
                  }
                }
                onDiskSizeOfNextBlock = block.getNextBlockOnDiskSize();
                offset += block.getOnDiskSizeWithHeader();
                blockCount++;
                if (block.getBlockType().isData()) {
                  dataBlockCount++;
                }
              } finally {
                // Ideally here the readBlock won't find the block in cache. We call this
                // readBlock so that block data is read from FS and cached in BC. we must call
                // returnBlock here to decrease the reference count of block.
                block.release();
              }
            }
            if (!interrupted) {
              cacheConf.getBlockCache().get().notifyFileCachingCompleted(path, blockCount,
                dataBlockCount, offset);
            }
          } catch (IOException e) {
            // IOExceptions are probably due to region closes (relocation, etc.)
            if (LOG.isDebugEnabled()) {
              LOG.debug("Prefetch " + getPathOffsetEndStr(path, offset, end), e);
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

  /*
   * Get the region name for the given file path. A HFile is always kept under the <region>/<column
   * family>/<hfile>. To find the region for a given hFile, just find the name of the grandparent
   * directory.
   */
  private static String getRegionName(Path path) {
    return path.getParent().getParent().getName();
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
