/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.io.hfile;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * Class to store blocks into memcached.
 * This should only be used on a cluster of Memcached daemons that are tuned well and have a
 * good network connection to the HBase regionservers. Any other use will likely slow down HBase
 * greatly.
 */
@InterfaceAudience.Private
public class MemcachedBlockCache implements BlockCache {
  private static final Log LOG = LogFactory.getLog(MemcachedBlockCache.class.getName());

  // Some memcache versions won't take more than 1024 * 1024. So set the limit below
  // that just in case this client is used with those versions.
  public static final int MAX_SIZE = 1020 * 1024;

  // Config key for what memcached servers to use.
  // They should be specified in a comma sperated list with ports.
  // like:
  //
  // host1:11211,host3:8080,host4:11211
  public static final String MEMCACHED_CONFIG_KEY = "hbase.cache.memcached.servers";
  public static final String MEMCACHED_TIMEOUT_KEY = "hbase.cache.memcached.timeout";
  public static final String MEMCACHED_OPTIMEOUT_KEY = "hbase.cache.memcached.optimeout";
  public static final String MEMCACHED_OPTIMIZE_KEY = "hbase.cache.memcached.spy.optimze";
  public static final long MEMCACHED_DEFAULT_TIMEOUT = 500;
  public static final boolean MEMCACHED_OPTIMIZE_DEFAULT = false;

  private final MemcachedClient client;
  private final HFileBlockTranscoder tc = new HFileBlockTranscoder();
  private final CacheStats cacheStats = new CacheStats("MemcachedBlockCache");

  public MemcachedBlockCache(Configuration c) throws IOException {
    LOG.info("Creating MemcachedBlockCache");

    long opTimeout = c.getLong(MEMCACHED_OPTIMEOUT_KEY, MEMCACHED_DEFAULT_TIMEOUT);
    long queueTimeout = c.getLong(MEMCACHED_TIMEOUT_KEY, opTimeout + MEMCACHED_DEFAULT_TIMEOUT);
    boolean optimize = c.getBoolean(MEMCACHED_OPTIMIZE_KEY, MEMCACHED_OPTIMIZE_DEFAULT);

    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder()
        .setOpTimeout(opTimeout)
        .setOpQueueMaxBlockTime(queueTimeout) // Cap the max time before anything times out
        .setFailureMode(FailureMode.Redistribute)
        .setShouldOptimize(optimize)
        .setDaemon(true)                      // Don't keep threads around past the end of days.
        .setUseNagleAlgorithm(false)          // Ain't nobody got time for that
        .setReadBufferSize(HConstants.DEFAULT_BLOCKSIZE * 4 * 1024); // Much larger just in case

    // Assume only the localhost is serving memecached.
    // A la mcrouter or co-locating memcached with split regionservers.
    //
    // If this config is a pool of memecached servers they will all be used according to the
    // default hashing scheme defined by the memcache client. Spy Memecache client in this
    // case.
    String serverListString = c.get(MEMCACHED_CONFIG_KEY,"localhost:11211");
    String[] servers = serverListString.split(",");
    List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(servers.length);
    for (String s:servers) {
      serverAddresses.add(Addressing.createInetSocketAddressFromHostAndPortStr(s));
    }

    client = new MemcachedClient(builder.build(), serverAddresses);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey,
                         Cacheable buf,
                         boolean inMemory,
                         boolean cacheDataInL1) {
    cacheBlock(cacheKey, buf);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    if (buf instanceof HFileBlock) {
      client.add(cacheKey.toString(), MAX_SIZE, (HFileBlock) buf, tc);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MemcachedBlockCache can not cache Cacheable's of type "
            + buf.getClass().toString());
      }
    }
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
                            boolean repeat, boolean updateCacheMetrics) {
    // Assume that nothing is the block cache
    HFileBlock result = null;

    try (TraceScope traceScope = Trace.startSpan("MemcachedBlockCache.getBlock")) {
      result = client.get(cacheKey.toString(), tc);
    } catch (Exception e) {
      // Catch a pretty broad set of exceptions to limit any changes in the memecache client
      // and how it handles failures from leaking into the read path.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception pulling from memcached [ "
            + cacheKey.toString()
            + " ]. Treating as a miss.", e);
      }
      result = null;
    } finally {
      // Update stats if this request doesn't have it turned off 100% of the time
      if (updateCacheMetrics) {
        if (result == null) {
          cacheStats.miss(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
        } else {
          cacheStats.hit(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
        }
      }
    }

    return result;
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    try {
      cacheStats.evict();
      return client.delete(cacheKey.toString()).get();
    } catch (InterruptedException e) {
      LOG.warn("Error deleting " + cacheKey.toString(), e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error deleting " + cacheKey.toString(), e);
      }
    }
    return false;
  }

  /**
   * This method does nothing so that memcached can handle all evictions.
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return 0;
  }

  @Override
  public CacheStats getStats() {
    return cacheStats;
  }

  @Override
  public void shutdown() {
    client.shutdown();
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public long getFreeSize() {
    return 0;
  }

  @Override
  public long getCurrentSize() {
    return 0;
  }

  @Override
  public long getBlockCount() {
    return 0;
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    return new Iterator<CachedBlock>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public CachedBlock next() {
        throw new NoSuchElementException("MemcachedBlockCache can't iterate over blocks.");
      }

      @Override
      public void remove() {

      }
    };
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return null;
  }

  /**
   * Class to encode and decode an HFileBlock to and from memecached's resulting byte arrays.
   */
  private static class HFileBlockTranscoder implements Transcoder<HFileBlock> {

    @Override
    public boolean asyncDecode(CachedData d) {
      return false;
    }

    @Override
    public CachedData encode(HFileBlock block) {
      ByteBuffer bb = ByteBuffer.allocate(block.getSerializedLength());
      block.serialize(bb);
      return new CachedData(0, bb.array(), CachedData.MAX_SIZE);
    }

    @Override
    public HFileBlock decode(CachedData d) {
      try {
        ByteBuffer buf = ByteBuffer.wrap(d.getData());
        return (HFileBlock) HFileBlock.blockDeserializer.deserialize(buf, true);
      } catch (IOException e) {
        LOG.warn("Error deserializing data from memcached",e);
      }
      return null;
    }

    @Override
    public int getMaxSize() {
      return MAX_SIZE;
    }
  }

}
