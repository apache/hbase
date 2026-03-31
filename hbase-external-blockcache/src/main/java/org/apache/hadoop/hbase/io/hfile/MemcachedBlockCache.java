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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Class to store blocks into memcached. This should only be used on a cluster of Memcached daemons
 * that are tuned well and have a good network connection to the HBase regionservers. Any other use
 * will likely slow down HBase greatly.
 */
@InterfaceAudience.Private
@SuppressWarnings("FutureReturnValueIgnored")
public class MemcachedBlockCache implements BlockCache {
  private static final Logger LOG = LoggerFactory.getLogger(MemcachedBlockCache.class.getName());

  // Some memcache versions won't take more than 1024 * 1024. So set the limit below
  // that just in case this client is used with those versions.
  public static final int MAX_SIZE = 1020 * 1024;

  // Start memcached with -I <MAX_SIZE> to ensure it has the ability to store blocks of this size
  public static final int MAX_TIME = 60 * 60 * 24 * 30; // 30 days, max allowed per the memcached
                                                        // spec

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
  public static final int STAT_THREAD_PERIOD = 60 * 5;

  private transient final MemcachedClient client;
  private transient final HFileBlockTranscoder tc = new HFileBlockTranscoder();
  private final CacheStats cacheStats = new CacheStats("MemcachedBlockCache");
  private final AtomicLong cachedCount = new AtomicLong();
  private final AtomicLong notCachedCount = new AtomicLong();
  private final AtomicLong cacheErrorCount = new AtomicLong();
  private final AtomicLong timeoutCount = new AtomicLong();

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private transient final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("MemcachedBlockCacheStatsExecutor").setDaemon(true).build());

  public MemcachedBlockCache(Configuration c) throws IOException {
    LOG.info("Creating MemcachedBlockCache");

    long opTimeout = c.getLong(MEMCACHED_OPTIMEOUT_KEY, MEMCACHED_DEFAULT_TIMEOUT);
    long queueTimeout = c.getLong(MEMCACHED_TIMEOUT_KEY, opTimeout + MEMCACHED_DEFAULT_TIMEOUT);
    boolean optimize = c.getBoolean(MEMCACHED_OPTIMIZE_KEY, MEMCACHED_OPTIMIZE_DEFAULT);

    ConnectionFactoryBuilder builder =
      new ConnectionFactoryBuilder().setOpTimeout(opTimeout).setOpQueueMaxBlockTime(queueTimeout)
        .setFailureMode(FailureMode.Redistribute).setShouldOptimize(optimize).setDaemon(true)
        .setUseNagleAlgorithm(false).setReadBufferSize(MAX_SIZE);

    // Assume only the localhost is serving memcached.
    // A la mcrouter or co-locating memcached with split regionservers.
    //
    // If this config is a pool of memcached servers they will all be used according to the
    // default hashing scheme defined by the memcached client. Spy Memecache client in this
    // case.
    String serverListString = c.get(MEMCACHED_CONFIG_KEY, "localhost:11211");
    String[] servers = serverListString.split(",");
    // MemcachedClient requires InetSocketAddresses, we have to create them now. Implies any
    // resolved identities cannot have their address mappings changed while the MemcachedClient
    // instance is alive. We won't get a chance to trigger re-resolution.
    List<InetSocketAddress> serverAddresses = new ArrayList<>(servers.length);
    for (String s : servers) {
      serverAddresses.add(Addressing.createInetSocketAddressFromHostAndPortStr(s));
    }

    client = createMemcachedClient(builder.build(), serverAddresses);
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this), STAT_THREAD_PERIOD,
      STAT_THREAD_PERIOD, TimeUnit.SECONDS);
  }

  protected MemcachedClient createMemcachedClient(ConnectionFactory factory,
    List<InetSocketAddress> serverAddresses) throws IOException {
    return new MemcachedClient(factory, serverAddresses);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    cacheBlock(cacheKey, buf);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    if (buf instanceof HFileBlock) {
      if (buf.getSerializedLength() > MAX_SIZE) {
        LOG.debug("Block of type {} with key {} is too large, size={}, max={}, will not cache",
          buf.getClass(), cacheKey, buf.getSerializedLength(), MAX_SIZE);
        notCachedCount.incrementAndGet();
        return;
      }
      client.set(cacheKey.toString(), MAX_TIME, (HFileBlock) buf, tc).addListener(f -> {
        try {
          f.get();
          cachedCount.incrementAndGet();
        } catch (Exception e) {
          LOG.warn("Failed to cache block with key " + cacheKey, e);
          cacheErrorCount.incrementAndGet();
        }
      });
    } else {
      LOG.debug("Can not cache Cacheables of type {} with key {}", buf.getClass(), cacheKey);
      notCachedCount.incrementAndGet();
    }
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    // Assume that nothing is the block cache
    HFileBlock result = null;
    Span span = TraceUtil.getGlobalTracer().spanBuilder("MemcachedBlockCache.getBlock").startSpan();
    try (Scope traceScope = span.makeCurrent()) {
      result = client.get(cacheKey.toString(), tc);
    } catch (Exception e) {
      // Catch a pretty broad set of exceptions to limit any changes in the memcache client
      // and how it handles failures from leaking into the read path.
      if (
        (e instanceof OperationTimeoutException) || ((e instanceof RuntimeException)
          && (e.getCause() instanceof OperationTimeoutException))
      ) {
        timeoutCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Timeout getting key " + cacheKey.toString(), e);
        }
      } else {
        cacheErrorCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Exception getting key " + cacheKey.toString(), e);
        }
      }
      result = null;
    } finally {
      span.end();
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
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < 10; i++) {
      if (!this.scheduleThreadPool.isShutdown()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    if (!this.scheduleThreadPool.isShutdown()) {
      List<Runnable> runnables = this.scheduleThreadPool.shutdownNow();
      LOG.debug("Still running " + runnables);
    }
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public long getMaxSize() {
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
  public long getCurrentDataSize() {
    return 0;
  }

  @Override
  public long getBlockCount() {
    return 0;
  }

  @Override
  public long getDataBlockCount() {
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
      block.serialize(bb, true);
      return new CachedData(0, bb.array(), CachedData.MAX_SIZE);
    }

    @Override
    public HFileBlock decode(CachedData d) {
      try {
        ByteBuff buf = new SingleByteBuff(ByteBuffer.wrap(d.getData()));
        return (HFileBlock) HFileBlock.BLOCK_DESERIALIZER.deserialize(buf, ByteBuffAllocator.HEAP);
      } catch (IOException e) {
        LOG.warn("Failed to deserialize data from memcached", e);
      }
      return null;
    }

    @Override
    public int getMaxSize() {
      return MAX_SIZE;
    }
  }

  private static class StatisticsThread extends Thread {

    private final MemcachedBlockCache c;

    public StatisticsThread(MemcachedBlockCache c) {
      super("MemcachedBlockCacheStats");
      setDaemon(true);
      this.c = c;
    }

    @Override
    public void run() {
      c.logStats();
    }

  }

  public void logStats() {
    LOG.info("cached=" + cachedCount.get() + ", notCached=" + notCachedCount.get()
      + ", cacheErrors=" + cacheErrorCount.get() + ", timeouts=" + timeoutCount.get() + ", reads="
      + cacheStats.getRequestCount() + ", " + "hits=" + cacheStats.getHitCount() + ", hitRatio="
      + (cacheStats.getHitCount() == 0
        ? "0"
        : (StringUtils.formatPercent(cacheStats.getHitRatio(), 2) + ", "))
      + "cachingAccesses=" + cacheStats.getRequestCachingCount() + ", " + "cachingHits="
      + cacheStats.getHitCachingCount() + ", " + "cachingHitsRatio="
      + (cacheStats.getHitCachingCount() == 0
        ? "0,"
        : (StringUtils.formatPercent(cacheStats.getHitCachingRatio(), 2) + ", "))
      + "evictions=" + cacheStats.getEvictionCount() + ", " + "evicted="
      + cacheStats.getEvictedCount() + ", " + "evictedPerRun=" + cacheStats.evictedPerEviction());
  }

}
