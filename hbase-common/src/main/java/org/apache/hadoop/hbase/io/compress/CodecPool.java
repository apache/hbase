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
package org.apache.hadoop.hbase.io.compress;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A global compressor/decompressor pool used to save and reuse (possibly native)
 * compression/decompression codecs. Copied from the class of the same name in hadoop-common and
 * augmented to improve borrow/return performance.
 */
@InterfaceAudience.Private
public class CodecPool {
  private static final Logger LOG = LoggerFactory.getLogger(CodecPool.class);

  private static final ConcurrentMap<Class<Compressor>, NavigableSet<Compressor>> COMPRESSOR_POOL =
    new ConcurrentHashMap<>();

  private static final ConcurrentMap<Class<Decompressor>,
    NavigableSet<Decompressor>> DECOMPRESSOR_POOL = new ConcurrentHashMap<>();

  private static <T> LoadingCache<Class<T>, AtomicInteger> createCache() {
    return Caffeine.newBuilder().build(key -> new AtomicInteger());
  }

  /**
   * Map to track the number of leased compressors. Only used in unit tests, kept null otherwise.
   */
  @Nullable
  private static LoadingCache<Class<Compressor>, AtomicInteger> compressorCounts = null;

  /**
   * Map to tracks the number of leased decompressors. Only used in unit tests, kept null otherwise.
   */
  @Nullable
  private static LoadingCache<Class<Decompressor>, AtomicInteger> decompressorCounts = null;

  /**
   * Call if you want lease counting to be enabled. Only used in unit tests.
   */
  static void initLeaseCounting() {
    compressorCounts = createCache();
    decompressorCounts = createCache();
  }

  private static <T> T borrow(ConcurrentMap<Class<T>, NavigableSet<T>> pool,
    Class<? extends T> codecClass) {
    if (codecClass == null) {
      return null;
    }

    NavigableSet<T> codecSet = pool.get(codecClass);
    if (codecSet != null) {
      // If a copy of the codec is available, pollFirst() will grab one.
      // If not, it will return null.
      return codecSet.pollFirst();
    } else {
      return null;
    }
  }

  private static <T> boolean payback(ConcurrentMap<Class<T>, NavigableSet<T>> pool, T codec) {
    if (codec != null) {
      Class<T> codecClass = ReflectionUtils.getClass(codec);
      Set<T> codecSet = pool.computeIfAbsent(codecClass,
        k -> new ConcurrentSkipListSet<>(Comparator.comparingInt(System::identityHashCode)));
      return codecSet.add(codec);
    }
    return false;
  }

  /**
   * Copied from hadoop-common without significant modification.
   */
  @SuppressWarnings("unchecked")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "LoadingCache will compute value if absent")
  private static <T> int getLeaseCount(LoadingCache<Class<T>, AtomicInteger> usageCounts,
    Class<? extends T> codecClass) {
    return usageCounts.get((Class<T>) codecClass).get();
  }

  /**
   * Copied from hadoop-common without significant modification.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "LoadingCache will compute value if absent")
  private static <T> void updateLeaseCount(LoadingCache<Class<T>, AtomicInteger> usageCounts,
    T codec, int delta) {
    if (codec != null && usageCounts != null) {
      Class<T> codecClass = ReflectionUtils.getClass(codec);
      usageCounts.get(codecClass).addAndGet(delta);
    }
  }

  /**
   * Get a {@link Compressor} for the given {@link CompressionCodec} from the pool, or get a new one
   * if the pool is empty. Copied from hadoop-common without significant modification.
   */
  public static Compressor getCompressor(CompressionCodec codec, Configuration conf) {
    Compressor compressor = borrow(COMPRESSOR_POOL, codec.getCompressorType());
    if (compressor == null) {
      compressor = codec.createCompressor();
      LOG.info("Got brand-new compressor [" + codec.getDefaultExtension() + "]");
    } else {
      compressor.reinit(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got recycled compressor");
      }
    }
    if (compressor != null && !compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      updateLeaseCount(compressorCounts, compressor, 1);
    }
    return compressor;
  }

  public static Compressor getCompressor(CompressionCodec codec) {
    return getCompressor(codec, null);
  }

  /**
   * Get a {@link Decompressor} for the given {@link CompressionCodec} from the pool, or get a new
   * one if the pool is empty. Copied from hadoop-common without significant modification.
   */
  public static Decompressor getDecompressor(CompressionCodec codec) {
    Decompressor decompressor = borrow(DECOMPRESSOR_POOL, codec.getDecompressorType());
    if (decompressor == null) {
      decompressor = codec.createDecompressor();
      LOG.info("Got brand-new decompressor [" + codec.getDefaultExtension() + "]");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got recycled decompressor");
      }
    }
    if (decompressor != null && !decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      updateLeaseCount(decompressorCounts, decompressor, 1);
    }
    return decompressor;
  }

  /**
   * Return the {@link Compressor} to the pool. Copied from hadoop-common without significant
   * modification.
   */
  public static void returnCompressor(Compressor compressor) {
    if (compressor == null) {
      return;
    }
    // if the compressor can't be reused, don't pool it.
    if (compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      compressor.end();
      return;
    }
    compressor.reset();
    if (payback(COMPRESSOR_POOL, compressor)) {
      updateLeaseCount(compressorCounts, compressor, -1);
    }
  }

  /**
   * Return the {@link Decompressor} to the pool. Copied from hadoop-common without significant
   * modification.
   */
  public static void returnDecompressor(Decompressor decompressor) {
    if (decompressor == null) {
      return;
    }
    // if the decompressor can't be reused, don't pool it.
    if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      decompressor.end();
      return;
    }
    decompressor.reset();
    if (payback(DECOMPRESSOR_POOL, decompressor)) {
      updateLeaseCount(decompressorCounts, decompressor, -1);
    }
  }

  /**
   * Returns the number of leased {@link Compressor}s for this {@link CompressionCodec}. Copied from
   * hadoop-common without significant modification.
   */
  static int getLeasedCompressorsCount(@Nullable CompressionCodec codec) {
    if (compressorCounts == null) {
      throw new IllegalStateException("initLeaseCounting() not called to set up lease counting");
    }
    return (codec == null) ? 0 : getLeaseCount(compressorCounts, codec.getCompressorType());
  }

  /**
   * Returns the number of leased {@link Decompressor}s for this {@link CompressionCodec}. Copied
   * from hadoop-common without significant modification.
   */
  static int getLeasedDecompressorsCount(@Nullable CompressionCodec codec) {
    if (decompressorCounts == null) {
      throw new IllegalStateException("initLeaseCounting() not called to set up lease counting");
    }
    return (codec == null) ? 0 : getLeaseCount(decompressorCounts, codec.getDecompressorType());
  }
}
