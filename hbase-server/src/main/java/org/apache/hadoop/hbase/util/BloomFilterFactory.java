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
package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterBase;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterWriter;
import org.apache.hadoop.hbase.io.hfile.CompoundRibbonFilter;
import org.apache.hadoop.hbase.io.hfile.CompoundRibbonFilterBase;
import org.apache.hadoop.hbase.io.hfile.CompoundRibbonFilterWriter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.BloomFilterImpl;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.ribbon.RibbonFilterUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Bloom filter initialization based on configuration and serialized metadata in the reader
 * and writer of {@link org.apache.hadoop.hbase.regionserver.HStoreFile}.
 */
@InterfaceAudience.Private
public final class BloomFilterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BloomFilterFactory.class.getName());

  /** This class should not be instantiated. */
  private BloomFilterFactory() {
  }

  /**
   * Specifies the target error rate to use when selecting the number of keys per Bloom filter.
   */
  public static final String IO_STOREFILE_BLOOM_ERROR_RATE = "io.storefile.bloom.error.rate";

  /**
   * Maximum folding factor allowed. The Bloom filter will be shrunk by the factor of up to 2 **
   * this times if we oversize it initially.
   */
  public static final String IO_STOREFILE_BLOOM_MAX_FOLD = "io.storefile.bloom.max.fold";

  /** Master switch to enable Bloom filters */
  public static final String IO_STOREFILE_BLOOM_ENABLED = "io.storefile.bloom.enabled";

  /** Master switch to enable Delete Family Bloom filters */
  public static final String IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED =
    "io.storefile.delete.family.bloom.enabled";

  /**
   * Target Bloom block size. Bloom filter blocks of approximately this size are interleaved with
   * data blocks.
   */
  public static final String IO_STOREFILE_BLOOM_BLOCK_SIZE = "io.storefile.bloom.block.size";

  /** Maximum number of times a Bloom filter can be "folded" if oversized */
  private static final int MAX_ALLOWED_FOLD_FACTOR = 7;

  /**
   * Instantiates the correct Bloom filter class based on the version provided in the meta block
   * data.
   * @param meta   the byte array holding the Bloom filter's metadata, including version information
   * @param reader the {@link HFile} reader to use to lazily load Bloom filter blocks
   * @return an instance of the correct type of Bloom filter
   */
  public static BloomFilter createFromMeta(DataInput meta, HFile.Reader reader)
    throws IllegalArgumentException, IOException {
    return createFromMeta(meta, reader, null);
  }

  public static BloomFilter createFromMeta(DataInput meta, HFile.Reader reader,
    BloomFilterMetrics metrics) throws IllegalArgumentException, IOException {
    int version = meta.readInt();
    return switch (version) {
      case CompoundBloomFilterBase.VERSION -> new CompoundBloomFilter(meta, reader, metrics);
      case CompoundRibbonFilterBase.VERSION -> new CompoundRibbonFilter(meta, reader, metrics);
      default -> throw new IllegalArgumentException("Bad bloom filter format version " + version);
    };
  }

  /**
   * Returns true if general Bloom (Row or RowCol) filters are enabled in the given configuration
   */
  public static boolean isGeneralBloomEnabled(Configuration conf) {
    return conf.getBoolean(IO_STOREFILE_BLOOM_ENABLED, true);
  }

  /** Returns true if Delete Family Bloom filters are enabled in the given configuration */
  public static boolean isDeleteFamilyBloomEnabled(Configuration conf) {
    return conf.getBoolean(IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED, true);
  }

  /** Returns the Bloom filter error rate in the given configuration */
  public static float getErrorRate(Configuration conf) {
    return conf.getFloat(IO_STOREFILE_BLOOM_ERROR_RATE, (float) 0.01);
  }

  /**
   * Returns the adjusted error rate for the given bloom type. In case of row/column bloom filter
   * lookups, each lookup is an OR of two separate lookups. Therefore, if each lookup's false
   * positive rate is p, the resulting false positive rate is err = 1 - (1 - p)^2, and p = 1 -
   * sqrt(1 - err).
   */
  private static double getAdjustedErrorRate(Configuration conf, BloomType bloomType) {
    double err = getErrorRate(conf);
    if (bloomType == BloomType.ROWCOL) {
      err = 1 - Math.sqrt(1 - err);
    }
    return err;
  }

  /** Returns the value for Bloom filter max fold in the given configuration */
  public static int getMaxFold(Configuration conf) {
    return conf.getInt(IO_STOREFILE_BLOOM_MAX_FOLD, MAX_ALLOWED_FOLD_FACTOR);
  }

  /** Returns the compound Bloom filter block size from the configuration */
  public static int getBloomBlockSize(Configuration conf) {
    return conf.getInt(IO_STOREFILE_BLOOM_BLOCK_SIZE, 128 * 1024);
  }

  /**
   * Creates a new general (Row or RowCol) Bloom or Ribbon filter at the time of
   * {@link org.apache.hadoop.hbase.regionserver.HStoreFile} writing.
   * @param bloomImpl The filter implementation (BLOOM or RIBBON)
   * @param writer    the HFile writer
   * @return the new Bloom/Ribbon filter, or null in case filters are disabled or when failed to
   *         create one.
   */
  public static BloomFilterWriter createGeneralBloomAtWrite(Configuration conf,
    CacheConfig cacheConf, BloomType bloomType, BloomFilterImpl bloomImpl, HFile.Writer writer) {
    if (!isGeneralBloomEnabled(conf)) {
      LOG.trace("Bloom filters are disabled by configuration for {}", writer.getPath());
      return null;
    } else if (bloomType == BloomType.NONE) {
      LOG.trace("Bloom filter is turned off for the column family");
      return null;
    }

    return switch (bloomImpl) {
      case RIBBON -> createRibbonFilterAtWrite(conf, cacheConf, bloomType, writer);
      default -> createTraditionalBloomFilterAtWrite(conf, cacheConf, bloomType, writer);
    };
  }

  /**
   * Creates a new traditional Bloom filter at the time of HStoreFile writing.
   * @param conf      Configuration
   * @param cacheConf Cache configuration
   * @param bloomType The bloom type (ROW, ROWCOL, etc.)
   * @param writer    The HFile writer
   * @return The new Bloom filter writer
   */
  private static BloomFilterWriter createTraditionalBloomFilterAtWrite(Configuration conf,
    CacheConfig cacheConf, BloomType bloomType, HFile.Writer writer) {
    float err = (float) getAdjustedErrorRate(conf, bloomType);
    int maxFold = conf.getInt(IO_STOREFILE_BLOOM_MAX_FOLD, MAX_ALLOWED_FOLD_FACTOR);

    // In case of compound Bloom filters we ignore the maxKeys hint.
    CompoundBloomFilterWriter bloomWriter = new CompoundBloomFilterWriter(getBloomBlockSize(conf),
      err, Hash.getHashType(conf), maxFold, cacheConf.shouldCacheBloomsOnWrite(),
      bloomType == BloomType.ROWCOL ? CellComparatorImpl.COMPARATOR : null, bloomType);
    writer.addInlineBlockWriter(bloomWriter);
    return bloomWriter;
  }

  /**
   * Creates a new Ribbon filter at the time of HStoreFile writing.
   * @param conf      Configuration
   * @param cacheConf Cache configuration
   * @param bloomType The bloom type for key extraction (ROW, ROWCOL, etc.)
   * @param writer    The HFile writer
   * @return The new Ribbon filter writer
   */
  private static BloomFilterWriter createRibbonFilterAtWrite(Configuration conf,
    CacheConfig cacheConf, BloomType bloomType, HFile.Writer writer) {
    int blockSize = getBloomBlockSize(conf);
    int hashType = Hash.getHashType(conf);
    double fpRate = getAdjustedErrorRate(conf, bloomType);

    CellComparator comparator =
      bloomType == BloomType.ROWCOL ? CellComparatorImpl.COMPARATOR : null;

    CompoundRibbonFilterWriter ribbonWriter =
      new CompoundRibbonFilterWriter(blockSize, RibbonFilterUtil.DEFAULT_BANDWIDTH, hashType,
        cacheConf.shouldCacheBloomsOnWrite(), comparator, bloomType, fpRate);

    writer.addInlineBlockWriter(ribbonWriter);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Created Ribbon filter for {} with blockSize={}, fpRate={}", writer.getPath(),
        blockSize, fpRate);
    }

    return ribbonWriter;
  }

  /**
   * Creates a new Delete Family Bloom or Ribbon filter at the time of
   * {@link org.apache.hadoop.hbase.regionserver.HStoreFile} writing.
   * <p>
   * If the bloom filter implementation is RIBBON, the delete family filter will also use Ribbon.
   * Otherwise, a traditional Bloom filter is created.
   * @param conf      Configuration
   * @param cacheConf Cache configuration
   * @param bloomImpl The filter implementation (BLOOM or RIBBON)
   * @param writer    the HFile writer
   * @return the new Bloom/Ribbon filter, or null in case filters are disabled or when failed to
   *         create one.
   */
  public static BloomFilterWriter createDeleteBloomAtWrite(Configuration conf,
    CacheConfig cacheConf, BloomFilterImpl bloomImpl, HFile.Writer writer) {
    if (!isDeleteFamilyBloomEnabled(conf)) {
      LOG.info("Delete Bloom filters are disabled by configuration for {}", writer.getPath());
      return null;
    }

    return switch (bloomImpl) {
      case RIBBON -> createDeleteRibbonAtWrite(conf, cacheConf, writer);
      default -> createTraditionalDeleteBloomAtWrite(conf, cacheConf, writer);
    };
  }

  /**
   * Creates a new traditional Delete Family Bloom filter at the time of HStoreFile writing.
   * @param conf      Configuration
   * @param cacheConf Cache configuration
   * @param writer    The HFile writer
   * @return The new Bloom filter writer
   */
  private static BloomFilterWriter createTraditionalDeleteBloomAtWrite(Configuration conf,
    CacheConfig cacheConf, HFile.Writer writer) {
    float err = getErrorRate(conf);
    int maxFold = getMaxFold(conf);

    CompoundBloomFilterWriter bloomWriter =
      new CompoundBloomFilterWriter(getBloomBlockSize(conf), err, Hash.getHashType(conf), maxFold,
        cacheConf.shouldCacheBloomsOnWrite(), null, BloomType.ROW);
    writer.addInlineBlockWriter(bloomWriter);
    return bloomWriter;
  }

  /**
   * Creates a new Delete Family Ribbon filter at the time of HStoreFile writing.
   * @param conf      Configuration
   * @param cacheConf Cache configuration
   * @param writer    The HFile writer
   * @return The new Ribbon filter writer
   */
  private static BloomFilterWriter createDeleteRibbonAtWrite(Configuration conf,
    CacheConfig cacheConf, HFile.Writer writer) {
    int blockSize = getBloomBlockSize(conf);
    int hashType = Hash.getHashType(conf);
    double fpRate = getErrorRate(conf);

    CompoundRibbonFilterWriter ribbonWriter =
      new CompoundRibbonFilterWriter(blockSize, RibbonFilterUtil.DEFAULT_BANDWIDTH, hashType,
        cacheConf.shouldCacheBloomsOnWrite(), null, BloomType.ROW, fpRate);

    writer.addInlineBlockWriter(ribbonWriter);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Created Delete Family Ribbon filter for {} with blockSize={}, fpRate={}",
        writer.getPath(), blockSize, fpRate);
    }

    return ribbonWriter;
  }
}
