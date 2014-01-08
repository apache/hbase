package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;

/**
 * {@link L2CacheAgent} wraps around a {@link CacheConfig} and {@link L2Cache}
 * instance and is {@link SchemaConfigured}. As such it will make sure schema
 * data is added to blocks going into the cache, allowing the appropriate
 * metrics to be updated throughout the blocks lifecycle. Further more
 * {@link L2CacheAgent} would allow for making schema aware cache decisions,
 * removing the need for individual block writers to deal with cache policies.
 */
public class L2CacheAgent extends SchemaConfigured {
  private CacheConfig cacheConfig;
  private L2Cache l2Cache;

  public L2CacheAgent(final CacheConfig cacheConfig,
                      final L2Cache l2Cache) {
    this.cacheConfig = cacheConfig;
    this.l2Cache = l2Cache;
  }

  public byte[] getRawBlockBytes(BlockCacheKey cacheKey) {
    return isL2CacheEnabled() ? l2Cache.getRawBlockBytes(cacheKey) : null;
  }

  public void cacheRawBlock(BlockCacheKey cacheKey, RawHFileBlock rawBlock) {
    if (isL2CacheEnabled()) {
      passSchemaMetricsTo(rawBlock);
      l2Cache.cacheRawBlock(cacheKey, rawBlock);
    }
  }

  public boolean evictRawBlock(BlockCacheKey cacheKey) {
    return isL2CacheEnabled() && l2Cache.evictRawBlock(cacheKey);
  }

  public int evictBlocksByHfileName(String hfileName, boolean isClose) {
    // If the HFile was closed but the policy is not to evict on close, skip
    // the eviction.
    if (isClose && !cacheConfig.shouldL2EvictOnClose()) return 0;
    return isL2CacheEnabled() ?
            l2Cache.evictBlocksByHfileName(hfileName) : 0;
  }

  public boolean isL2CacheEnabled() {
    return l2Cache != null && l2Cache.isEnabled();
  }
}
