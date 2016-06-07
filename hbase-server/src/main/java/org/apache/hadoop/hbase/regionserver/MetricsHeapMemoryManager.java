package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHeapMemoryManager {
  private final MetricsHeapMemoryManagerSource source;
  private MetricsHeapMemoryManagerWrapper wrapper;

  public MetricsHeapMemoryManager(MetricsHeapMemoryManagerWrapper wrapper) {
    this(wrapper, CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
                                                                             .getHeapMemoryManager(wrapper));
  }
  
  public MetricsHeapMemoryManager(MetricsHeapMemoryManagerWrapper wrapper,
      MetricsHeapMemoryManagerSource source) {
    this.wrapper = wrapper;
    this.source = source;
  }
  
  public MetricsHeapMemoryManagerSource getMetricsSource() {
    return source;
  }
  
  public MetricsHeapMemoryManagerWrapper getHeapMemoryManagerWrapper() {
    return wrapper;
  }

  public void updateHeapOccupancy(final float heapOccupancyPercent, final long heapUsed) {
    source.updateHeapOccupancy(heapOccupancyPercent);
    source.updateCurHeapSize(heapUsed);
  }

  public void updateCacheEvictedCount(final long cacheEvictedCount) {
    source.updateCacheEvictCount(cacheEvictedCount);
  }

  public void updateCacheMissCount(final long cacheMissCount) {
    source.updateCacheMissCount(cacheMissCount);
  }

  public void updateBlockedFlushCount(final long bFlushCount) {
    source.updateBlockedFlushCount(bFlushCount);
  }

  public void updateUnblockedFlushCount(final long unbFlushCount) {
    source.updateUnblockedFlushCount(unbFlushCount);
  }

  public void updateCurBlockCache(final float curBlockCacheUsed, final long blockcacheSize) {
    source.updateCurBlockCachePercentage(curBlockCacheUsed);
    source.updateCurBlockCacheSize(blockcacheSize);
  }

  public void updateCurMemStore(final float curMemStoreUsed, final long memstoreSize) {
    source.updateCurMemStorePercentage(curMemStoreUsed);
    source.updateCurMemStoreSize(memstoreSize);
  }

  public void updateDeltaMemStoreSize(final int deltaMemStoreSize) {
    source.updateDeltaMemStoreSize(deltaMemStoreSize);
  }

  public void updateDeltaBlockCacheSize(final int deltaBlockCacheSize) {
    source.updateDeltaBlockCacheSize(deltaBlockCacheSize);
  }
}
