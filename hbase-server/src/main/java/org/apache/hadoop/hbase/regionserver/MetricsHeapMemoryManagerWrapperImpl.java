package org.apache.hadoop.hbase.regionserver;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;

@InterfaceAudience.Private
public class MetricsHeapMemoryManagerWrapperImpl
    implements MetricsHeapMemoryManagerWrapper {
  
  private long maxHeapSize = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
  
  private final HRegionServer server;
  private final RegionServerAccounting rsAccounting;
  private final ResizableBlockCache blockCache;
  
  public MetricsHeapMemoryManagerWrapperImpl(final Server server,
      final RegionServerAccounting rsAccounting, ResizableBlockCache blockCache) {
    this.server = (HRegionServer) server;
    this.rsAccounting = rsAccounting;
    this.blockCache = blockCache;
  }

  @Override
  public String getServerName() {
    ServerName serverName = server.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public String getClusterId() {
    return server.getClusterId();
  }

  @Override
  public long getMaxHeap() {
    return maxHeapSize;
  }

  @Override
  public float getHeapUsed() {
    MemoryUsage memUsage = getMemoryUsage();
    return (float) memUsage.getUsed() / (float) memUsage.getCommitted();
  }

  @Override
  public long getHeapUsedSize() {
    return getMemoryUsage().getUsed();
  }

  @Override
  public float getBlockCacheUsed() {
    return (float) (getBlockCacheUsedSize() / maxHeapSize);
  }

  @Override
  public long getBlockCacheUsedSize() {
    return blockCache.getCurrentSize();
  }

  @Override
  public float getMemStoreUsed() {
    return (float) (getMemStoreUsedSize() / maxHeapSize);
  }

  @Override
  public long getMemStoreUsedSize() {
    return rsAccounting.getGlobalMemstoreSize();
  }
  
  private MemoryUsage getMemoryUsage() {
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
  }
}
