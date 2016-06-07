package org.apache.hadoop.hbase.regionserver;

public class MetricsHeapMemoryManagerWrapperStub implements MetricsHeapMemoryManagerWrapper {

  @Override
  public String getServerName() {
    return "test";
  }

  @Override
  public String getClusterId() {
    return "tClusterId";
  }

  @Override
  public long getMaxHeap() {
    return 1024;
  }

  @Override
  public float getHeapUsed() {
    return 0.5f;
  }

  @Override
  public long getHeapUsedSize() {
    return 512;
  }

  @Override
  public float getBlockCacheUsed() {
    return 0.25f;
  }

  @Override
  public long getBlockCacheUsedSize() {
    return 256;
  }

  @Override
  public float getMemStoreUsed() {
    return 0.25f;
  }

  @Override
  public long getMemStoreUsedSize() {
    return 256;
  }
}
