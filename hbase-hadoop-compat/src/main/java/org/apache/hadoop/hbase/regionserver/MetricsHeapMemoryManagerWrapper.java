package org.apache.hadoop.hbase.regionserver;

public interface MetricsHeapMemoryManagerWrapper {
  
  /**
   * Get ServerName
   */
  String getServerName();

  /**
   * Get the Cluster ID
   */
  String getClusterId();
  
  /**
   * Get Max heap size
   */
  long getMaxHeap();
  
  /**
   * Get Heap Used in percentage
   */
  float getHeapUsed();
  
  /**
   * Get Heap Used in size
   */
  long getHeapUsedSize();
  
  /**
   * Get BlockCache used in percentage
   */
  float getBlockCacheUsed();
  
  /**
   * Get BlockCached used in size
   */
  long getBlockCacheUsedSize();
  
  /**
   * Get MemStore used in percentage
   */
  float getMemStoreUsed();
  
  /**
   * Get MemStore used in size
   */
  long getMemStoreUsedSize();
}
