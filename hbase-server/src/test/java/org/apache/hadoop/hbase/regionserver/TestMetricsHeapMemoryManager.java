package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test version of rs metrics tests.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestMetricsHeapMemoryManager {
  public static MetricsAssertHelper HELPER = CompatibilitySingletonFactory
      .getInstance(MetricsAssertHelper.class);

  private MetricsHeapMemoryManagerWrapperStub wrapper;
  private MetricsHeapMemoryManager hmm;
  private MetricsHeapMemoryManagerSource source;

  @BeforeClass
  public static void classSetUp() {
    HELPER.init();
  }

  @Before
  public void setUp() {
    wrapper = new MetricsHeapMemoryManagerWrapperStub();
    hmm = new MetricsHeapMemoryManager(wrapper);
    source = hmm.getMetricsSource();
  }

  @Test
  public void testWrapperSource() {
    HELPER.assertTag("serverName", "test", source);
    HELPER.assertTag("clusterId", "tClusterId", source);
    HELPER.assertGauge("maxHeap", 1024, source);
    HELPER.assertGauge("heapUsed", 0.5f, source);
    HELPER.assertGauge("heapUsedSize", 512, source);
    HELPER.assertGauge("blockCacheUsed", 0.25f, source);
    HELPER.assertGauge("blockCacheUsedSize", 256, source);
    HELPER.assertGauge("memStoreUsed", 0.25f, source);
    HELPER.assertGauge("memStoreUsedSize", 256, source);
  }

  @Test
  public void testConstuctor() {
    assertNotNull("There should be a hadoop1/hadoop2 metrics source", hmm.getMetricsSource());
    assertNotNull("The RegionServerMetricsWrapper should be accessable",
      hmm.getHeapMemoryManagerWrapper());
  }

  @Test
  public void testMemoryAndCacheCounter() {
    for (int i = 0; i < 10; i++) {
      hmm.updateCacheEvictedCount(5);
      hmm.updateCacheMissCount(6);
    }
    for (int i = 0; i < 11; i++) {
      hmm.updateBlockedFlushCount(7);
      hmm.updateUnblockedFlushCount(8);
    }
    HELPER.assertCounter("cacheEvictedCount", 10, source);
    HELPER.assertCounter("cacheMissCount", 10, source);
    HELPER.assertCounter("blockedFlushCount", 11, source);
    HELPER.assertCounter("unblockedFlushCount", 11, source);
  }

  @Test
  public void testHeapChange() {
    long HEAP_COMMITED = 2048;
    long MAX_HEAP = 3072;

    // initialization
    hmm.updateHeapOccupancy(0.9f, (long) (HEAP_COMMITED * 0.9));  // 1843
    hmm.updateCurMemStore(0.4f, (long) (MAX_HEAP * 0.4)); // 1229
    hmm.updateCurBlockCache(0.2f, (long) (MAX_HEAP * 0.2)); // 614
    HELPER.assertGauge("curHeapSize", 1843, source);
    HELPER.assertGauge("curMemStoreSize", 1229, source);
    HELPER.assertGauge("curBlockCacheSize", 614, source);
    
    // tuning 1
    hmm.updateDeltaMemStoreSize(-200);
    hmm.updateDeltaBlockCacheSize(200);
    hmm.updateCurMemStore(0.34f, 1229 - 200); // 1029
    hmm.updateCurBlockCache(0.26f, 614 + 200); // 814
    HELPER.assertGauge("curMemStoreSize", 1229, source);
    HELPER.assertGauge("curBlockCacheSize", 614, source);
   
    // tuning 2
    hmm.updateDeltaMemStoreSize(50);
    hmm.updateDeltaBlockCacheSize(-50);
    hmm.updateCurMemStore(0.35f, 1029 + 50); // 1079
    hmm.updateCurBlockCache(0.25f, 814 - 50); // 764
    HELPER.assertGauge("curMemStoreSize", 1079, source);
    HELPER.assertGauge("curBlockCacheSize", 764, source);
  }
}
