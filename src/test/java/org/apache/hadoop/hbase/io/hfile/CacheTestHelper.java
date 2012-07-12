package org.apache.hadoop.hbase.io.hfile;

import java.lang.reflect.Method;

public class CacheTestHelper {
  public static void forceDelayedEviction(BlockCache bc) throws Exception{
    if (bc instanceof LruBlockCache){
      LruBlockCache lbc = (LruBlockCache)bc;
      Method doDelayedEvictionMethod = LruBlockCache.class.getDeclaredMethod("doDelayedEviction");
      doDelayedEvictionMethod.setAccessible(true);
      doDelayedEvictionMethod.invoke(lbc);
    }
  }
  

}
