package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.junit.Test;

public class TestMetricsHLogSource {

  @Test(expected=RuntimeException.class)
  public void testGetInstanceNoHadoopCompat() throws Exception {
    //This should throw an exception because there is no compat lib on the class path.
    CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);

  }
}
