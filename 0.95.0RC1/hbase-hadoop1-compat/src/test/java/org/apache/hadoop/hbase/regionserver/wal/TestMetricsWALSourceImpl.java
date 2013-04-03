package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestMetricsWALSourceImpl {

  @Test
  public void testGetInstance() throws Exception {
    MetricsWALSource walSource =
        CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    assertTrue(walSource instanceof MetricsWALSourceImpl);
    assertSame(walSource,
        CompatibilitySingletonFactory.getInstance(MetricsWALSource.class));
  }
}
