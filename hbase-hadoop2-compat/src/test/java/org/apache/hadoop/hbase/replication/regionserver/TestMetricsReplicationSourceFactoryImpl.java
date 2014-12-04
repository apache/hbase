package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMetricsReplicationSourceFactoryImpl {


  @Test
  public void testGetInstance() throws Exception {
    MetricsReplicationSourceFactory rms = CompatibilitySingletonFactory
        .getInstance(MetricsReplicationSourceFactory.class);
    assertTrue(rms instanceof MetricsReplicationSourceFactoryImpl);
  }

}