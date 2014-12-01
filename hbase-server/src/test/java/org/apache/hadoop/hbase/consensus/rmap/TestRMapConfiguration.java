package org.apache.hadoop.hbase.consensus.rmap;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Before;
import org.junit.Test;

public class TestRMapConfiguration {
  private Configuration conf;
  private RMapConfiguration rmapConf;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.RMAP_SUBSCRIPTION,
            getClass().getResource("rmap.json").toURI().toString());
    conf.set(HConstants.HYDRABASE_DCNAME, "DUMMYCLUSTER1");

    rmapConf = new RMapConfiguration(conf);
    URI uri = RMapConfiguration.getRMapSubscription(conf);
    rmapConf.readRMap(uri);
    rmapConf.appliedRMap(uri);
  }

  @Test
  public void testReadingEmptyRMapSubscription() {
    conf.set(HConstants.RMAP_SUBSCRIPTION, "");
    assertNull("RMap subscription should be empty",
      rmapConf.getRMapSubscription(conf));
  }

  @Test
  public void testReadingNonEmptyRMapSubscription()
          throws URISyntaxException {
    conf.set(HConstants.RMAP_SUBSCRIPTION,
            "hbase/rmaps/map1");
    URI expectedRMapSubscription = new URI("hbase/rmaps/map1");
    assertEquals(expectedRMapSubscription,
            rmapConf.getRMapSubscription(conf));
  }

  @Test
  public void shouldApplyRMap() {
    URI subscription = RMapConfiguration.getRMapSubscription(conf);
    assertTrue(rmapConf.isRMapApplied(subscription));
  }
}
