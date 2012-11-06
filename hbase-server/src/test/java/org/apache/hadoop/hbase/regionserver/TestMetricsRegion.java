package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetricsRegion {


  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testRegionWrapperMetrics() {
    MetricsRegion mr = new MetricsRegion(new MetricsRegionWrapperStub());
    MetricsRegionAggregateSource agg = mr.getSource().getAggregateSource();

    HELPER.assertGauge("table.MetricsRegionWrapperStub.region.DEADBEEF001.storeCount", 101, agg);
    HELPER.assertGauge("table.MetricsRegionWrapperStub.region.DEADBEEF001.storeFileCount", 102, agg);
    HELPER.assertGauge("table.MetricsRegionWrapperStub.region.DEADBEEF001.memstoreSize", 103, agg);
    mr.close();
  }
}
