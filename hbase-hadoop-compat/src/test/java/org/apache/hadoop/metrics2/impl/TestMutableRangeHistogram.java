package org.apache.hadoop.metrics2.impl;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableRangeHistogram;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

@Category({ MetricsTests.class, SmallTests.class})
public class TestMutableRangeHistogram {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMutableRangeHistogram.class);

    private static final String RECORD_NAME = "test";
    private static final String SIZE_METRIC_NAME = "TestSize";
    private static final String TIME_METRIC_NAME = "TestTime";

    @Test
    public void testMutableSizeHistogram() {
        testMutableRangeHistogram(SIZE_METRIC_NAME, new MutableSizeHistogram(SIZE_METRIC_NAME, ""));
    }

    @Test
    public void testMutableTimeHistogram() {
        testMutableRangeHistogram(TIME_METRIC_NAME, new MutableTimeHistogram(TIME_METRIC_NAME, ""));
    }

    private static void testMutableRangeHistogram(String metricName, MutableRangeHistogram histogram) {
        // fill up values
        long[] ranges = histogram.getRanges();
        for (long val : ranges) {
            histogram.add(val - 1);
        }
        histogram.add(ranges[ranges.length - 1] + 1);
        // put into metrics collector
        MetricsCollectorImpl collector = new MetricsCollectorImpl();
        MetricsRecordBuilder builder = collector.addRecord(RECORD_NAME);
        histogram.snapshot(builder, true);
        // get and print
        Collection<MetricsRecordImpl> records = collector.getRecords();
        assertEquals(records.size(), 1);
        MetricsRecordImpl record = records.iterator().next();
        assertEquals(record.name(), RECORD_NAME);
        String prefix = metricName + "_" + histogram.getRangeType();
        int count = 0;
        for (AbstractMetric metric : record.metrics()) {
            if (metric.name().startsWith(prefix)) {
                System.out.println(metric.name() + ": " + metric.value());
                count++;
            }
        }
        assertEquals(count, ranges.length + 1);
    }
}
