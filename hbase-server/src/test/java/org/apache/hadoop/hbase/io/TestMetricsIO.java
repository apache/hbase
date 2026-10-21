/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import java.lang.management.ManagementFactory;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
@Tag(MiscTests.TAG)
public class TestMetricsIO {

  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testMetrics() throws Exception {
    MetricsIO metrics = new MetricsIO(new MetricsIOWrapper() {
      @Override
      public long getChecksumFailures() {
        return 40;
      }
    });

    metrics.updateFsReadTime(100);
    metrics.updateFsReadTime(200);

    metrics.updateFsPreadTime(300);

    metrics.updateFsWriteTime(400);
    metrics.updateFsWriteTime(500);
    metrics.updateFsWriteTime(600);

    long reads = HELPER.getCounter(MetricsIOSource.STORE_FILE_TRACKER_FILE_READ_ATTEMPTS_KEY,
      metrics.getMetricsSource());
    long writes = HELPER.getCounter(MetricsIOSource.STORE_FILE_TRACKER_FILE_WRITE_ATTEMPTS_KEY,
      metrics.getMetricsSource());
    metrics.incrStoreFileTrackerFileReadAttempts();
    metrics.incrStoreFileTrackerFileReadAttempts();
    metrics.incrStoreFileTrackerFileWriteAttempts();
    HELPER.assertCounter(MetricsIOSource.STORE_FILE_TRACKER_FILE_READ_ATTEMPTS_KEY, reads + 2,
      metrics.getMetricsSource());
    HELPER.assertCounter(MetricsIOSource.STORE_FILE_TRACKER_FILE_WRITE_ATTEMPTS_KEY, writes + 1,
      metrics.getMetricsSource());

    HELPER.assertCounter("fsChecksumFailureCount", 40, metrics.getMetricsSource());

    HELPER.assertCounter("fsReadTime_numOps", 2, metrics.getMetricsSource());
    HELPER.assertCounter("fsPReadTime_numOps", 1, metrics.getMetricsSource());
    HELPER.assertCounter("fsWriteTime_numOps", 3, metrics.getMetricsSource());

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name =
      new ObjectName("Hadoop:service=HBase,name=" + MetricsIOSource.METRICS_JMX_CONTEXT);
    // JMX can cache the snapshot taken while the source is being registered.
    new HBaseCommonTestingUtil().waitFor(15000, () -> {
      try {
        return (long) server.getAttribute(name, "StoreFileTrackerFileReadAttempts") == reads + 2
          && (long) server.getAttribute(name, "StoreFileTrackerFileWriteAttempts") == writes + 1;
      } catch (AttributeNotFoundException e) {
        return false;
      }
    });
  }
}
