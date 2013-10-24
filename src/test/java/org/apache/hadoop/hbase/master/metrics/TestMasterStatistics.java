/**
 *
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
package org.apache.hadoop.hbase.master.metrics;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests {@link MasterMetrics} and access to it through the
 * {@link MasterStatistics} management bean.
 */
@Category(SmallTests.class)
public class TestMasterStatistics {

  @Test
  public void testMasterStatistics() throws Exception {
    // No timer updates started here since NullContext is
    // instantiated by default for HBase:
    MasterMetrics masterMetrics = new MasterMetrics("foo");
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final ObjectName objectName = new ObjectName(
          "hadoop:name=MasterStatistics,service=Master");

      masterMetrics.doUpdates(null);

      masterMetrics.resetAllMinMax();

      masterMetrics.incrementRequests(10);
      Thread.sleep(1001);

      masterMetrics.addSnapshot(1L);
      masterMetrics.addSnapshotClone(2L);
      masterMetrics.addSnapshotRestore(3L);

      // 3 times added split, average = (5+3+4)/3 = 4
      masterMetrics.addSplit(4L, 5L);
      masterMetrics.addSplit(2L, 3L);
      masterMetrics.addSplit(13L, 4L);

      masterMetrics.doUpdates(null);

      final float f = masterMetrics.getRequests();
      // f = 10/T, where T >= 1 sec. So, we assert that 0 < f <= 10:
      if (f <= 0.0f || f > 10.0f) {
        fail("Unexpected rate value: " + f);
      }
      Object attribute = server.getAttribute(objectName, "cluster_requests");
      float f2 = ((Float) attribute).floatValue();
      assertEquals("The value obtained through bean server should be equal to the one " +
          "obtained directly.", f, f2, 1e-4);

      // NB: these 3 metrics are not pushed upon masterMetrics.doUpdates(),
      // so they always return null:
      attribute = server.getAttribute(objectName, "snapshotTimeNumOps");
      assertEquals(Integer.valueOf(0), attribute);
      attribute = server.getAttribute(objectName, "snapshotRestoreTimeNumOps");
      assertEquals(Integer.valueOf(0), attribute);
      attribute = server.getAttribute(objectName, "snapshotCloneTimeNumOps");
      assertEquals(Integer.valueOf(0), attribute);

      attribute = server.getAttribute(objectName, "splitSizeNumOps");
      assertEquals(Integer.valueOf(3), attribute);
      attribute = server.getAttribute(objectName, "splitSizeAvgTime");
      assertEquals(Long.valueOf(4), attribute);
    } finally {
      masterMetrics.shutdown();
    }
  }

  @Test
  public void testHBaseInfoBean() throws Exception {
    MasterMetrics masterMetrics = new MasterMetrics("foo");
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      // Test Info bean:
      final ObjectName objectName2 = new ObjectName(
          "hadoop:name=Info,service=HBase");
      Object attribute;
      attribute = server.getAttribute(objectName2, "revision");
      assertNotNull(attribute);
      attribute = server.getAttribute(objectName2, "version");
      assertNotNull(attribute);
      attribute = server.getAttribute(objectName2, "hdfsUrl");
      assertNotNull(attribute);
      attribute = server.getAttribute(objectName2, "user");
      assertNotNull(attribute);
    } finally {
      masterMetrics.shutdown();
    }
  }
}
