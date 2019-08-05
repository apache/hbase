/**
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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceLimitRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

/**
 * Test class for {@link QuotaSettingsFactory}.
 */
@Category(SmallTests.class)
public class TestQuotaSettingsFactory {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaSettingsFactory.class);

  @Test
  public void testAllQuotasAddedToList() {
    final SpaceQuota spaceQuota = SpaceQuota.newBuilder()
        .setSoftLimit(1024L * 1024L * 1024L * 50L) // 50G
        .setViolationPolicy(QuotaProtos.SpaceViolationPolicy.DISABLE) // Disable the table
        .build();
    final long readLimit = 1000;
    final long writeLimit = 500;
    final Throttle throttle = Throttle.newBuilder()
        // 1000 read reqs/min
        .setReadNum(TimedQuota.newBuilder().setSoftLimit(readLimit)
            .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build())
        // 500 write reqs/min
        .setWriteNum(TimedQuota.newBuilder().setSoftLimit(writeLimit)
            .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build())
        .build();
    final Quotas quotas = Quotas.newBuilder()
        .setSpace(spaceQuota) // Set the FS quotas
        .setThrottle(throttle) // Set some RPC limits
        .build();
    final TableName tn = TableName.valueOf("my_table");
    List<QuotaSettings> settings = QuotaSettingsFactory.fromTableQuotas(tn, quotas);
    assertEquals(3, settings.size());
    boolean seenRead = false;
    boolean seenWrite = false;
    boolean seenSpace = false;
    for (QuotaSettings setting : settings) {
      if (setting instanceof ThrottleSettings) {
        ThrottleSettings throttleSettings = (ThrottleSettings) setting;
        switch (throttleSettings.getThrottleType()) {
          case READ_NUMBER:
            assertFalse("Should not have multiple read quotas", seenRead);
            assertEquals(readLimit, throttleSettings.getSoftLimit());
            assertEquals(TimeUnit.MINUTES, throttleSettings.getTimeUnit());
            assertEquals(tn, throttleSettings.getTableName());
            assertNull("Username should be null", throttleSettings.getUserName());
            assertNull("Namespace should be null", throttleSettings.getNamespace());
            assertNull("RegionServer should be null", throttleSettings.getRegionServer());
            seenRead = true;
            break;
          case WRITE_NUMBER:
            assertFalse("Should not have multiple write quotas", seenWrite);
            assertEquals(writeLimit, throttleSettings.getSoftLimit());
            assertEquals(TimeUnit.MINUTES, throttleSettings.getTimeUnit());
            assertEquals(tn, throttleSettings.getTableName());
            assertNull("Username should be null", throttleSettings.getUserName());
            assertNull("Namespace should be null", throttleSettings.getNamespace());
            assertNull("RegionServer should be null", throttleSettings.getRegionServer());
            seenWrite = true;
            break;
          default:
            fail("Unexpected throttle type: " + throttleSettings.getThrottleType());
        }
      } else if (setting instanceof SpaceLimitSettings) {
        assertFalse("Should not have multiple space quotas", seenSpace);
        SpaceLimitSettings spaceLimit = (SpaceLimitSettings) setting;
        assertEquals(tn, spaceLimit.getTableName());
        assertNull("Username should be null", spaceLimit.getUserName());
        assertNull("Namespace should be null", spaceLimit.getNamespace());
        assertNull("RegionServer should be null", spaceLimit.getRegionServer());
        assertTrue("SpaceLimitSettings should have a SpaceQuota", spaceLimit.getProto().hasQuota());
        assertEquals(spaceQuota, spaceLimit.getProto().getQuota());
        seenSpace = true;
      } else {
        fail("Unexpected QuotaSettings implementation: " + setting.getClass());
      }
    }
    assertTrue("Should have seen a read quota", seenRead);
    assertTrue("Should have seen a write quota", seenWrite);
    assertTrue("Should have seen a space quota", seenSpace);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNeitherTableNorNamespace() {
    final SpaceQuota spaceQuota = SpaceQuota.newBuilder()
        .setSoftLimit(1L)
        .setViolationPolicy(QuotaProtos.SpaceViolationPolicy.DISABLE)
        .build();
    QuotaSettingsFactory.fromSpace(null, null, spaceQuota);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBothTableAndNamespace() {
    final SpaceQuota spaceQuota = SpaceQuota.newBuilder()
        .setSoftLimit(1L)
        .setViolationPolicy(QuotaProtos.SpaceViolationPolicy.DISABLE)
        .build();
    QuotaSettingsFactory.fromSpace(TableName.valueOf("foo"), "bar", spaceQuota);
  }

  @Test
  public void testSpaceLimitSettings() {
    final TableName tableName = TableName.valueOf("foo");
    final long sizeLimit = 1024L * 1024L * 1024L * 75; // 75GB
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings settings =
        QuotaSettingsFactory.limitTableSpace(tableName, sizeLimit, violationPolicy);
    assertNotNull("QuotaSettings should not be null", settings);
    assertTrue("Should be an instance of SpaceLimitSettings",
        settings instanceof SpaceLimitSettings);
    SpaceLimitSettings spaceLimitSettings = (SpaceLimitSettings) settings;
    SpaceLimitRequest protoRequest = spaceLimitSettings.getProto();
    assertTrue("Request should have a SpaceQuota", protoRequest.hasQuota());
    SpaceQuota quota = protoRequest.getQuota();
    assertEquals(sizeLimit, quota.getSoftLimit());
    assertEquals(violationPolicy, ProtobufUtil.toViolationPolicy(quota.getViolationPolicy()));
    assertFalse("The remove attribute should be false", quota.getRemove());
  }

  @Test
  public void testSpaceLimitSettingsForDeletes() {
    final String ns = "ns1";
    final TableName tn = TableName.valueOf("tn1");
    QuotaSettings nsSettings = QuotaSettingsFactory.removeNamespaceSpaceLimit(ns);
    assertNotNull("QuotaSettings should not be null", nsSettings);
    assertTrue("Should be an instance of SpaceLimitSettings",
        nsSettings instanceof SpaceLimitSettings);
    SpaceLimitRequest nsProto = ((SpaceLimitSettings) nsSettings).getProto();
    assertTrue("Request should have a SpaceQuota", nsProto.hasQuota());
    assertTrue("The remove attribute should be true", nsProto.getQuota().getRemove());

    QuotaSettings tableSettings = QuotaSettingsFactory.removeTableSpaceLimit(tn);
    assertNotNull("QuotaSettings should not be null", tableSettings);
    assertTrue("Should be an instance of SpaceLimitSettings",
        tableSettings instanceof SpaceLimitSettings);
    SpaceLimitRequest tableProto = ((SpaceLimitSettings) tableSettings).getProto();
    assertTrue("Request should have a SpaceQuota", tableProto.hasQuota());
    assertTrue("The remove attribute should be true", tableProto.getQuota().getRemove());
  }
}
