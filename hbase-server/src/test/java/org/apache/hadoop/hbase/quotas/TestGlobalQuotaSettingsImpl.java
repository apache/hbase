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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

@Category(SmallTests.class)
public class TestGlobalQuotaSettingsImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGlobalQuotaSettingsImpl.class);

  QuotaProtos.TimedQuota REQUEST_THROTTLE = QuotaProtos.TimedQuota.newBuilder()
      .setScope(QuotaProtos.QuotaScope.MACHINE).setSoftLimit(100)
      .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build();
  QuotaProtos.Throttle THROTTLE = QuotaProtos.Throttle.newBuilder()
      .setReqNum(REQUEST_THROTTLE).build();

  QuotaProtos.SpaceQuota SPACE_QUOTA = QuotaProtos.SpaceQuota.newBuilder()
      .setSoftLimit(1024L * 1024L).setViolationPolicy(QuotaProtos.SpaceViolationPolicy.NO_WRITES)
      .build();

  @Test
  public void testMergeThrottle() throws IOException {
    QuotaProtos.Quotas quota = QuotaProtos.Quotas.newBuilder()
        .setThrottle(THROTTLE).build();
    QuotaProtos.TimedQuota writeQuota = REQUEST_THROTTLE.toBuilder()
        .setSoftLimit(500).build();
    // Unset the req throttle, set a write throttle
    QuotaProtos.ThrottleRequest writeThrottle = QuotaProtos.ThrottleRequest.newBuilder()
        .setTimedQuota(writeQuota).setType(QuotaProtos.ThrottleType.WRITE_NUMBER).build();

    GlobalQuotaSettingsImpl settings = new GlobalQuotaSettingsImpl("joe", null, null, null, quota);
    GlobalQuotaSettingsImpl merged = settings.merge(
      new ThrottleSettings("joe", null, null, null, writeThrottle));

    QuotaProtos.Throttle mergedThrottle = merged.getThrottleProto();
    // Verify the request throttle is in place
    assertTrue(mergedThrottle.hasReqNum());
    QuotaProtos.TimedQuota actualReqNum = mergedThrottle.getReqNum();
    assertEquals(REQUEST_THROTTLE.getSoftLimit(), actualReqNum.getSoftLimit());

    // Verify the write throttle is in place
    assertTrue(mergedThrottle.hasWriteNum());
    QuotaProtos.TimedQuota actualWriteNum = mergedThrottle.getWriteNum();
    assertEquals(writeQuota.getSoftLimit(), actualWriteNum.getSoftLimit());
  }

  @Test
  public void testMergeSpace() throws IOException {
    TableName tn = TableName.valueOf("foo");
    QuotaProtos.Quotas quota = QuotaProtos.Quotas.newBuilder()
        .setSpace(SPACE_QUOTA).build();

    GlobalQuotaSettingsImpl settings = new GlobalQuotaSettingsImpl(null, tn, null, null, quota);
    // Switch the violation policy to DISABLE
    GlobalQuotaSettingsImpl merged = settings.merge(
        new SpaceLimitSettings(tn, SPACE_QUOTA.getSoftLimit(), SpaceViolationPolicy.DISABLE));

    QuotaProtos.SpaceQuota mergedSpaceQuota = merged.getSpaceProto();
    assertEquals(SPACE_QUOTA.getSoftLimit(), mergedSpaceQuota.getSoftLimit());
    assertEquals(
        QuotaProtos.SpaceViolationPolicy.DISABLE, mergedSpaceQuota.getViolationPolicy());
  }

  @Test
  public void testMergeThrottleAndSpace() throws IOException {
    final String ns = "org1";
    QuotaProtos.Quotas quota = QuotaProtos.Quotas.newBuilder()
        .setThrottle(THROTTLE).setSpace(SPACE_QUOTA).build();
    GlobalQuotaSettingsImpl settings = new GlobalQuotaSettingsImpl(null, null, ns, null, quota);

    QuotaProtos.TimedQuota writeQuota = REQUEST_THROTTLE.toBuilder()
        .setSoftLimit(500).build();
    // Add a write throttle
    QuotaProtos.ThrottleRequest writeThrottle = QuotaProtos.ThrottleRequest.newBuilder()
        .setTimedQuota(writeQuota).setType(QuotaProtos.ThrottleType.WRITE_NUMBER).build();

    GlobalQuotaSettingsImpl merged = settings.merge(
      new ThrottleSettings(null, null, ns, null, writeThrottle));
    GlobalQuotaSettingsImpl finalQuota = merged.merge(new SpaceLimitSettings(
        ns, SPACE_QUOTA.getSoftLimit(), SpaceViolationPolicy.NO_WRITES_COMPACTIONS));

    // Verify both throttle quotas
    QuotaProtos.Throttle throttle = finalQuota.getThrottleProto();
    assertTrue(throttle.hasReqNum());
    QuotaProtos.TimedQuota reqNumQuota = throttle.getReqNum();
    assertEquals(REQUEST_THROTTLE.getSoftLimit(), reqNumQuota.getSoftLimit());

    assertTrue(throttle.hasWriteNum());
    QuotaProtos.TimedQuota writeNumQuota = throttle.getWriteNum();
    assertEquals(writeQuota.getSoftLimit(), writeNumQuota.getSoftLimit());

    // Verify space quota
    QuotaProtos.SpaceQuota finalSpaceQuota = finalQuota.getSpaceProto();
    assertEquals(SPACE_QUOTA.getSoftLimit(), finalSpaceQuota.getSoftLimit());
    assertEquals(
        QuotaProtos.SpaceViolationPolicy.NO_WRITES_COMPACTIONS,
        finalSpaceQuota.getViolationPolicy());
  }
}
