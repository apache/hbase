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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

@Category({SmallTests.class})
public class TestThrottleSettings {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThrottleSettings.class);

  @Test
  public void testMerge() throws IOException {
    TimedQuota tq1 = TimedQuota.newBuilder().setSoftLimit(10)
        .setScope(QuotaProtos.QuotaScope.MACHINE)
        .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build();
    ThrottleRequest tr1 = ThrottleRequest.newBuilder().setTimedQuota(tq1)
        .setType(QuotaProtos.ThrottleType.REQUEST_NUMBER).build();
    ThrottleSettings orig = new ThrottleSettings("joe", null, null, null, tr1);

    TimedQuota tq2 = TimedQuota.newBuilder().setSoftLimit(10)
        .setScope(QuotaProtos.QuotaScope.MACHINE)
        .setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build();
    ThrottleRequest tr2 = ThrottleRequest.newBuilder().setTimedQuota(tq2)
        .setType(QuotaProtos.ThrottleType.REQUEST_NUMBER).build();

    ThrottleSettings merged = orig.merge(new ThrottleSettings("joe", null, null, null, tr2));

    assertEquals(10, merged.getSoftLimit());
    assertEquals(ThrottleType.REQUEST_NUMBER, merged.getThrottleType());
    assertEquals(TimeUnit.SECONDS, merged.getTimeUnit());
  }

  @Test
  public void testIncompatibleThrottleTypes() throws IOException {
    TimedQuota requestsQuota = TimedQuota.newBuilder().setSoftLimit(10)
        .setScope(QuotaProtos.QuotaScope.MACHINE)
        .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build();
    ThrottleRequest requestsQuotaReq = ThrottleRequest.newBuilder().setTimedQuota(requestsQuota)
        .setType(QuotaProtos.ThrottleType.REQUEST_NUMBER).build();
    ThrottleSettings orig = new ThrottleSettings("joe", null, null, null, requestsQuotaReq);

    TimedQuota readsQuota = TimedQuota.newBuilder().setSoftLimit(10)
        .setScope(QuotaProtos.QuotaScope.MACHINE)
        .setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build();
    ThrottleRequest readsQuotaReq = ThrottleRequest.newBuilder().setTimedQuota(readsQuota)
        .setType(QuotaProtos.ThrottleType.READ_NUMBER).build();

    try {
      orig.merge(new ThrottleSettings("joe", null, null, null, readsQuotaReq));
      fail("A read throttle should not be capable of being merged with a request quota");
    } catch (IllegalArgumentException e) {
      // Pass
    }
  }

  @Test
  public void testNoThrottleReturnsOriginal() throws IOException {
    TimedQuota tq1 = TimedQuota.newBuilder().setSoftLimit(10)
        .setScope(QuotaProtos.QuotaScope.MACHINE)
        .setTimeUnit(HBaseProtos.TimeUnit.MINUTES).build();
    ThrottleRequest tr1 = ThrottleRequest.newBuilder().setTimedQuota(tq1)
        .setType(QuotaProtos.ThrottleType.REQUEST_NUMBER).build();
    ThrottleSettings orig = new ThrottleSettings("joe", null, null, null, tr1);

    ThrottleRequest tr2 = ThrottleRequest.newBuilder()
        .setType(QuotaProtos.ThrottleType.REQUEST_NUMBER).build();

    assertTrue(
        "The same object should be returned by merge, but it wasn't",
      orig == orig.merge(new ThrottleSettings("joe", null, null, null, tr2)));
  }
}
