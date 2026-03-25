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
package org.apache.hadoop.hbase.quotas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceLimitRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * Test class for {@link SpaceLimitSettings}.
 */
@Tag(SmallTests.TAG)
public class TestSpaceLimitSettings {

  @Test
  public void testInvalidTableQuotaSizeLimit() {
    assertThrows(IllegalArgumentException.class,
      () -> new SpaceLimitSettings(TableName.valueOf("foo"), -1, SpaceViolationPolicy.NO_INSERTS));
  }

  @Test
  public void testNullTableName() {
    TableName tn = null;
    assertThrows(NullPointerException.class,
      () -> new SpaceLimitSettings(tn, 1, SpaceViolationPolicy.NO_INSERTS));
  }

  @Test
  public void testNullTableViolationPolicy() {
    assertThrows(NullPointerException.class,
      () -> new SpaceLimitSettings(TableName.valueOf("foo"), 1, null));
  }

  @Test
  public void testInvalidNamespaceQuotaSizeLimit() {
    assertThrows(IllegalArgumentException.class,
      () -> new SpaceLimitSettings("foo_ns", -1, SpaceViolationPolicy.NO_INSERTS));
  }

  @Test
  public void testNullNamespace() {
    String ns = null;
    assertThrows(NullPointerException.class,
      () -> new SpaceLimitSettings(ns, 1, SpaceViolationPolicy.NO_INSERTS));
  }

  @Test
  public void testNullNamespaceViolationPolicy() {
    assertThrows(NullPointerException.class, () -> new SpaceLimitSettings("foo_ns", 1, null));

  }

  @Test
  public void testTableQuota() {
    final TableName tableName = TableName.valueOf("foo");
    final long sizeLimit = 1024 * 1024;
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_WRITES;
    SpaceLimitSettings settings = new SpaceLimitSettings(tableName, sizeLimit, policy);
    SetQuotaRequest proto = QuotaSettings.buildSetQuotaRequestProto(settings);

    assertFalse(proto.hasUserName(), "User should be missing");
    assertFalse(proto.hasNamespace(), "Namespace should be missing");
    assertEquals(ProtobufUtil.toProtoTableName(tableName), proto.getTableName());
    SpaceLimitRequest spaceLimitReq = proto.getSpaceLimit();
    assertNotNull(spaceLimitReq, "SpaceLimitRequest was null");
    SpaceQuota spaceQuota = spaceLimitReq.getQuota();
    assertNotNull(spaceQuota, "SpaceQuota was null");
    assertEquals(sizeLimit, spaceQuota.getSoftLimit());
    assertEquals(ProtobufUtil.toProtoViolationPolicy(policy), spaceQuota.getViolationPolicy());

    assertEquals(QuotaType.SPACE, settings.getQuotaType());

    SpaceLimitSettings copy = new SpaceLimitSettings(tableName, sizeLimit, policy);
    assertEquals(settings, copy);
    assertEquals(settings.hashCode(), copy.hashCode());
  }

  @Test
  public void testNamespaceQuota() {
    final String namespace = "foo_ns";
    final long sizeLimit = 1024 * 1024;
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_WRITES;
    SpaceLimitSettings settings = new SpaceLimitSettings(namespace, sizeLimit, policy);
    SetQuotaRequest proto = QuotaSettings.buildSetQuotaRequestProto(settings);

    assertFalse(proto.hasUserName(), "User should be missing");
    assertFalse(proto.hasTableName(), "TableName should be missing");
    assertEquals(namespace, proto.getNamespace());
    SpaceLimitRequest spaceLimitReq = proto.getSpaceLimit();
    assertNotNull(spaceLimitReq, "SpaceLimitRequest was null");
    SpaceQuota spaceQuota = spaceLimitReq.getQuota();
    assertNotNull(spaceQuota, "SpaceQuota was null");
    assertEquals(sizeLimit, spaceQuota.getSoftLimit());
    assertEquals(ProtobufUtil.toProtoViolationPolicy(policy), spaceQuota.getViolationPolicy());

    assertEquals(QuotaType.SPACE, settings.getQuotaType());

    SpaceLimitSettings copy = new SpaceLimitSettings(namespace, sizeLimit, policy);
    assertEquals(settings, copy);
    assertEquals(settings.hashCode(), copy.hashCode());
  }

  @Test
  public void testQuotaMerging() throws IOException {
    TableName tn = TableName.valueOf("foo");
    QuotaSettings originalSettings =
      QuotaSettingsFactory.limitTableSpace(tn, 1024L * 1024L, SpaceViolationPolicy.DISABLE);
    QuotaSettings largerSizeLimit =
      QuotaSettingsFactory.limitTableSpace(tn, 5L * 1024L * 1024L, SpaceViolationPolicy.DISABLE);
    QuotaSettings differentPolicy =
      QuotaSettingsFactory.limitTableSpace(tn, 1024L * 1024L, SpaceViolationPolicy.NO_WRITES);
    QuotaSettings incompatibleSettings = QuotaSettingsFactory.limitNamespaceSpace("ns1",
      5L * 1024L * 1024L, SpaceViolationPolicy.NO_WRITES);

    assertEquals(originalSettings.merge(largerSizeLimit), largerSizeLimit);
    assertEquals(originalSettings.merge(differentPolicy), differentPolicy);
    try {
      originalSettings.merge(incompatibleSettings);
      fail("Should not be able to merge a Table space quota with a namespace space quota.");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }
}
