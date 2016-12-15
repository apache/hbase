/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.quotas.policies.BulkLoadVerifyingViolationPolicyEnforcement;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link ActivePolicyEnforcement}.
 */
@Category(SmallTests.class)
public class TestActivePolicyEnforcement {

  @Test
  public void testGetter() {
    final TableName tableName = TableName.valueOf("table");
    Map<TableName, SpaceViolationPolicyEnforcement> map = new HashMap<>();
    map.put(tableName, new NoWritesViolationPolicyEnforcement());
    ActivePolicyEnforcement ape = new ActivePolicyEnforcement(map, Collections.emptyMap(), null);
    assertEquals(map.get(tableName), ape.getPolicyEnforcement(tableName));
  }

  @Test
  public void testNoPolicyReturnsNoopEnforcement() {
    ActivePolicyEnforcement ape = new ActivePolicyEnforcement(
        new HashMap<>(), Collections.emptyMap(), mock(RegionServerServices.class));
    SpaceViolationPolicyEnforcement enforcement = ape.getPolicyEnforcement(
        TableName.valueOf("nonexistent"));
    assertNotNull(enforcement);
    assertTrue(
        "Expected an instance of NoopViolationPolicyEnforcement",
        enforcement instanceof BulkLoadVerifyingViolationPolicyEnforcement);
  }

  @Test
  public void testNoBulkLoadChecksOnNoSnapshot() {
    ActivePolicyEnforcement ape = new ActivePolicyEnforcement(
        new HashMap<TableName, SpaceViolationPolicyEnforcement>(),
        Collections.<TableName,SpaceQuotaSnapshot> emptyMap(),
        mock(RegionServerServices.class));
    SpaceViolationPolicyEnforcement enforcement = ape.getPolicyEnforcement(
        TableName.valueOf("nonexistent"));
    assertFalse("Should not check bulkloads", enforcement.shouldCheckBulkLoads());
  }
}
