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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.policies.DefaultViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.MissingSnapshotViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link ActivePolicyEnforcement}.
 */
@Category(SmallTests.class)
public class TestActivePolicyEnforcement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestActivePolicyEnforcement.class);

  private RegionServerServices rss;

  @Before
  public void setup() {
    rss = mock(RegionServerServices.class);
  }

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
        "Expected an instance of MissingSnapshotViolationPolicyEnforcement, but got "
            + enforcement.getClass(),
        enforcement instanceof MissingSnapshotViolationPolicyEnforcement);
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

  @Test
  public void testNoQuotaReturnsSingletonPolicyEnforcement() {
    final ActivePolicyEnforcement ape = new ActivePolicyEnforcement(
        Collections.emptyMap(), Collections.emptyMap(), rss);
    final TableName tableName = TableName.valueOf("my_table");
    SpaceViolationPolicyEnforcement policyEnforcement = ape.getPolicyEnforcement(tableName);
    // This should be the same exact instance, the singleton
    assertTrue(policyEnforcement == MissingSnapshotViolationPolicyEnforcement.getInstance());
    assertEquals(1, ape.getLocallyCachedPolicies().size());
    Entry<TableName,SpaceViolationPolicyEnforcement> entry =
        ape.getLocallyCachedPolicies().entrySet().iterator().next();
    assertTrue(policyEnforcement == entry.getValue());
  }

  @Test
  public void testNonViolatingQuotaCachesPolicyEnforcment() {
    final Map<TableName,SpaceQuotaSnapshot> snapshots = new HashMap<>();
    final TableName tableName = TableName.valueOf("my_table");
    snapshots.put(tableName, new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 0, 1024));
    final ActivePolicyEnforcement ape = new ActivePolicyEnforcement(
        Collections.emptyMap(), snapshots, rss);
    SpaceViolationPolicyEnforcement policyEnforcement = ape.getPolicyEnforcement(tableName);
    assertTrue(
        "Found the wrong class: " + policyEnforcement.getClass(),
        policyEnforcement instanceof DefaultViolationPolicyEnforcement);
    SpaceViolationPolicyEnforcement copy = ape.getPolicyEnforcement(tableName);
    assertTrue("Expected the instance to be cached", policyEnforcement == copy);
    Entry<TableName,SpaceViolationPolicyEnforcement> entry =
        ape.getLocallyCachedPolicies().entrySet().iterator().next();
    assertTrue(policyEnforcement == entry.getValue());
  }

  @Test
  public void testViolatingQuotaCachesNothing() {
    final TableName tableName = TableName.valueOf("my_table");
    SpaceViolationPolicyEnforcement policyEnforcement = mock(SpaceViolationPolicyEnforcement.class);
    final Map<TableName,SpaceViolationPolicyEnforcement> activePolicies = new HashMap<>();
    activePolicies.put(tableName, policyEnforcement);
    final ActivePolicyEnforcement ape = new ActivePolicyEnforcement(
        activePolicies, Collections.emptyMap(), rss);
    assertTrue(ape.getPolicyEnforcement(tableName) == policyEnforcement);
    assertEquals(0, ape.getLocallyCachedPolicies().size());
  }
}
