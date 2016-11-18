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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link SpaceQuotaViolationPolicyRefresherChore}.
 */
@Category(SmallTests.class)
public class TestSpaceQuotaViolationPolicyRefresherChore {

  private RegionServerSpaceQuotaManager manager;
  private RegionServerServices rss;
  private SpaceQuotaViolationPolicyRefresherChore chore;
  private Configuration conf;

  @Before
  public void setup() {
    conf = HBaseConfiguration.create();
    rss = mock(RegionServerServices.class);
    manager = mock(RegionServerSpaceQuotaManager.class);
    when(manager.getRegionServerServices()).thenReturn(rss);
    when(rss.getConfiguration()).thenReturn(conf);
    chore = new SpaceQuotaViolationPolicyRefresherChore(manager);
  }

  @Test
  public void testPoliciesAreEnforced() throws IOException {
    final Map<TableName,SpaceViolationPolicy> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), SpaceViolationPolicy.DISABLE);
    policiesToEnforce.put(TableName.valueOf("table2"), SpaceViolationPolicy.NO_INSERTS);
    policiesToEnforce.put(TableName.valueOf("table3"), SpaceViolationPolicy.NO_WRITES);
    policiesToEnforce.put(TableName.valueOf("table4"), SpaceViolationPolicy.NO_WRITES_COMPACTIONS);

    // No active enforcements
    when(manager.getActiveViolationPolicyEnforcements()).thenReturn(Collections.emptyMap());
    // Policies to enforce
    when(manager.getViolationPoliciesToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolationPolicy> entry : policiesToEnforce.entrySet()) {
      // Ensure we enforce the policy
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
      // Don't disable any policies
      verify(manager, never()).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testOldPoliciesAreRemoved() throws IOException {
    final Map<TableName,SpaceViolationPolicy> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), SpaceViolationPolicy.DISABLE);
    policiesToEnforce.put(TableName.valueOf("table2"), SpaceViolationPolicy.NO_INSERTS);

    final Map<TableName,SpaceViolationPolicy> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table3"), SpaceViolationPolicy.NO_WRITES);
    previousPolicies.put(TableName.valueOf("table4"), SpaceViolationPolicy.NO_WRITES);

    // No active enforcements
    when(manager.getActiveViolationPolicyEnforcements()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationPoliciesToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolationPolicy> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
    }

    for (Entry<TableName,SpaceViolationPolicy> entry : previousPolicies.entrySet()) {
      verify(manager).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testNewPolicyOverridesOld() throws IOException {
    final Map<TableName,SpaceViolationPolicy> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(TableName.valueOf("table1"), SpaceViolationPolicy.DISABLE);
    policiesToEnforce.put(TableName.valueOf("table2"), SpaceViolationPolicy.NO_WRITES);
    policiesToEnforce.put(TableName.valueOf("table3"), SpaceViolationPolicy.NO_INSERTS);

    final Map<TableName,SpaceViolationPolicy> previousPolicies = new HashMap<>();
    previousPolicies.put(TableName.valueOf("table1"), SpaceViolationPolicy.NO_WRITES);

    // No active enforcements
    when(manager.getActiveViolationPolicyEnforcements()).thenReturn(previousPolicies);
    // Policies to enforce
    when(manager.getViolationPoliciesToEnforce()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceViolationPolicy> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
    }
    verify(manager, never()).disableViolationPolicyEnforcement(TableName.valueOf("table1"));
  }
}
