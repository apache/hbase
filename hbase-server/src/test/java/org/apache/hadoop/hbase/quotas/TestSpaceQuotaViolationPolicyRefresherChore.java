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

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link SpaceQuotaRefresherChore}.
 */
@Category(SmallTests.class)
public class TestSpaceQuotaViolationPolicyRefresherChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaViolationPolicyRefresherChore.class);

  private RegionServerSpaceQuotaManager manager;
  private RegionServerServices rss;
  private SpaceQuotaRefresherChore chore;
  private Configuration conf;
  private Connection conn;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws IOException {
    conf = HBaseConfiguration.create();
    rss = mock(RegionServerServices.class);
    manager = mock(RegionServerSpaceQuotaManager.class);
    conn = mock(Connection.class);
    when(manager.getRegionServerServices()).thenReturn(rss);
    when(rss.getConfiguration()).thenReturn(conf);


    chore = mock(SpaceQuotaRefresherChore.class);
    when(chore.getConnection()).thenReturn(conn);
    when(chore.getManager()).thenReturn(manager);
    when(chore.checkQuotaTableExists()).thenReturn(true);
    doCallRealMethod().when(chore).chore();
    when(chore.isInViolation(any())).thenCallRealMethod();
    doCallRealMethod().when(chore).extractQuotaSnapshot(any(), any());
  }

  @Test
  public void testPoliciesAreEnforced() throws IOException {
    // Create a number of policies that should be enforced (usage > limit)
    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(
        TableName.valueOf("table1"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 1024L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table2"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_INSERTS), 2048L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table3"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 4096L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table4"),
        new SpaceQuotaSnapshot(
            new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES_COMPACTIONS), 8192L, 512L));

    // No active enforcements
    when(manager.copyQuotaSnapshots()).thenReturn(Collections.emptyMap());
    // Policies to enforce
    when(chore.fetchSnapshotsFromQuotaTable()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceQuotaSnapshot> entry : policiesToEnforce.entrySet()) {
      // Ensure we enforce the policy
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
      // Don't disable any policies
      verify(manager, never()).disableViolationPolicyEnforcement(entry.getKey());
    }
  }

  @Test
  public void testOldPoliciesAreRemoved() throws IOException {
    final Map<TableName,SpaceQuotaSnapshot> previousPolicies = new HashMap<>();
    previousPolicies.put(
        TableName.valueOf("table3"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 4096L, 512L));
    previousPolicies.put(
        TableName.valueOf("table4"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 8192L, 512L));

    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(
        TableName.valueOf("table1"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 1024L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table2"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_INSERTS), 2048L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table3"),
        new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 256L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table4"),
        new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 128L, 512L));

    // No active enforcements
    when(manager.copyQuotaSnapshots()).thenReturn(previousPolicies);
    // Policies to enforce
    when(chore.fetchSnapshotsFromQuotaTable()).thenReturn(policiesToEnforce);

    chore.chore();

    verify(manager).enforceViolationPolicy(
        TableName.valueOf("table1"), policiesToEnforce.get(TableName.valueOf("table1")));
    verify(manager).enforceViolationPolicy(
        TableName.valueOf("table2"), policiesToEnforce.get(TableName.valueOf("table2")));

    verify(manager).disableViolationPolicyEnforcement(TableName.valueOf("table3"));
    verify(manager).disableViolationPolicyEnforcement(TableName.valueOf("table4"));
  }

  @Test
  public void testNewPolicyOverridesOld() throws IOException {
    final Map<TableName,SpaceQuotaSnapshot> policiesToEnforce = new HashMap<>();
    policiesToEnforce.put(
        TableName.valueOf("table1"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 1024L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table2"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 2048L, 512L));
    policiesToEnforce.put(
        TableName.valueOf("table3"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_INSERTS), 4096L, 512L));

    final Map<TableName,SpaceQuotaSnapshot> previousPolicies = new HashMap<>();
    previousPolicies.put(
        TableName.valueOf("table1"),
        new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 8192L, 512L));

    // No active enforcements
    when(manager.getActivePoliciesAsMap()).thenReturn(previousPolicies);
    // Policies to enforce
    when(chore.fetchSnapshotsFromQuotaTable()).thenReturn(policiesToEnforce);

    chore.chore();

    for (Entry<TableName,SpaceQuotaSnapshot> entry : policiesToEnforce.entrySet()) {
      verify(manager).enforceViolationPolicy(entry.getKey(), entry.getValue());
    }
    verify(manager, never()).disableViolationPolicyEnforcement(TableName.valueOf("table1"));
  }

  @Test
  public void testMissingAllColumns() throws IOException {
    when(chore.fetchSnapshotsFromQuotaTable()).thenCallRealMethod();
    ResultScanner scanner = mock(ResultScanner.class);
    Table quotaTable = mock(Table.class);
    when(conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);
    when(quotaTable.getScanner(any(Scan.class))).thenReturn(scanner);

    List<Result> results = new ArrayList<>();
    results.add(Result.create(Collections.emptyList()));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      chore.fetchSnapshotsFromQuotaTable();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we had no cells in the row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testMissingDesiredColumn() throws IOException {
    when(chore.fetchSnapshotsFromQuotaTable()).thenCallRealMethod();
    ResultScanner scanner = mock(ResultScanner.class);
    Table quotaTable = mock(Table.class);
    when(conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);
    when(quotaTable.getScanner(any(Scan.class))).thenReturn(scanner);

    List<Result> results = new ArrayList<>();
    // Give a column that isn't the one we want
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("q"), toBytes("s"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      chore.fetchSnapshotsFromQuotaTable();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we were missing the column we expected in this row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testParsingError() throws IOException {
    when(chore.fetchSnapshotsFromQuotaTable()).thenCallRealMethod();
    ResultScanner scanner = mock(ResultScanner.class);
    Table quotaTable = mock(Table.class);
    when(conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);
    when(quotaTable.getScanner(any(Scan.class))).thenReturn(scanner);

    List<Result> results = new ArrayList<>();
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("u"), toBytes("v"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      chore.fetchSnapshotsFromQuotaTable();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // We provided a garbage serialized protobuf message (empty byte array), this should
      // in turn throw an IOException
    }
  }
}
