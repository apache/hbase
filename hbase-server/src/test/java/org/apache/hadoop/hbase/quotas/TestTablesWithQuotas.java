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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore.TablesWithQuotas;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

/**
 * Non-HBase cluster unit tests for {@link TablesWithQuotas}.
 */
@Category(SmallTests.class)
public class TestTablesWithQuotas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTablesWithQuotas.class);

  private Connection conn;
  private Configuration conf;

  @Before
  public void setup() throws Exception {
    conn = mock(Connection.class);
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testImmutableGetters() {
    Set<TableName> tablesWithTableQuotas = new HashSet<>();
    Set<TableName> tablesWithNamespaceQuotas = new HashSet<>();
    final TablesWithQuotas tables = new TablesWithQuotas(conn, conf);
    for (int i = 0; i < 5; i++) {
      TableName tn = TableName.valueOf("tn" + i);
      tablesWithTableQuotas.add(tn);
      tables.addTableQuotaTable(tn);
    }
    for (int i = 0; i < 3; i++) {
      TableName tn = TableName.valueOf("tn_ns" + i);
      tablesWithNamespaceQuotas.add(tn);
      tables.addNamespaceQuotaTable(tn);
    }
    Set<TableName> actualTableQuotaTables = tables.getTableQuotaTables();
    Set<TableName> actualNamespaceQuotaTables = tables.getNamespaceQuotaTables();
    assertEquals(tablesWithTableQuotas, actualTableQuotaTables);
    assertEquals(tablesWithNamespaceQuotas, actualNamespaceQuotaTables);
    try {
      actualTableQuotaTables.add(null);
      fail("Should not be able to add an element");
    } catch (UnsupportedOperationException e) {
      // pass
    }
    try {
      actualNamespaceQuotaTables.add(null);
      fail("Should not be able to add an element");
    } catch (UnsupportedOperationException e) {
      // pass
    }
  }

  @Test
  public void testInsufficientlyReportedTableFiltering() throws Exception {
    final Map<TableName,Integer> reportedRegions = new HashMap<>();
    final Map<TableName,Integer> actualRegions = new HashMap<>();
    final Configuration conf = HBaseConfiguration.create();
    conf.setDouble(QuotaObserverChore.QUOTA_OBSERVER_CHORE_REPORT_PERCENT_KEY, 0.95);

    TableName tooFewRegionsTable = TableName.valueOf("tn1");
    TableName sufficientRegionsTable = TableName.valueOf("tn2");
    TableName tooFewRegionsNamespaceTable = TableName.valueOf("ns1", "tn2");
    TableName sufficientRegionsNamespaceTable = TableName.valueOf("ns1", "tn2");
    final TablesWithQuotas tables = new TablesWithQuotas(conn, conf) {
      @Override
      Configuration getConfiguration() {
        return conf;
      }

      @Override
      int getNumRegions(TableName tableName) {
        return actualRegions.get(tableName);
      }

      @Override
      int getNumReportedRegions(TableName table, QuotaSnapshotStore<TableName> tableStore) {
        return reportedRegions.get(table);
      }
    };
    tables.addTableQuotaTable(tooFewRegionsTable);
    tables.addTableQuotaTable(sufficientRegionsTable);
    tables.addNamespaceQuotaTable(tooFewRegionsNamespaceTable);
    tables.addNamespaceQuotaTable(sufficientRegionsNamespaceTable);

    reportedRegions.put(tooFewRegionsTable, 5);
    actualRegions.put(tooFewRegionsTable, 10);
    reportedRegions.put(sufficientRegionsTable, 19);
    actualRegions.put(sufficientRegionsTable, 20);
    reportedRegions.put(tooFewRegionsNamespaceTable, 9);
    actualRegions.put(tooFewRegionsNamespaceTable, 10);
    reportedRegions.put(sufficientRegionsNamespaceTable, 98);
    actualRegions.put(sufficientRegionsNamespaceTable, 100);

    // Unused argument
    tables.filterInsufficientlyReportedTables(null);
    Set<TableName> filteredTablesWithTableQuotas = tables.getTableQuotaTables();
    assertEquals(Collections.singleton(sufficientRegionsTable), filteredTablesWithTableQuotas);
    Set<TableName> filteredTablesWithNamespaceQutoas = tables.getNamespaceQuotaTables();
    assertEquals(Collections.singleton(sufficientRegionsNamespaceTable), filteredTablesWithNamespaceQutoas);
  }

  @Test
  public void testGetTablesByNamespace() {
    final TablesWithQuotas tables = new TablesWithQuotas(conn, conf);
    tables.addTableQuotaTable(TableName.valueOf("ignored1"));
    tables.addTableQuotaTable(TableName.valueOf("ignored2"));
    tables.addNamespaceQuotaTable(TableName.valueOf("ns1", "t1"));
    tables.addNamespaceQuotaTable(TableName.valueOf("ns1", "t2"));
    tables.addNamespaceQuotaTable(TableName.valueOf("ns1", "t3"));
    tables.addNamespaceQuotaTable(TableName.valueOf("ns2", "t1"));
    tables.addNamespaceQuotaTable(TableName.valueOf("ns2", "t2"));

    Multimap<String,TableName> tablesByNamespace = tables.getTablesByNamespace();
    Collection<TableName> tablesInNs = tablesByNamespace.get("ns1");
    assertEquals(3, tablesInNs.size());
    assertTrue("Unexpected results for ns1: " + tablesInNs,
        tablesInNs.containsAll(Arrays.asList(
            TableName.valueOf("ns1", "t1"),
            TableName.valueOf("ns1", "t2"),
            TableName.valueOf("ns1", "t3"))));
    tablesInNs = tablesByNamespace.get("ns2");
    assertEquals(2, tablesInNs.size());
    assertTrue("Unexpected results for ns2: " + tablesInNs,
        tablesInNs.containsAll(Arrays.asList(
            TableName.valueOf("ns2", "t1"),
            TableName.valueOf("ns2", "t2"))));
  }

  @Test
  public void testFilteringMissingTables() throws Exception {
    final TableName missingTable = TableName.valueOf("doesNotExist");
    // Set up Admin to return null (match the implementation)
    Admin admin = mock(Admin.class);
    when(conn.getAdmin()).thenReturn(admin);
    when(admin.getTableRegions(missingTable)).thenReturn(null);

    QuotaObserverChore chore = mock(QuotaObserverChore.class);
    Map<RegionInfo,Long> regionUsage = new HashMap<>();
    TableQuotaSnapshotStore store = new TableQuotaSnapshotStore(conn, chore, regionUsage);

    // A super dirty hack to verify that, after getting no regions for our table,
    // we bail out and start processing the next element (which there is none).
    final TablesWithQuotas tables = new TablesWithQuotas(conn, conf) {
      @Override
      int getNumReportedRegions(TableName table, QuotaSnapshotStore<TableName> tableStore) {
        throw new RuntimeException("Should should not reach here");
      }
    };
    tables.addTableQuotaTable(missingTable);

    tables.filterInsufficientlyReportedTables(store);

    final Set<TableName> tablesWithQuotas = tables.getTableQuotaTables();
    assertTrue(
        "Expected to find no tables, but found " + tablesWithQuotas, tablesWithQuotas.isEmpty());
  }
}
