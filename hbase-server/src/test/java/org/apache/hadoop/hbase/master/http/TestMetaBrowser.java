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
package org.apache.hadoop.hbase.master.http;

import static org.apache.hadoop.hbase.client.hamcrest.BytesMatchers.bytesAsStringBinary;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.hbase.ClearUserNamespacesAndTablesRule;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.IterableUtils;

/**
 * Cluster-backed correctness tests for the functionality provided by {@link MetaBrowser}.
 */
@Category({ MasterTests.class, MediumTests.class})
public class TestMetaBrowser {

  @ClassRule
  public static final HBaseClassTestRule testRule =
    HBaseClassTestRule.forClass(TestMetaBrowser.class);
  @ClassRule
  public static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder().build();

  private final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);
  private final ClearUserNamespacesAndTablesRule clearUserNamespacesAndTablesRule =
    new ClearUserNamespacesAndTablesRule(connectionRule::getAsyncConnection);

  @Rule
  public TestRule rule = RuleChain.outerRule(connectionRule)
    .around(clearUserNamespacesAndTablesRule);

  @Rule
  public TestName testNameRule = new TestName();

  private AsyncConnection connection;
  private AsyncAdmin admin;

  @Before
  public void before() {
    connection = connectionRule.getAsyncConnection();
    admin = connection.getAdmin();
  }

  @Test
  public void noFilters() {
    final String namespaceName = testNameRule.getMethodName();
    final TableName a = TableName.valueOf("a");
    final TableName b = TableName.valueOf(namespaceName, "b");

    CompletableFuture.allOf(
      createTable(a),
      createNamespace(namespaceName).thenCompose(_void -> createTable(b, 2)))
      .join();

    final HttpServletRequest request = new MockRequestBuilder().build();
    final List<RegionReplicaInfo> rows;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request).getResults()) {
      rows = IterableUtils.toList(results);
    }
    assertThat(rows, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(a + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",80000000")))));
  }

  @Test
  public void limit() {
    final String tableName = testNameRule.getMethodName();
    createTable(TableName.valueOf(tableName), 8).join();

    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(5)
      .build();
    final List<RegionReplicaInfo> rows;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request).getResults()) {
      rows = IterableUtils.toList(results);
    }
    assertThat(rows, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(tableName + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(tableName + ",20000000"))),
      hasProperty("row", bytesAsStringBinary(startsWith(tableName + ",40000000"))),
      hasProperty("row", bytesAsStringBinary(startsWith(tableName + ",60000000"))),
      hasProperty("row", bytesAsStringBinary(startsWith(tableName + ",80000000")))));
  }

  @Test
  public void regionStateFilter() {
    final String namespaceName = testNameRule.getMethodName();
    final TableName foo = TableName.valueOf(namespaceName, "foo");
    final TableName bar = TableName.valueOf(namespaceName, "bar");

    createNamespace(namespaceName)
      .thenCompose(_void1 -> CompletableFuture.allOf(
        createTable(foo, 2).thenCompose(_void2 -> admin.disableTable(foo)),
        createTable(bar, 2)))
      .join();

    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(10_000)
      .setRegionState(RegionState.State.OPEN)
      .setTable(namespaceName)
      .build();
    final List<RegionReplicaInfo> rows;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request).getResults()) {
      rows = IterableUtils.toList(results);
    }
    assertThat(rows, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(bar.toString() + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(bar.toString() + ",80000000")))));
  }

  @Test
  public void scanTableFilter() {
    final String namespaceName = testNameRule.getMethodName();
    final TableName a = TableName.valueOf("a");
    final TableName b = TableName.valueOf(namespaceName, "b");

    CompletableFuture.allOf(
      createTable(a),
      createNamespace(namespaceName).thenCompose(_void -> createTable(b, 2)))
      .join();

    final HttpServletRequest request = new MockRequestBuilder()
      .setTable(namespaceName)
      .build();
    final List<RegionReplicaInfo> rows;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request).getResults()) {
      rows = IterableUtils.toList(results);
    }
    assertThat(rows, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",80000000")))));
  }

  @Test
  public void paginateWithReplicas() {
    final String namespaceName = testNameRule.getMethodName();
    final TableName a = TableName.valueOf("a");
    final TableName b = TableName.valueOf(namespaceName, "b");

    CompletableFuture.allOf(
      createTableWithReplicas(a, 2),
      createNamespace(namespaceName).thenCompose(_void -> createTable(b, 2)))
      .join();

    final HttpServletRequest request1 = new MockRequestBuilder()
      .setLimit(2)
      .build();
    final List<RegionReplicaInfo> rows1;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request1).getResults()) {
      rows1 = IterableUtils.toList(results);
    }
    assertThat(rows1, contains(
      allOf(
        hasProperty("regionName", bytesAsStringBinary(startsWith(a + ",,"))),
        hasProperty("replicaId", equalTo(0))),
      allOf(
        hasProperty("regionName", bytesAsStringBinary(startsWith(a + ",,"))),
        hasProperty("replicaId", equalTo(1)))));

    final HttpServletRequest request2 = new MockRequestBuilder()
      .setLimit(2)
      .setStart(MetaBrowser.buildStartParamFrom(rows1.get(rows1.size() - 1).getRow()))
      .build();
    final List<RegionReplicaInfo> rows2;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request2).getResults()) {
      rows2 = IterableUtils.toList(results);
    }
    assertThat(rows2, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",80000000")))));
  }

  @Test
  public void paginateWithTableFilter() {
    final String namespaceName = testNameRule.getMethodName();
    final TableName a = TableName.valueOf("a");
    final TableName b = TableName.valueOf(namespaceName, "b");

    CompletableFuture.allOf(
      createTable(a),
      createNamespace(namespaceName).thenCompose(_void -> createTable(b, 5)))
      .join();

    final HttpServletRequest request1 = new MockRequestBuilder()
      .setLimit(2)
      .setTable(namespaceName)
      .build();
    final List<RegionReplicaInfo> rows1;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request1).getResults()) {
      rows1 = IterableUtils.toList(results);
    }
    assertThat(rows1, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",,"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",33333333")))));

    final HttpServletRequest request2 = new MockRequestBuilder()
      .setLimit(2)
      .setTable(namespaceName)
      .setStart(MetaBrowser.buildStartParamFrom(rows1.get(rows1.size() - 1).getRow()))
      .build();
    final List<RegionReplicaInfo> rows2;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request2).getResults()) {
      rows2 = IterableUtils.toList(results);
    }
    assertThat(rows2, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",66666666"))),
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",99999999")))));

    final HttpServletRequest request3 = new MockRequestBuilder()
      .setLimit(2)
      .setTable(namespaceName)
      .setStart(MetaBrowser.buildStartParamFrom(rows2.get(rows2.size() - 1).getRow()))
      .build();
    final List<RegionReplicaInfo> rows3;
    try (final MetaBrowser.Results results = new MetaBrowser(connection, request3).getResults()) {
      rows3 = IterableUtils.toList(results);
    }
    assertThat(rows3, contains(
      hasProperty("row", bytesAsStringBinary(startsWith(b + ",cccccccc")))));
  }

  private ColumnFamilyDescriptor columnFamilyDescriptor() {
    return ColumnFamilyDescriptorBuilder.of("f1");
  }

  private TableDescriptor tableDescriptor(final TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(columnFamilyDescriptor())
      .build();
  }

  private TableDescriptor tableDescriptor(final TableName tableName, final int replicaCount) {
    return TableDescriptorBuilder.newBuilder(tableName)
      .setRegionReplication(replicaCount)
      .setColumnFamily(columnFamilyDescriptor())
      .build();
  }

  private CompletableFuture<Void> createTable(final TableName tableName) {
    return admin.createTable(tableDescriptor(tableName));
  }

  private CompletableFuture<Void> createTable(final TableName tableName, final int splitCount) {
    return admin.createTable(
      tableDescriptor(tableName),
      new RegionSplitter.HexStringSplit().split(splitCount));
  }

  private CompletableFuture<Void> createTableWithReplicas(final TableName tableName,
    final int replicaCount) {
    return admin.createTable(tableDescriptor(tableName, replicaCount));
  }

  private CompletableFuture<Void> createNamespace(final String namespace) {
    final NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();
    return admin.createNamespace(descriptor);
  }

  /**
   * Helper for mocking an {@link HttpServletRequest} relevant to the test.
   */
  static class MockRequestBuilder {

    private String limit = null;
    private String regionState = null;
    private String start = null;
    private String table = null;

    public MockRequestBuilder setLimit(final int value) {
      this.limit = Integer.toString(value);
      return this;
    }

    public MockRequestBuilder setLimit(final String value) {
      this.limit = value;
      return this;
    }

    public MockRequestBuilder setRegionState(final RegionState.State value) {
      this.regionState = value.toString();
      return this;
    }

    public MockRequestBuilder setRegionState(final String value) {
      this.regionState = value;
      return this;
    }

    public MockRequestBuilder setStart(final String value) {
      this.start = value;
      return this;
    }

    public MockRequestBuilder setTable(final String value) {
      this.table = value;
      return this;
    }

    public HttpServletRequest build() {
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRequestURI()).thenReturn("/table.jsp");
      when(request.getParameter("name")).thenReturn("hbase%3Ameta");

      when(request.getParameter("scan_limit")).thenReturn(limit);
      when(request.getParameter("scan_region_state")).thenReturn(regionState);
      when(request.getParameter("scan_start")).thenReturn(start);
      when(request.getParameter("scan_table")).thenReturn(table);

      return request;
    }
  }
}
