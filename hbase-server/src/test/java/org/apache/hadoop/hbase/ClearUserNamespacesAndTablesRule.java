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
package org.apache.hadoop.hbase;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TestRule} that clears all user namespaces and tables
 * {@link ExternalResource#before() before} the test executes. Can be used in either the
 * {@link Rule} or {@link ClassRule} positions. Lazily realizes the provided
 * {@link AsyncConnection} so as to avoid initialization races with other {@link Rule Rules}.
 * <b>Does not</b> {@link AsyncConnection#close() close()} provided connection instance when
 * finished.
 * </p>
 * Use in combination with {@link MiniClusterRule} and {@link ConnectionRule}, for example:
 *
 * <pre>{@code
 *   public class TestMyClass {
 *     @ClassRule
 *     public static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder().build();
 *
 *     private final ConnectionRule connectionRule =
 *       new ConnectionRule(miniClusterRule::createConnection);
 *     private final ClearUserNamespacesAndTablesRule clearUserNamespacesAndTablesRule =
 *       new ClearUserNamespacesAndTablesRule(connectionRule::getConnection);
 *
 *     @Rule
 *     public TestRule rule = RuleChain
 *       .outerRule(connectionRule)
 *       .around(clearUserNamespacesAndTablesRule);
 *   }
 * }</pre>
 */
public class ClearUserNamespacesAndTablesRule extends ExternalResource {
  private static final Logger logger =
    LoggerFactory.getLogger(ClearUserNamespacesAndTablesRule.class);

  private final Supplier<AsyncConnection> connectionSupplier;
  private AsyncAdmin admin;

  public ClearUserNamespacesAndTablesRule(final Supplier<AsyncConnection> connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
  }

  @Override
  protected void before() throws Throwable {
    final AsyncConnection connection = Objects.requireNonNull(connectionSupplier.get());
    admin = connection.getAdmin();

    clearTablesAndNamespaces().join();
  }

  private CompletableFuture<Void> clearTablesAndNamespaces() {
    return deleteUserTables().thenCompose(_void -> deleteUserNamespaces());
  }

  private CompletableFuture<Void> deleteUserTables() {
    return listTableNames()
      .thenApply(tableNames -> tableNames.stream()
        .map(tableName -> disableIfEnabled(tableName).thenCompose(_void -> deleteTable(tableName)))
        .toArray(CompletableFuture[]::new))
      .thenCompose(CompletableFuture::allOf);
  }

  private CompletableFuture<List<TableName>> listTableNames() {
    return CompletableFuture
      .runAsync(() -> logger.trace("listing tables"))
      .thenCompose(_void -> admin.listTableNames(false))
      .thenApply(tableNames -> {
        if (logger.isTraceEnabled()) {
          final StringJoiner joiner = new StringJoiner(", ", "[", "]");
          tableNames.stream().map(TableName::getNameAsString).forEach(joiner::add);
          logger.trace("found existing tables {}", joiner.toString());
        }
        return tableNames;
      });
  }

  private CompletableFuture<Boolean> isTableEnabled(final TableName tableName) {
    return admin.isTableEnabled(tableName)
      .thenApply(isEnabled -> {
        logger.trace("table {} is enabled.", tableName);
        return isEnabled;
      });
  }

  private CompletableFuture<Void> disableIfEnabled(final TableName tableName) {
    return isTableEnabled(tableName)
      .thenCompose(isEnabled -> isEnabled
        ? disableTable(tableName)
        : CompletableFuture.completedFuture(null));
  }

  private CompletableFuture<Void> disableTable(final TableName tableName) {
    return CompletableFuture
      .runAsync(() -> logger.trace("disabling enabled table {}", tableName))
      .thenCompose(_void -> admin.disableTable(tableName));
  }

  private CompletableFuture<Void> deleteTable(final TableName tableName) {
    return CompletableFuture
      .runAsync(() -> logger.trace("deleting disabled table {}", tableName))
      .thenCompose(_void -> admin.deleteTable(tableName));
  }

  private CompletableFuture<List<String>> listUserNamespaces() {
    return CompletableFuture
      .runAsync(() -> logger.trace("listing namespaces"))
      .thenCompose(_void -> admin.listNamespaceDescriptors())
      .thenApply(namespaceDescriptors -> {
        final StringJoiner joiner = new StringJoiner(", ", "[", "]");
        final List<String> names = namespaceDescriptors.stream()
          .map(NamespaceDescriptor::getName)
          .peek(joiner::add)
          .collect(Collectors.toList());
        logger.trace("found existing namespaces {}", joiner);
        return names;
      })
      .thenApply(namespaces -> namespaces.stream()
        .filter(namespace -> !Objects.equals(
          namespace, NamespaceDescriptor.SYSTEM_NAMESPACE.getName()))
        .filter(namespace -> !Objects.equals(
          namespace, NamespaceDescriptor.DEFAULT_NAMESPACE.getName()))
        .collect(Collectors.toList()));
  }

  private CompletableFuture<Void> deleteNamespace(final String namespace) {
    return CompletableFuture
      .runAsync(() -> logger.trace("deleting namespace {}", namespace))
      .thenCompose(_void -> admin.deleteNamespace(namespace));
  }

  private CompletableFuture<Void> deleteUserNamespaces() {
    return listUserNamespaces()
      .thenCompose(namespaces -> CompletableFuture.allOf(namespaces.stream()
        .map(this::deleteNamespace)
        .toArray(CompletableFuture[]::new)));
  }
}
