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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

/**
 * A {@link Rule} that manages the lifecycle of an instance of {@link AsyncConnection}. Can be used
 * in either the {@link Rule} or {@link ClassRule} positions.
 * </p>
 * Use in combination with {@link MiniClusterRule}, for example:
 *
 * <pre>{@code
 *   public class TestMyClass {
 *     private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder().build();
 *     private static final ConnectionRule connectionRule =
 *       ConnectionRule.createAsyncConnectionRule(miniClusterRule::createConnection);
 *
 *     @ClassRule
 *     public static final TestRule rule = RuleChain
 *       .outerRule(miniClusterRule)
 *       .around(connectionRule);
 *   }
 * }</pre>
 */
public final class ConnectionRule extends ExternalResource {

  private final Supplier<Connection> connectionSupplier;
  private final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier;

  private Connection connection;
  private AsyncConnection asyncConnection;

  public static ConnectionRule createConnectionRule(
    final Supplier<Connection> connectionSupplier
  ) {
    return new ConnectionRule(connectionSupplier, null);
  }

  public static ConnectionRule createAsyncConnectionRule(
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier
  ) {
    return new ConnectionRule(null, asyncConnectionSupplier);
  }

  public static ConnectionRule createConnectionRule(
    final Supplier<Connection> connectionSupplier,
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier
  ) {
    return new ConnectionRule(connectionSupplier, asyncConnectionSupplier);
  }

  private ConnectionRule(
    final Supplier<Connection> connectionSupplier,
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier
  ) {
    this.connectionSupplier = connectionSupplier;
    this.asyncConnectionSupplier = asyncConnectionSupplier;
  }

  public Connection getConnection() {
    if (connection == null) {
      throw new IllegalStateException(
        "ConnectionRule not initialized with a synchronous connection.");
    }
    return connection;
  }

  public AsyncConnection getAsyncConnection() {
    if (asyncConnection == null) {
      throw new IllegalStateException(
        "ConnectionRule not initialized with an asynchronous connection.");
    }
    return asyncConnection;
  }

  @Override
  protected void before() throws Throwable {
    if (connectionSupplier != null) {
      this.connection = connectionSupplier.get();
    }
    if (asyncConnectionSupplier != null) {
      this.asyncConnection = asyncConnectionSupplier.get().join();
    }
    if (connection == null && asyncConnection != null) {
      this.connection = asyncConnection.toConnection();
    }
  }

  @Override
  protected void after() {
    CompletableFuture<Void> closeConnection = CompletableFuture.runAsync(() -> {
      if (this.connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    CompletableFuture<Void> closeAsyncConnection = CompletableFuture.runAsync(() -> {
      if (this.asyncConnection != null) {
        try {
          asyncConnection.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    CompletableFuture.allOf(closeConnection, closeAsyncConnection).join();
  }
}
