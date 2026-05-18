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
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An {@link Extension} that manages the lifecycle of an instance of {@link AsyncConnection}.
 * </p>
 * Use in combination with {@link MiniClusterExtension}, for example:
 *
 * <pre>
 * {
 *   public class TestMyClass {
 *
 *     &#64;Order(1)
 *     &#64;RegisterExtension
 *     private static final MiniClusterExtension miniClusterExtension =
 *        miniClusterExtension.newBuilder().build();
 *
 *     &#64;Order(2)
 *     &#64;RegisterExtension
 *     private static final ConnectionExtension connectionExtension =
 *         ConnectionExtension.createAsyncConnectionExtension(
 *            miniClusterExtension::createConnection);
 *   }
 * </pre>
 */
public final class ConnectionExtension implements BeforeAllCallback, AfterAllCallback {

  private final Supplier<Connection> connectionSupplier;
  private final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier;

  private Connection connection;
  private AsyncConnection asyncConnection;

  public static ConnectionExtension
    createConnectionExtension(final Supplier<Connection> connectionSupplier) {
    return new ConnectionExtension(connectionSupplier, null);
  }

  public static ConnectionExtension createAsyncConnectionExtension(
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier) {
    return new ConnectionExtension(null, asyncConnectionSupplier);
  }

  public static ConnectionExtension createConnectionExtension(
    final Supplier<Connection> connectionSupplier,
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier) {
    return new ConnectionExtension(connectionSupplier, asyncConnectionSupplier);
  }

  private ConnectionExtension(final Supplier<Connection> connectionSupplier,
    final Supplier<CompletableFuture<AsyncConnection>> asyncConnectionSupplier) {
    this.connectionSupplier = connectionSupplier;
    this.asyncConnectionSupplier = asyncConnectionSupplier;
  }

  public Connection getConnection() {
    if (connection == null) {
      throw new IllegalStateException(
        "ConnectionExtension not initialized with a synchronous connection.");
    }
    return connection;
  }

  public AsyncConnection getAsyncConnection() {
    if (asyncConnection == null) {
      throw new IllegalStateException(
        "ConnectionExtension not initialized with an asynchronous connection.");
    }
    return asyncConnection;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
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
  public void afterAll(ExtensionContext context) {
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
