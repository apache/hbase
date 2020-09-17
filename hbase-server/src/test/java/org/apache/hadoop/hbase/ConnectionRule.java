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
 *       new ConnectionRule(miniClusterRule::createConnection);
 *
 *     @ClassRule
 *     public static final TestRule rule = RuleChain
 *       .outerRule(miniClusterRule)
 *       .around(connectionRule);
 *   }
 * }</pre>
 */
public class ConnectionRule extends ExternalResource {

  private final Supplier<CompletableFuture<AsyncConnection>> connectionSupplier;
  private AsyncConnection connection;

  public ConnectionRule(final Supplier<CompletableFuture<AsyncConnection>> connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
  }

  public AsyncConnection getConnection() {
    return connection;
  }

  @Override
  protected void before() throws Throwable {
    this.connection = connectionSupplier.get().join();
  }

  @Override
  protected void after() {
    if (this.connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
