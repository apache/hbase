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
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * A {@link TestRule} that manages an instance of the {@link SingleProcessHBaseCluster}. Can be used
 * in either the {@link Rule} or {@link ClassRule} positions. Built on top of an instance of
 * {@link HBaseTestingUtil}, so be weary of intermixing direct use of that class with this Rule.
 * </p>
 * Use in combination with {@link ConnectionRule}, for example:
 *
 * <pre>
 * {
 *   &#64;code
 *   public class TestMyClass {
 *     &#64;ClassRule
 *     public static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder().build();
 *
 *     &#64;Rule
 *     public final ConnectionRule connectionRule =
 *       ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);
 *   }
 * }
 * </pre>
 */
public final class MiniClusterRule extends ExternalResource {

  /**
   * A builder for fluent composition of a new {@link MiniClusterRule}.
   */
  public static class Builder {

    private StartTestingClusterOption miniClusterOption;
    private Configuration conf;

    /**
     * Use the provided {@link StartTestingClusterOption} to construct the
     * {@link SingleProcessHBaseCluster}.
     */
    public Builder setMiniClusterOption(final StartTestingClusterOption miniClusterOption) {
      this.miniClusterOption = miniClusterOption;
      return this;
    }

    /**
     * Seed the underlying {@link HBaseTestingUtil} with the provided {@link Configuration}.
     */
    public Builder setConfiguration(final Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setConfiguration(Supplier<Configuration> supplier) {
      return setConfiguration(supplier.get());
    }

    public MiniClusterRule build() {
      return new MiniClusterRule(conf, miniClusterOption != null ? miniClusterOption :
        StartTestingClusterOption.builder().build());
    }
  }

  private final HBaseTestingUtil testingUtility;
  private final StartTestingClusterOption miniClusterOptions;

  private SingleProcessHBaseCluster miniCluster;

  private MiniClusterRule(final Configuration conf,
    final StartTestingClusterOption miniClusterOptions) {
    this.testingUtility = new HBaseTestingUtil(conf);
    this.miniClusterOptions = miniClusterOptions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the underlying instance of {@link HBaseTestingUtil}
   */
  public HBaseTestingUtil getTestingUtility() {
    return testingUtility;
  }

  /**
   * Create a {@link Connection} to the managed {@link SingleProcessHBaseCluster}. It's up to
   * the caller to {@link Connection#close() close()} the connection when finished.
   */
  public Connection createConnection() {
    try {
      return createAsyncConnection().get().toConnection();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link AsyncConnection} to the managed {@link SingleProcessHBaseCluster}. It's up to
   * the caller to {@link AsyncConnection#close() close()} the connection when finished.
   */
  public CompletableFuture<AsyncConnection> createAsyncConnection() {
    if (miniCluster == null) {
      throw new IllegalStateException("test cluster not initialized");
    }
    return ConnectionFactory.createAsyncConnection(miniCluster.getConf());
  }

  @Override
  protected void before() throws Throwable {
    miniCluster = testingUtility.startMiniCluster(miniClusterOptions);
  }

  @Override
  protected void after() {
    try {
      testingUtility.shutdownMiniCluster();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
