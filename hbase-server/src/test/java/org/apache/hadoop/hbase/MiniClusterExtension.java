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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An {@link Extension} that manages an instance of the {@link MiniHBaseCluster}. Built on top of an
 * instance of {@link HBaseTestingUtility}, so be weary of intermixing direct use of that class with
 * this Extension.
 * </p>
 * Use in combination with {@link ConnectionExtension}, for example:
 *
 * <pre>
 * {
 *   &#64;code
 *   public class TestMyClass {
 *
 *     &#64;RegisterExtension
 *     public static final MiniClusterExtension miniClusterExtension =
 *       MiniClusterExtension.newBuilder().build();
 *
 *     &#64;RegisterExtension
 *     public final ConnectionExtension connectionExtension = ConnectionExtension
 *       .createAsyncConnectionExtension(miniClusterExtension::createAsyncConnection);
 *   }
 * }
 * </pre>
 */
public final class MiniClusterExtension implements BeforeAllCallback, AfterAllCallback {

  /**
   * A builder for fluent composition of a new {@link MiniClusterExtension}.
   */
  public static final class Builder {

    private StartMiniClusterOption miniClusterOption;
    private Configuration conf;

    /**
     * Use the provided {@link StartMiniClusterOption} to construct the {@link MiniHBaseCluster}.
     */
    public Builder setMiniClusterOption(final StartMiniClusterOption miniClusterOption) {
      this.miniClusterOption = miniClusterOption;
      return this;
    }

    /**
     * Seed the underlying {@link HBaseTestingUtility} with the provided {@link Configuration}.
     */
    public Builder setConfiguration(final Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setConfiguration(final Supplier<Configuration> supplier) {
      return setConfiguration(supplier.get());
    }

    public MiniClusterExtension build() {
      return new MiniClusterExtension(conf,
        miniClusterOption != null ? miniClusterOption : StartMiniClusterOption.builder().build());
    }
  }

  /**
   * Returns the underlying instance of {@link HBaseTestingUtility}
   */
  private final HBaseTestingUtility testingUtility;
  private final StartMiniClusterOption miniClusterOptions;

  private MiniHBaseCluster miniCluster;

  private MiniClusterExtension(final Configuration conf,
    final StartMiniClusterOption miniClusterOptions) {
    this.testingUtility = new HBaseTestingUtility(conf);
    this.miniClusterOptions = miniClusterOptions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the underlying instance of {@link HBaseTestingUtility}
   */
  public HBaseTestingUtility getTestingUtility() {
    return testingUtility;
  }

  /**
   * Create a {@link Connection} to the managed {@link MiniHBaseCluster}. It's up to the caller to
   * {@link Connection#close() close()} the connection when finished.
   */
  public Connection createConnection() {
    if (miniCluster == null) {
      throw new IllegalStateException("test cluster not initialized");
    }
    try {
      return ConnectionFactory.createConnection(miniCluster.getConf());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link AsyncConnection} to the managed {@link MiniHBaseCluster}. It's up to the caller
   * to {@link AsyncConnection#close() close()} the connection when finished.
   */
  public CompletableFuture<AsyncConnection> createAsyncConnection() {
    if (miniCluster == null) {
      throw new IllegalStateException("test cluster not initialized");
    }
    return ConnectionFactory.createAsyncConnection(miniCluster.getConf());
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    miniCluster = testingUtility.startMiniCluster(miniClusterOptions);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    try {
      testingUtility.shutdownMiniCluster();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
