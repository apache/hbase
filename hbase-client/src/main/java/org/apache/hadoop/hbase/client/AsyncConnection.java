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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The asynchronous version of Connection.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncConnection extends Closeable {

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Retrieve a AsyncRegionLocator implementation to inspect region information on a table. The
   * returned AsyncRegionLocator is not thread-safe, so a new instance should be created for each
   * using thread. This is a lightweight operation. Pooling or caching of the returned
   * AsyncRegionLocator is neither required nor desired.
   * @param tableName Name of the table who's region is to be examined
   * @return An AsyncRegionLocator instance
   */
  AsyncTableRegionLocator getRegionLocator(TableName tableName);

  /**
   * Clear all the entries in the region location cache, for all the tables.
   * <p/>
   * If you only want to clear the cache for a specific table, use
   * {@link AsyncTableRegionLocator#clearRegionLocationCache()}.
   * <p/>
   * This may cause performance issue so use it with caution.
   */
  void clearRegionLocationCache();

  /**
   * Retrieve an {@link AsyncTable} implementation for accessing a table.
   * <p>
   * The returned instance will use default configs. Use {@link #getTableBuilder(TableName)} if
   * you want to customize some configs.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * <p>
   * The returned {@code CompletableFuture} will be finished directly in the rpc framework's
   * callback thread, so typically you should not do any time consuming work inside these methods.
   * And also the observer style scan API will use {@link AdvancedScanResultConsumer} which is
   * designed for experts only. Only use it when you know what you are doing.
   * @param tableName the name of the table
   * @return an AsyncTable to use for interactions with this table
   * @see #getTableBuilder(TableName)
   */
  default AsyncTable<AdvancedScanResultConsumer> getTable(TableName tableName) {
    return getTableBuilder(tableName).build();
  }

  /**
   * Returns an {@link AsyncTableBuilder} for creating {@link AsyncTable}.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * @param tableName the name of the table
   */
  AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName);

  /**
   * Retrieve an {@link AsyncTable} implementation for accessing a table.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * @param tableName the name of the table
   * @param pool the thread pool to use for executing callback
   * @return an AsyncTable to use for interactions with this table
   */
  default AsyncTable<ScanResultConsumer> getTable(TableName tableName, ExecutorService pool) {
    return getTableBuilder(tableName, pool).build();
  }

  /**
   * Returns an {@link AsyncTableBuilder} for creating {@link AsyncTable}.
   * <p>
   * This method no longer checks table existence. An exception will be thrown if the table does not
   * exist only when the first operation is attempted.
   * @param tableName the name of the table
   * @param pool the thread pool to use for executing callback
   */
  AsyncTableBuilder<ScanResultConsumer> getTableBuilder(TableName tableName, ExecutorService pool);

  /**
   * Retrieve an {@link AsyncAdmin} implementation to administer an HBase cluster.
   * <p>
   * The returned instance will use default configs. Use {@link #getAdminBuilder()} if you want to
   * customize some configs.
   * <p>
   * The admin operation's returned {@code CompletableFuture} will be finished directly in the rpc
   * framework's callback thread, so typically you should not do any time consuming work inside
   * these methods.
   * @return an {@link AsyncAdmin} instance for cluster administration
   */
  default AsyncAdmin getAdmin() {
    return getAdminBuilder().build();
  }

  /**
   * Returns an {@link AsyncAdminBuilder} for creating {@link AsyncAdmin}.
   * <p>
   * The admin operation's returned {@code CompletableFuture} will be finished directly in the rpc
   * framework's callback thread, so typically you should not do any time consuming work inside
   * these methods.
   */
  AsyncAdminBuilder getAdminBuilder();

  /**
   * Retrieve an {@link AsyncAdmin} implementation to administer an HBase cluster.
   * <p>
   * The returned instance will use default configs. Use {@link #getAdminBuilder(ExecutorService)}
   * if you want to customize some configs.
   * @param pool the thread pool to use for executing callback
   * @return an {@link AsyncAdmin} instance for cluster administration
   */
  default AsyncAdmin getAdmin(ExecutorService pool) {
    return getAdminBuilder(pool).build();
  }

  /**
   * Returns an {@link AsyncAdminBuilder} for creating {@link AsyncAdmin}.
   * @param pool the thread pool to use for executing callback
   */
  AsyncAdminBuilder getAdminBuilder(ExecutorService pool);

  /**
   * Retrieve an {@link AsyncBufferedMutator} for performing client-side buffering of writes.
   * <p>
   * The returned instance will use default configs. Use
   * {@link #getBufferedMutatorBuilder(TableName)} if you want to customize some configs.
   * @param tableName the name of the table
   * @return an {@link AsyncBufferedMutator} for the supplied tableName.
   */
  default AsyncBufferedMutator getBufferedMutator(TableName tableName) {
    return getBufferedMutatorBuilder(tableName).build();
  }

  /**
   * Returns an {@link AsyncBufferedMutatorBuilder} for creating {@link AsyncBufferedMutator}.
   * @param tableName the name of the table
   */
  AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName);

  /**
   * Retrieve an {@link AsyncBufferedMutator} for performing client-side buffering of writes.
   * <p>
   * The returned instance will use default configs. Use
   * {@link #getBufferedMutatorBuilder(TableName, ExecutorService)} if you want to customize some
   * configs.
   * @param tableName the name of the table
   * @param pool the thread pool to use for executing callback
   * @return an {@link AsyncBufferedMutator} for the supplied tableName.
   */
  default AsyncBufferedMutator getBufferedMutator(TableName tableName, ExecutorService pool) {
    return getBufferedMutatorBuilder(tableName, pool).build();
  }

  /**
   * Returns an {@link AsyncBufferedMutatorBuilder} for creating {@link AsyncBufferedMutator}.
   * @param tableName the name of the table
   * @param pool the thread pool to use for executing callback
   */
  AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName, ExecutorService pool);

  /**
   * Returns whether the connection is closed or not.
   * @return true if this connection is closed
   */
  boolean isClosed();

  /**
   * Retrieve an Hbck implementation to fix an HBase cluster. The returned Hbck is not guaranteed to
   * be thread-safe. A new instance should be created by each thread. This is a lightweight
   * operation. Pooling or caching of the returned Hbck instance is not recommended.
   * <p/>
   * The caller is responsible for calling {@link Hbck#close()} on the returned Hbck instance.
   * <p/>
   * This will be used mostly by hbck tool.
   * @return an Hbck instance for active master. Active master is fetched from the zookeeper.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.HBCK)
  CompletableFuture<Hbck> getHbck();

  /**
   * Retrieve an Hbck implementation to fix an HBase cluster. The returned Hbck is not guaranteed to
   * be thread-safe. A new instance should be created by each thread. This is a lightweight
   * operation. Pooling or caching of the returned Hbck instance is not recommended.
   * <p/>
   * The caller is responsible for calling {@link Hbck#close()} on the returned Hbck instance.
   * <p/>
   * This will be used mostly by hbck tool. This may only be used to by pass getting registered
   * master from ZK. In situations where ZK is not available or active master is not registered with
   * ZK and user can get master address by other means, master can be explicitly specified.
   * @param masterServer explicit {@link ServerName} for master server
   * @return an Hbck instance for a specified master server
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.HBCK)
  Hbck getHbck(ServerName masterServer) throws IOException;
}
