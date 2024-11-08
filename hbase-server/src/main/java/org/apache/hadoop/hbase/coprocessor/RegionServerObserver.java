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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

/**
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.regionserver.HRegionServer} process. Since most implementations
 * will be interested in only a subset of hooks, this class uses 'default' functions to avoid having
 * to add unnecessary overrides. When the functions are non-empty, it's simply to satisfy the
 * compiler by returning value of expected (non-void) type. It is done in a way that these default
 * definitions act as no-op. So our suggestion to implementation would be to not call these
 * 'default' methods from overrides. <br>
 * <br>
 * <h3>Exception Handling</h3> For all functions, exception handling is done as follows:
 * <ul>
 * <li>Exceptions of type {@link IOException} are reported back to client.</li>
 * <li>For any other kind of exception:
 * <ul>
 * <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then the
 * server aborts.</li>
 * <li>Otherwise, coprocessor is removed from the server and
 * {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 * </ul>
 * </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionServerObserver {
  /**
   * Called before stopping region server.
   * @param ctx the environment to interact with the framework and region server.
   */
  default void preStopRegionServer(final ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * This will be called before executing user request to roll a region server WAL.
   * @param ctx the environment to interact with the framework and region server.
   */
  default void preRollWALWriterRequest(
    final ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
  }

  /**
   * This will be called after executing user request to roll a region server WAL.
   * @param ctx the environment to interact with the framework and region server.
   */
  default void postRollWALWriterRequest(
    final ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
  }

  /**
   * This will be called after the replication endpoint is instantiated.
   * @param ctx      the environment to interact with the framework and region server.
   * @param endpoint - the base endpoint for replication
   * @return the endpoint to use during replication.
   */
  default ReplicationEndpoint postCreateReplicationEndPoint(
    ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
    return endpoint;
  }

  // TODO remove below 2 hooks when we implement AC as a core impl than a CP impl.
  /**
   * This will be called before executing replication request to shipping log entries.
   * @param ctx the environment to interact with the framework and region server.
   * @deprecated As of release 2.0.0 with out any replacement, plan to remove in 4.0.0. This is
   *             maintained for internal usage by AccessController. Do not use these hooks in custom
   *             co-processors.
   */
  @Deprecated
  default void preReplicateLogEntries(final ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * This will be called after executing replication request to shipping log entries.
   * @param ctx the environment to interact with the framework and region server.
   * @deprecated As of release 2.0.0 with out any replacement, plan to remove in 4.0.0. This is
   *             maintained for internal usage by AccessController. Do not use these hooks in custom
   *             co-processors.
   */
  @Deprecated
  default void postReplicateLogEntries(
    final ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
  }

  /**
   * This will be called before clearing compaction queues
   * @param ctx the environment to interact with the framework and region server.
   */
  default void preClearCompactionQueues(
    final ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
  }

  /**
   * This will be called after clearing compaction queues
   * @param ctx the environment to interact with the framework and region server.
   */
  default void postClearCompactionQueues(
    final ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
  }

  /**
   * This will be called before executing procedures
   * @param ctx the environment to interact with the framework and region server.
   */
  default void preExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * This will be called after executing procedures
   * @param ctx the environment to interact with the framework and region server.
   */
  default void postExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * This will be called before replication sink mutations are executed on the sink table as part of
   * batch call.
   * @param ctx      the environment to interact with the framework and region server.
   * @param walEntry wal entry from which mutation is formed.
   * @param mutation mutation to be applied at sink cluster.
   * @throws IOException if something goes wrong.
   */
  default void preReplicationSinkBatchMutate(
    ObserverContext<RegionServerCoprocessorEnvironment> ctx, AdminProtos.WALEntry walEntry,
    Mutation mutation) throws IOException {

  }

  /**
   * This will be called after replication sink mutations are executed on the sink table as part of
   * batch call.
   * @param ctx      the environment to interact with the framework and region server.
   * @param walEntry wal entry from which mutation is formed.
   * @param mutation mutation to be applied at sink cluster.
   * @throws IOException if something goes wrong.
   */
  default void postReplicationSinkBatchMutate(
    ObserverContext<RegionServerCoprocessorEnvironment> ctx, AdminProtos.WALEntry walEntry,
    Mutation mutation) throws IOException {

  }

  /**
   * Called before clearing the block caches for one or more regions
   * @param ctx the coprocessor instance's environment
   * @throws IOException if you need to signal an IO error
   */
  default void preClearRegionBlockCache(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after clearing the block caches for one or more regions
   * @param ctx   the coprocessor instance's environment
   * @param stats statistics about the cache evictions that happened
   * @throws IOException if you need to signal an IO error
   */
  default void postClearRegionBlockCache(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
    CacheEvictionStats stats) throws IOException {
  }

  /**
   * Called before reloading the RegionServer's {@link Configuration} from disk
   * @param ctx           the coprocessor instance's environment
   * @param preReloadConf the {@link Configuration} in use prior to reload
   * @throws IOException if you need to signal an IO error
   */
  default void preUpdateRegionServerConfiguration(
    ObserverContext<RegionServerCoprocessorEnvironment> ctx, Configuration preReloadConf)
    throws IOException {
  }

  /**
   * Called after reloading the RegionServer's {@link Configuration} from disk
   * @param ctx            the coprocessor instance's environment
   * @param postReloadConf the {@link Configuration} that was loaded
   * @throws IOException if you need to signal an IO error
   */
  default void postUpdateRegionServerConfiguration(
    ObserverContext<RegionServerCoprocessorEnvironment> ctx, Configuration postReloadConf)
    throws IOException {
  }

}
