/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;

/**
 * An abstract class that implements RegionServerObserver.
 * By extending it, you can create your own region server observer without
 * overriding all abstract methods of RegionServerObserver.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class BaseRegionServerObserver implements RegionServerObserver {

  @Override
  public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
      throws IOException { }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException { }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException { }

  @Override
  public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA,
      Region regionB) throws IOException { }

  @Override
  public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA,
      Region regionB, Region mergedRegion) throws IOException { }

  @Override
  public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB, List<Mutation> metaEntries) throws IOException { }

  @Override
  public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB, Region mergedRegion) throws IOException { }

  @Override
  public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB) throws IOException { }

  @Override
  public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB) throws IOException { }

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

  @Override
  public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

  @Override
  public ReplicationEndpoint postCreateReplicationEndPoint(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
    return endpoint;
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      List<WALEntry> entries, CellScanner cells) throws IOException { }

  @Override
  public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      List<WALEntry> entries, CellScanner cells) throws IOException { }
}
