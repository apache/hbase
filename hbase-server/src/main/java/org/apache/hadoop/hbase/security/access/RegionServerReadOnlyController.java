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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class RegionServerReadOnlyController extends AbstractReadOnlyController
  implements RegionServerObserver, RegionServerCoprocessor {

  @Override
  public Optional<RegionServerObserver> getRegionServerObserver() {
    return Optional.of(this);
  }

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preRollWALWriterRequest(ctx);
  }

  @Override
  public void preReplicationSinkBatchMutate(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
    AdminProtos.WALEntry walEntry, Mutation mutation) throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preReplicateLogEntries(ctx);
  }

}
