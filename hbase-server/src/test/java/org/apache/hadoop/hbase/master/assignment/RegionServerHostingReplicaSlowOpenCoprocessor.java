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
package org.apache.hadoop.hbase.master.assignment;

import java.util.Optional;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This coprocessor is used to slow down opening of the replica regions.
 */
public class RegionServerHostingReplicaSlowOpenCoprocessor
  implements RegionCoprocessor, RegionObserver {

  private static final Logger LOG =
    LoggerFactory.getLogger(RegionServerHostingReplicaSlowOpenCoprocessor.class);

  static volatile boolean slowDownReplicaOpen = false;

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    int replicaId = c.getEnvironment().getRegion().getRegionInfo().getReplicaId();
    if (replicaId != RegionInfo.DEFAULT_REPLICA_ID) {
      while (slowDownReplicaOpen) {
        LOG.info("Slow down replica region open a bit");
        try {
          Thread.sleep(250);
        } catch (InterruptedException ignored) {
          return;
        }
      }
    }
  }
}
