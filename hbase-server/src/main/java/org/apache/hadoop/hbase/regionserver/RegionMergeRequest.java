/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.security.PrivilegedAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles processing region merges. Put in a queue, owned by HRegionServer.
 */
// TODO:UNUSED: REMOVE!!!
@InterfaceAudience.Private
class RegionMergeRequest implements Runnable {
  private static final Log LOG = LogFactory.getLog(RegionMergeRequest.class);
  private final RegionInfo region_a;
  private final RegionInfo region_b;
  private final HRegionServer server;
  private final boolean forcible;
  private final User user;

  RegionMergeRequest(Region a, Region b, HRegionServer hrs, boolean forcible,
      long masterSystemTime, User user) {
    Preconditions.checkNotNull(hrs);
    this.region_a = a.getRegionInfo();
    this.region_b = b.getRegionInfo();
    this.server = hrs;
    this.forcible = forcible;
    this.user = user;
  }

  @Override
  public String toString() {
    return "MergeRequest,regions:" + region_a + ", " + region_b + ", forcible="
        + forcible;
  }

  private void doMerge() {
    boolean success = false;
    //server.metricsRegionServer.incrMergeRequest();

    if (user != null && user.getUGI() != null) {
      user.getUGI().doAs (new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          requestRegionMerge();
          return null;
        }
      });
    } else {
      requestRegionMerge();
    }
  }

  private void requestRegionMerge() {
    final TableName table = region_a.getTable();
    if (!table.equals(region_b.getTable())) {
      LOG.error("Can't merge regions from two different tables: " + region_a + ", " + region_b);
      return;
    }

    // TODO: fake merged region for compat with the report protocol
    final RegionInfo merged = RegionInfoBuilder.newBuilder(table).build();

    // Send the split request to the master. the master will do the validation on the split-key.
    // The parent region will be unassigned and the two new regions will be assigned.
    // hri_a and hri_b objects may not reflect the regions that will be created, those objectes
    // are created just to pass the information to the reportRegionStateTransition().
    if (!server.reportRegionStateTransition(TransitionCode.READY_TO_MERGE, merged, region_a, region_b)) {
      LOG.error("Unable to ask master to merge: " + region_a + ", " + region_b);
    }
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping merge because server is stopping="
          + this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }

    doMerge();
  }
}
