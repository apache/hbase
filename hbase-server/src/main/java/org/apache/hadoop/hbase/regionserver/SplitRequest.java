/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.security.PrivilegedAction;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class SplitRequest implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SplitRequest.class);
  private final RegionInfo parent;
  private final byte[] midKey;
  private final HRegionServer server;
  private final User user;

  SplitRequest(Region region, byte[] midKey, HRegionServer hrs, User user) {
    Preconditions.checkNotNull(hrs);
    this.parent = region.getRegionInfo();
    this.midKey = midKey;
    this.server = hrs;
    this.user = user;
  }

  @Override
  public String toString() {
    return "regionName=" + parent + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  private void doSplitting() {
    server.getMetrics().incrSplitRequest();
    if (user != null && user.getUGI() != null) {
      user.getUGI().doAs((PrivilegedAction<Void>) () -> {
        requestRegionSplit();
        return null;
      });
    } else {
      requestRegionSplit();
    }
  }

  private void requestRegionSplit() {
    final TableName table = parent.getTable();
    final RegionInfo hri_a = RegionInfoBuilder.newBuilder(table)
        .setStartKey(parent.getStartKey())
        .setEndKey(midKey)
        .build();
    final RegionInfo hri_b = RegionInfoBuilder.newBuilder(table)
        .setStartKey(midKey)
        .setEndKey(parent.getEndKey())
        .build();
    // Send the split request to the master. the master will do the validation on the split-key.
    // The parent region will be unassigned and the two new regions will be assigned.
    // hri_a and hri_b objects may not reflect the regions that will be created, those objects
    // are created just to pass the information to the reportRegionStateTransition().
    if (!server.reportRegionStateTransition(new RegionStateTransitionContext(
      TransitionCode.READY_TO_SPLIT, HConstants.NO_SEQNUM, -1, parent, hri_a, hri_b))) {
      LOG.error("Unable to ask master to split " + parent.getRegionNameAsString());
    }
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping split because server is stopping=" +
        this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }

    doSplitting();
  }
}
