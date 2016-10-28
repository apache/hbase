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

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.base.Preconditions;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class SplitRequest implements Runnable {
  private static final Log LOG = LogFactory.getLog(SplitRequest.class);
  private final HRegion parent;
  private final byte[] midKey;
  private final HRegionServer server;
  private final User user;

  SplitRequest(Region region, byte[] midKey, HRegionServer hrs, User user) {
    Preconditions.checkNotNull(hrs);
    this.parent = (HRegion)region;
    this.midKey = midKey;
    this.server = hrs;
    this.user = user;
  }

  @Override
  public String toString() {
    return "regionName=" + parent + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  private void doSplitting() {
    boolean success = false;
    server.metricsRegionServer.incrSplitRequest();
    long startTime = EnvironmentEdgeManager.currentTime();

    try {
      long procId;
      if (user != null && user.getUGI() != null) {
        procId = user.getUGI().doAs (new PrivilegedAction<Long>() {
          @Override
          public Long run() {
            try {
              return server.requestRegionSplit(parent.getRegionInfo(), midKey);
            } catch (Exception e) {
              LOG.error("Failed to complete region split ", e);
            }
            return (long)-1;
          }
        });
      } else {
        procId = server.requestRegionSplit(parent.getRegionInfo(), midKey);
      }

      if (procId != -1) {
        // wait for the split to complete or get interrupted.  If the split completes successfully,
        // the procedure will return true; if the split fails, the procedure would throw exception.
        //
        try {
          while (!(success = server.isProcedureFinished(procId))) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              LOG.warn("Split region " + parent + " is still in progress.  Not waiting...");
              break;
            }
          }
        } catch (IOException e) {
          LOG.error("Split region " + parent + " failed.", e);
        }
      } else {
        LOG.error("Fail to split region " + parent);
      }
    } finally {
      if (this.parent.getCoprocessorHost() != null) {
        try {
          this.parent.getCoprocessorHost().postCompleteSplit();
        } catch (IOException io) {
          LOG.error("Split failed " + this,
            io instanceof RemoteException ? ((RemoteException) io).unwrapRemoteException() : io);
        }
      }

      // Update regionserver metrics with the split transaction total running time
      server.metricsRegionServer.updateSplitTime(EnvironmentEdgeManager.currentTime() - startTime);

      if (parent.shouldForceSplit()) {
        parent.clearSplit();
      }

      if (success) {
        server.metricsRegionServer.incrSplitSuccess();
      }
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
