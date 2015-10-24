/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is an example showing how a RegionObserver could configured
 * via ZooKeeper in order to control a Region compaction, flush, and scan policy.
 *
 * This also demonstrated the use of shared 
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} state.
 * See {@link RegionCoprocessorEnvironment#getSharedData()}.
 *
 * This would be useful for an incremental backup tool, which would indicate the last
 * time of a successful backup via ZK and instruct HBase to not delete data that was
 * inserted since (based on wall clock time). 
 *
 * This implements org.apache.zookeeper.Watcher directly instead of using
 * {@link org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher}, 
 * because RegionObservers come and go and currently
 * listeners registered with ZooKeeperWatcher cannot be removed.
 */
public class ZooKeeperScanPolicyObserver extends BaseRegionObserver {
  public static final String node = "/backup/example/lastbackup";
  public static final String zkkey = "ZK";
  private static final Log LOG = LogFactory.getLog(ZooKeeperScanPolicyObserver.class);

  /**
   * Internal watcher that keep "data" up to date asynchronously.
   */
  private static class ZKWatcher implements Watcher {
    private byte[] data = null;
    private ZooKeeper zk;
    private volatile boolean needSetup = true;
    private volatile long lastSetupTry = 0;

    public ZKWatcher(ZooKeeper zk) {
      this.zk = zk;
      // trigger the listening
      getData();
    }

    /**
     * Get the maintained data. In case of any ZK exceptions this will retry
     * establishing the connection (but not more than twice/minute).
     *
     * getData is on the critical path, so make sure it is fast unless there is
     * a problem (network partion, ZK ensemble down, etc)
     * Make sure at most one (unlucky) thread retries and other threads don't pile up
     * while that threads tries to recreate the connection.
     *
     * @return the last know version of the data
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION")
    public byte[] getData() {
      // try at most twice/minute
      if (needSetup && EnvironmentEdgeManager.currentTime() > lastSetupTry + 30000) {
        synchronized (this) {
          // make sure only one thread tries to reconnect
          if (needSetup) {
            needSetup = false;
          } else {
            return data;
          }
        }
        // do this without the lock held to avoid threads piling up on this lock,
        // as it can take a while
        try {
          LOG.debug("Connecting to ZK");
          // record this attempt
          lastSetupTry = EnvironmentEdgeManager.currentTime();
          if (zk.exists(node, false) != null) {
            data = zk.getData(node, this, null);
            LOG.debug("Read synchronously: "+(data == null ? "null" : Bytes.toLong(data)));
          } else {
            zk.exists(node, this);
          }
        } catch (Exception x) {
          // try again if this fails
          needSetup = true;
        }
      }
      return data;
    }

    @Override
    public void process(WatchedEvent event) {
      switch(event.getType()) {
      case NodeDataChanged:
      case NodeCreated:
      try {
        // get data and re-watch
        data = zk.getData(node, this, null);
        LOG.debug("Read asynchronously: "+(data == null ? "null" : Bytes.toLong(data)));
      } catch (InterruptedException ix) {
      } catch (KeeperException kx) {
        needSetup = true;
      }
      break;

      case NodeDeleted:
      try {
        // just re-watch
        zk.exists(node, this);
        data = null;
      } catch (InterruptedException ix) {
      } catch (KeeperException kx) {
        needSetup = true;
      }
      break;

      default:
        // ignore
      }
    }
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment) e;
    if (!re.getSharedData().containsKey(zkkey)) {
      // there is a short race here
      // in the worst case we create a watcher that will be notified once
      re.getSharedData().putIfAbsent(
          zkkey,
          new ZKWatcher(re.getRegionServerServices().getZooKeeper()
              .getRecoverableZooKeeper().getZooKeeper()));
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    // nothing to do here
  }

  protected ScanInfo getScanInfo(Store store, RegionCoprocessorEnvironment e) {
    byte[] data = ((ZKWatcher)e.getSharedData().get(zkkey)).getData();
    if (data == null) {
      return null;
    }
    ScanInfo oldSI = store.getScanInfo();
    if (oldSI.getTtl() == Long.MAX_VALUE) {
      return null;
    }
    long ttl = Math.max(EnvironmentEdgeManager.currentTime() -
        Bytes.toLong(data), oldSI.getTtl());
    return new ScanInfo(oldSI.getConfiguration(), store.getFamily(), ttl,
        oldSI.getTimeToPurgeDeletes(), oldSI.getComparator());
  }

  @Override
  public InternalScanner preFlushScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    ScanInfo scanInfo = getScanInfo(store, c.getEnvironment());
    if (scanInfo == null) {
      // take default action
      return null;
    }
    Scan scan = new Scan();
    scan.setMaxVersions(scanInfo.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, Collections.singletonList(memstoreScanner),
        ScanType.COMPACT_RETAIN_DELETES, store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
  }

  @Override
  public InternalScanner preCompactScannerOpen(
      final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    ScanInfo scanInfo = getScanInfo(store, c.getEnvironment());
    if (scanInfo == null) {
      // take default action
      return null;
    }
    Scan scan = new Scan();
    scan.setMaxVersions(scanInfo.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, scanners, scanType,
        store.getSmallestReadPoint(), earliestPutTs);
  }

  @Override
  public KeyValueScanner preStoreScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final Scan scan, final NavigableSet<byte[]> targetCols,
      final KeyValueScanner s) throws IOException {
    ScanInfo scanInfo = getScanInfo(store, c.getEnvironment());
    if (scanInfo == null) {
      // take default action
      return null;
    }
    return new StoreScanner(store, scanInfo, scan, targetCols,
      ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
  }
}
