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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RegionServerCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ServiceException;

/**
 * This class is responsible for replaying the edits coming from a failed region server.
 * <p/>
 * This class uses the native HBase client in order to replay WAL entries.
 * <p/>
 */
@InterfaceAudience.Private
public class WALEditsReplaySink {

  private static final Log LOG = LogFactory.getLog(WALEditsReplaySink.class);
  private static final int MAX_BATCH_SIZE = 1024;

  private final Configuration conf;
  private final HConnection conn;
  private final TableName tableName;
  private final MetricsWALEditsReplay metrics;
  private final AtomicLong totalReplayedEdits = new AtomicLong();
  private final boolean skipErrors;
  private final int replayTimeout;
  private RpcControllerFactory rpcControllerFactory;

  /**
   * Create a sink for WAL log entries replay
   * @param conf
   * @param tableName
   * @param conn
   * @throws IOException
   */
  public WALEditsReplaySink(Configuration conf, TableName tableName, HConnection conn)
      throws IOException {
    this.conf = conf;
    this.metrics = new MetricsWALEditsReplay();
    this.conn = conn;
    this.tableName = tableName;
    this.skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
      HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS);
    // a single replay operation time out and default is 60 seconds
    this.replayTimeout = conf.getInt("hbase.regionserver.logreplay.timeout", 60000);
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
  }

  /**
   * Replay an array of actions of the same region directly into the newly assigned Region Server
   * @param entries
   * @throws IOException
   */
  public void replayEntries(List<Pair<HRegionLocation, HLog.Entry>> entries) throws IOException {
    if (entries.size() == 0) {
      return;
    }

    int batchSize = entries.size();
    Map<HRegionInfo, List<HLog.Entry>> entriesByRegion =
        new HashMap<HRegionInfo, List<HLog.Entry>>();
    HRegionLocation loc = null;
    HLog.Entry entry = null;
    List<HLog.Entry> regionEntries = null;
    // Build the action list.
    for (int i = 0; i < batchSize; i++) {
      loc = entries.get(i).getFirst();
      entry = entries.get(i).getSecond();
      if (entriesByRegion.containsKey(loc.getRegionInfo())) {
        regionEntries = entriesByRegion.get(loc.getRegionInfo());
      } else {
        regionEntries = new ArrayList<HLog.Entry>();
        entriesByRegion.put(loc.getRegionInfo(), regionEntries);
      }
      regionEntries.add(entry);
    }

    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    // replaying edits by region
    for (Map.Entry<HRegionInfo, List<HLog.Entry>> _entry : entriesByRegion.entrySet()) {
      HRegionInfo curRegion = _entry.getKey();
      List<HLog.Entry> allActions = _entry.getValue();
      // send edits in chunks
      int totalActions = allActions.size();
      int replayedActions = 0;
      int curBatchSize = 0;
      for (; replayedActions < totalActions;) {
        curBatchSize = (totalActions > (MAX_BATCH_SIZE + replayedActions)) ? MAX_BATCH_SIZE
                : (totalActions - replayedActions);
        replayEdits(loc, curRegion, allActions.subList(replayedActions,
          replayedActions + curBatchSize));
        replayedActions += curBatchSize;
      }
    }

    long endTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
    LOG.debug("number of rows:" + entries.size() + " are sent by batch! spent " + endTime
        + "(ms)!");

    metrics.updateReplayTime(endTime);
    metrics.updateReplayBatchSize(batchSize);

    this.totalReplayedEdits.addAndGet(batchSize);
  }

  /**
   * Get a string representation of this sink's metrics
   * @return string with the total replayed edits count
   */
  public String getStats() {
    return this.totalReplayedEdits.get() == 0 ? "" : "Sink: total replayed edits: "
        + this.totalReplayedEdits;
  }

  private void replayEdits(final HRegionLocation regionLoc, final HRegionInfo regionInfo,
      final List<HLog.Entry> entries) throws IOException {
    try {
      RpcRetryingCallerFactory factory = RpcRetryingCallerFactory.instantiate(conf);
      ReplayServerCallable<ReplicateWALEntryResponse> callable =
          new ReplayServerCallable<ReplicateWALEntryResponse>(this.conn, this.tableName, regionLoc,
              regionInfo, entries);
      factory.<ReplicateWALEntryResponse> newCaller().callWithRetries(callable, this.replayTimeout);
    } catch (IOException ie) {
      if (skipErrors) {
        LOG.warn(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS
            + "=true so continuing replayEdits with error:" + ie.getMessage());
      } else {
        throw ie;
      }
    }
  }

  /**
   * Callable that handles the <code>replay</code> method call going against a single regionserver
   * @param <R>
   */
  class ReplayServerCallable<R> extends RegionServerCallable<ReplicateWALEntryResponse> {
    private HRegionInfo regionInfo;
    private List<HLog.Entry> entries;

    ReplayServerCallable(final HConnection connection, final TableName tableName,
        final HRegionLocation regionLoc, final HRegionInfo regionInfo,
        final List<HLog.Entry> entries) {
      super(connection, tableName, null);
      this.entries = entries;
      this.regionInfo = regionInfo;
      setLocation(regionLoc);
    }

    @Override
    public ReplicateWALEntryResponse call() throws IOException {
      try {
        replayToServer(this.regionInfo, this.entries);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
      return null;
    }

    private void replayToServer(HRegionInfo regionInfo, List<HLog.Entry> entries)
        throws IOException, ServiceException {
      if (entries.isEmpty()) return;

      HLog.Entry[] entriesArray = new HLog.Entry[entries.size()];
      entriesArray = entries.toArray(entriesArray);
      AdminService.BlockingInterface remoteSvr = conn.getAdmin(getLocation().getServerName());

      Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p =
          ReplicationProtbufUtil.buildReplicateWALEntryRequest(entriesArray);
      try {
        PayloadCarryingRpcController controller = new PayloadCarryingRpcController(p.getSecond());
        remoteSvr.replay(controller, p.getFirst());
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
    }

    @Override
    public void prepare(boolean reload) throws IOException {
      if (!reload) return;
      // relocate regions in case we have a new dead server or network hiccup
      // if not due to connection issue, the following code should run fast because it uses
      // cached location
      boolean skip = false;
      for (HLog.Entry entry : this.entries) {
        WALEdit edit = entry.getEdit();
        List<KeyValue> kvs = edit.getKeyValues();
        for (KeyValue kv : kvs) {
          // filtering HLog meta entries
          setLocation(conn.locateRegion(tableName, kv.getRow()));
          skip = true;
          break;
        }
        // use first log entry to relocate region because all entries are for one region
        if (skip) break;
      }
    }
  }
}
