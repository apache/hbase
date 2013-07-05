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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
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
  private static final int MAX_BATCH_SIZE = 3000;

  private final Configuration conf;
  private final HConnection conn;
  private final byte[] tableName;
  private final MetricsWALEditsReplay metrics;
  private final AtomicLong totalReplayedEdits = new AtomicLong();
  private final boolean skipErrors;
  private final int replayTimeout;

  /**
   * Create a sink for WAL log entries replay
   * @param conf
   * @param tableName
   * @param conn
   * @throws IOException
   */
  public WALEditsReplaySink(Configuration conf, byte[] tableName, HConnection conn)
      throws IOException {
    this.conf = conf;
    this.metrics = new MetricsWALEditsReplay();
    this.conn = conn;
    this.tableName = tableName;
    this.skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
      HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS);
    // a single replay operation time out and default is 60 seconds
    this.replayTimeout = conf.getInt("hbase.regionserver.logreplay.timeout", 60000);
  }

  /**
   * Replay an array of actions of the same region directly into the newly assigned Region Server
   * @param actions
   * @throws IOException
   */
  public void replayEntries(List<Pair<HRegionLocation, Row>> actions) throws IOException {
    if (actions.size() == 0) {
      return;
    }

    int batchSize = actions.size();
    int dataSize = 0;
    Map<HRegionInfo, List<Action<Row>>> actionsByRegion = 
        new HashMap<HRegionInfo, List<Action<Row>>>();
    HRegionLocation loc = null;
    Row row = null;
    List<Action<Row>> regionActions = null;
    // Build the action list. 
    for (int i = 0; i < batchSize; i++) {
      loc = actions.get(i).getFirst();
      row = actions.get(i).getSecond();
      if (actionsByRegion.containsKey(loc.getRegionInfo())) {
        regionActions = actionsByRegion.get(loc.getRegionInfo());
      } else {
        regionActions = new ArrayList<Action<Row>>();
        actionsByRegion.put(loc.getRegionInfo(), regionActions);
      }
      Action<Row> action = new Action<Row>(row, i);
      regionActions.add(action);
      dataSize += row.getRow().length;
    }
    
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    // replaying edits by region
    for (HRegionInfo curRegion : actionsByRegion.keySet()) {
      List<Action<Row>> allActions = actionsByRegion.get(curRegion);
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
    LOG.debug("number of rows:" + actions.size() + " are sent by batch! spent " + endTime
        + "(ms)!");

    metrics.updateReplayTime(endTime);
    metrics.updateReplayBatchSize(batchSize);
    metrics.updateReplayDataSize(dataSize);

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
      final List<Action<Row>> actions) throws IOException {
    try {
      ReplayServerCallable<MultiResponse> callable = new ReplayServerCallable<MultiResponse>(
          this.conn, this.tableName, regionLoc, regionInfo, actions);
      callable.withRetries();
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
  class ReplayServerCallable<R> extends ServerCallable<MultiResponse> {
    private HRegionInfo regionInfo;
    private List<Action<Row>> actions;

    private Map<HRegionLocation, Map<HRegionInfo, List<Action<Row>>>> retryActions = null;

    ReplayServerCallable(final HConnection connection, final byte [] tableName, 
        final HRegionLocation regionLoc, final HRegionInfo regionInfo,
        final List<Action<Row>> actions) {
      super(connection, tableName, null, replayTimeout);
      this.actions = actions;
      this.regionInfo = regionInfo;
      this.location = regionLoc;
    }
    
    @Override
    public MultiResponse call() throws IOException {
      try {
        replayToServer(this.regionInfo, this.actions);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
      return null;
    }

    private void replayToServer(HRegionInfo regionInfo, List<Action<Row>> actions)
        throws IOException, ServiceException {
      AdminService.BlockingInterface remoteSvr = connection.getAdmin(location.getServerName());
      MultiRequest request = RequestConverter.buildMultiRequest(regionInfo.getRegionName(),
        actions);
      MultiResponse protoResults = remoteSvr.replay(null, request);
      // check if it's a partial success
      List<ActionResult> resultList = protoResults.getResultList();
      for (int i = 0, n = resultList.size(); i < n; i++) {
        ActionResult result = resultList.get(i);
        if (result.hasException()) {
          Throwable t = ProtobufUtil.toException(result.getException());
          if (!skipErrors) {
            IOException ie = new IOException();
            ie.initCause(t);
            // retry
            throw ie;
          } else {
            LOG.warn(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS
                + "=true so continuing replayToServer with error:" + t.getMessage());
            return;
          }
        }
      }
    }

    @Override
    public void prepare(boolean reload) throws IOException {
      if (!reload) return;
      
      // relocate regions in case we have a new dead server or network hiccup
      // if not due to connection issue, the following code should run fast because it uses
      // cached location
      for (Action<Row> action : actions) {
        // use first row to relocate region because all actions are for one region
        this.location = this.connection.locateRegion(tableName, action.getAction().getRow());
        break;
      }
    }
  }
  
}
