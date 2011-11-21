/**
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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.util.Writables;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.List;

/**
 * Region server schema change tracker. RS uses this tracker to keep track of
 * alter schema requests from master and updates the status once the schema change
 * is complete.
 */
public class SchemaChangeTracker extends ZooKeeperNodeTracker {
  public static final Log LOG = LogFactory.getLog(SchemaChangeTracker.class);
  private RegionServerServices regionServer = null;
  private volatile int sleepTimeMillis = 0;


  /**
   * Constructs a new ZK node tracker.
   * <p/>
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param node
   * @param abortable
   */
  public SchemaChangeTracker(ZooKeeperWatcher watcher,
                             Abortable abortable,
                             RegionServerServices regionServer) {
    super(watcher, watcher.schemaZNode, abortable);
    this.regionServer = regionServer;
  }

  @Override
  public void start() {
    try {
      watcher.registerListener(this);
      ZKUtil.listChildrenAndWatchThem(watcher, node);
      // Clean-up old in-process schema changes for this RS now?
    } catch (KeeperException e) {
      LOG.error("RegionServer SchemaChangeTracker startup failed with " +
          "KeeperException.", e);
    }
  }


  /**
   * This event will be triggered whenever new schema change request is processed by the
   * master. The path will be of the format /hbase/schema/<table name>
   * @param path full path of the node whose children have changed
   */
  @Override
  public void nodeChildrenChanged(String path) {
    LOG.debug("NodeChildrenChanged. Path = " + path);
    if (path.equals(watcher.schemaZNode)) {
      try {
        List<String> tables =
          ZKUtil.listChildrenAndWatchThem(watcher, watcher.schemaZNode);
        LOG.debug("RS.SchemaChangeTracker: " +
            "Current list of tables with schema change = " + tables);
        if (tables != null) {
          handleSchemaChange(tables);
        } else {
          LOG.error("No tables found for schema change event." +
              " Skipping instant schema refresh");
        }
      } catch (KeeperException ke) {
        String errmsg = "KeeperException while handling nodeChildrenChanged for path = "
            + path + " Cause = " + ke.getCause();
        LOG.error(errmsg, ke);
        TaskMonitor.get().createStatus(errmsg);
      }
    }
  }

  private void handleSchemaChange(List<String> tables) {
    for (String tableName : tables) {
      if (tableName != null) {
        LOG.debug("Processing schema change with status for table = " + tableName);
        handleSchemaChange(tableName);
      }
    }
  }

  private void handleSchemaChange(String tableName) {
    int refreshedRegionsCount = 0, onlineRegionsCount = 0;
    MonitoredTask status = null;
    try {
      List<HRegion> onlineRegions =
          regionServer.getOnlineRegions(Bytes.toBytes(tableName));
      if (onlineRegions != null && !onlineRegions.isEmpty()) {
        status = TaskMonitor.get().createStatus("Region server "
             + regionServer.getServerName().getServerName()
             + " handling schema change for table = " + tableName
             + " number of online regions = " + onlineRegions.size());
        onlineRegionsCount = onlineRegions.size();
        createStateNode(tableName, onlineRegions.size());
        for (HRegion hRegion : onlineRegions) {
          regionServer.refreshRegion(hRegion);
          refreshedRegionsCount ++;
        }
        SchemaAlterStatus alterStatus = getSchemaAlterStatus(tableName);
        alterStatus.update(SchemaAlterStatus.AlterState.SUCCESS, refreshedRegionsCount);
        updateSchemaChangeStatus(tableName, alterStatus);
        String msg = "Refresh schema completed for table name = " + tableName
        + " server = " + regionServer.getServerName().getServerName()
        + " online Regions = " + onlineRegions.size()
        + " refreshed Regions = " + refreshedRegionsCount;
        LOG.debug(msg);
        status.setStatus(msg);
      } else {
        LOG.debug("Server " + regionServer.getServerName().getServerName()
         + " has no online regions for table = " + tableName
         + " Ignoring the schema change request");
      }
    } catch (IOException ioe) {
      reportAndLogSchemaRefreshError(tableName, onlineRegionsCount,
          refreshedRegionsCount, ioe, status);
    } catch (KeeperException ke) {
      reportAndLogSchemaRefreshError(tableName, onlineRegionsCount,
          refreshedRegionsCount, ke, status);
    }
  }

  private int getZKNodeVersion(String nodePath) throws KeeperException {
    return ZKUtil.checkExists(this.watcher, nodePath);
  }

  private void reportAndLogSchemaRefreshError(String tableName,
                                              int onlineRegionsCount,
                                              int refreshedRegionsCount,
                                              Throwable exception,
                                              MonitoredTask status) {
    try {
      String errmsg =
          " Region Server " + regionServer.getServerName().getServerName()
              + " failed during schema change process. Cause = "
              + exception.getCause()
              + " Number of onlineRegions = " + onlineRegionsCount
              + " Processed regions = " + refreshedRegionsCount;
      SchemaAlterStatus alterStatus = getSchemaAlterStatus(tableName);
      alterStatus.update(SchemaAlterStatus.AlterState.FAILURE,
          refreshedRegionsCount, errmsg);
      String nodePath = getSchemaChangeNodePathForTableAndServer(tableName,
              regionServer.getServerName().getServerName());
      ZKUtil.updateExistingNodeData(this.watcher, nodePath,
          Writables.getBytes(alterStatus), getZKNodeVersion(nodePath));
      LOG.info("reportAndLogSchemaRefreshError() " +
          " Updated child ZKNode with SchemaAlterStatus = "
          + alterStatus + " for table = " + tableName);
      if (status == null) {
        status = TaskMonitor.get().createStatus(errmsg);
      } else {
        status.setStatus(errmsg);
      }
    } catch (KeeperException e) {
    // Retry ?
      String errmsg = "KeeperException while updating the schema change node with "
        + "error status for table = "
        + tableName + " server = "
        + regionServer.getServerName().getServerName()
        + " Cause = " + e.getCause();
      LOG.error(errmsg, e);
      TaskMonitor.get().createStatus(errmsg);
    } catch(IOException ioe) {
      // retry ??
      String errmsg = "IOException while updating the schema change node with "
        + "server name for table = "
        + tableName + " server = "
        + regionServer.getServerName().getServerName()
        + " Cause = " + ioe.getCause();
      TaskMonitor.get().createStatus(errmsg);
      LOG.error(errmsg, ioe);
    }
  }


  private void createStateNode(String tableName, int numberOfOnlineRegions)
      throws IOException {
    SchemaAlterStatus sas =
        new SchemaAlterStatus(regionServer.getServerName().getServerName(),
            numberOfOnlineRegions);
    LOG.debug("Creating Schema Alter State node = " + sas);
    try {
      ZKUtil.createSetData(this.watcher,
          getSchemaChangeNodePathForTableAndServer(tableName,
                regionServer.getServerName().getServerName()),
                Writables.getBytes(sas));
    } catch (KeeperException ke) {
      String errmsg = "KeeperException while creating the schema change node with "
          + "server name for table = "
          + tableName + " server = "
          + regionServer.getServerName().getServerName()
          + " Message = " + ke.getCause();
      LOG.error(errmsg, ke);
      TaskMonitor.get().createStatus(errmsg);
    }

  }

  private SchemaAlterStatus getSchemaAlterStatus(String tableName)
      throws KeeperException, IOException {
    byte[] statusBytes = ZKUtil.getData(this.watcher,
        getSchemaChangeNodePathForTableAndServer(tableName,
            regionServer.getServerName().getServerName()));
    if (statusBytes == null || statusBytes.length <= 0) {
      return null;
    }
    SchemaAlterStatus sas = new SchemaAlterStatus();
    Writables.getWritable(statusBytes, sas);
    return sas;
  }

  private void updateSchemaChangeStatus(String tableName,
                                     SchemaAlterStatus schemaAlterStatus)
      throws KeeperException, IOException {
    try {
      if(sleepTimeMillis > 0) {
        try {
          LOG.debug("SchemaChangeTracker sleeping for "
              + sleepTimeMillis);
          Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      ZKUtil.updateExistingNodeData(this.watcher,
          getSchemaChangeNodePathForTableAndServer(tableName,
              regionServer.getServerName().getServerName()),
          Writables.getBytes(schemaAlterStatus), -1);
      String msg = "Schema change tracker completed for table = " + tableName
              + " status = " + schemaAlterStatus;
      LOG.debug(msg);
      TaskMonitor.get().createStatus(msg);
    } catch (KeeperException.NoNodeException e) {
      String errmsg = "KeeperException.NoNodeException while updating the schema "
          + "change node with server name for table = "
          + tableName + " server = "
          + regionServer.getServerName().getServerName()
          + " Cause = " + e.getCause();
      TaskMonitor.get().createStatus(errmsg);
      LOG.error(errmsg, e);
    } catch (KeeperException e) {
      // Retry ?
      String errmsg = "KeeperException while updating the schema change node with "
          + "server name for table = "
          + tableName + " server = "
          + regionServer.getServerName().getServerName()
          + " Cause = " + e.getCause();
      LOG.error(errmsg, e);
      TaskMonitor.get().createStatus(errmsg);
    } catch(IOException ioe) {
      String errmsg = "IOException while updating the schema change node with "
          + "server name for table = "
          + tableName + " server = "
          + regionServer.getServerName().getServerName()
          + " Cause = " + ioe.getCause();
      LOG.error(errmsg, ioe);
      TaskMonitor.get().createStatus(errmsg);
    }
  }

  private String getSchemaChangeNodePathForTable(String tableName) {
    return ZKUtil.joinZNode(watcher.schemaZNode, tableName);
  }

  private String getSchemaChangeNodePathForTableAndServer(
      String tableName, String regionServerName) {
    return ZKUtil.joinZNode(getSchemaChangeNodePathForTable(tableName),
        regionServerName);
  }

  public int getSleepTimeMillis() {
    return sleepTimeMillis;
  }

  /**
   * Set a sleep time in millis before this RS can update it's progress status.
   * Used only for test cases to test complex test scenarios such as RS failures and
   * RS exemption handling.
   * @param sleepTimeMillis
   */
  public void setSleepTimeMillis(int sleepTimeMillis) {
    this.sleepTimeMillis = sleepTimeMillis;
  }

  /**
   * Check whether there are any schema change requests that are in progress now for the given table.
   * We simply assume that a schema change is in progress if we see a ZK schema node this
   * any table. We may revisit for fine grained checks such as check the current alter status
   * et al, but it is not required now.
   * @return
   */
  public boolean isSchemaChangeInProgress(String tableName) {
    try {
      List<String> schemaChanges = ZKUtil.listChildrenAndWatchThem(this.watcher,
          watcher.schemaZNode);
      if (schemaChanges != null) {
        for (String alterTableName : schemaChanges) {
          if (alterTableName.equals(tableName)) {
            return true;
          }
        }
        return false;
      }
    } catch (KeeperException ke) {
      LOG.debug("isSchemaChangeInProgress. " +
          "KeeperException while getting current schema change progress.");
      return false;
    }
    return false;
  }

  /**
   * Holds the current alter state for a table. Alter state includes the
   * current alter status (INPROCESS, FAILURE, SUCCESS, or IGNORED, current RS
   * host name, timestamp of alter request, number of online regions this RS has for
   * the given table, number of processed regions and an errorCause in case
   * if the RS failed during the schema change process.
   *
   * RS keeps track of schema change requests per table using the alter status and
   * periodically updates the alter status based on schema change status.
   */
  public static class SchemaAlterStatus implements Writable {

    public enum AlterState {
      INPROCESS,        // Inprocess alter
      SUCCESS,          // completed alter
      FAILURE,          // failure alter
      IGNORED           // Ignore the alter processing.
    }

    private AlterState currentAlterStatus;
    // TimeStamp
    private long stamp;
    private int numberOfOnlineRegions;
    private String errorCause = " ";
    private String hostName;
    private int numberOfRegionsProcessed = 0;

    public SchemaAlterStatus() {

    }

    public SchemaAlterStatus(String hostName, int numberOfOnlineRegions) {
      this.numberOfOnlineRegions = numberOfOnlineRegions;
      this.stamp = System.currentTimeMillis();
      this.currentAlterStatus = AlterState.INPROCESS;
      //this.rsToProcess = activeHosts;
      this.hostName = hostName;
    }

    public AlterState getCurrentAlterStatus() {
      return currentAlterStatus;
    }

    public void setCurrentAlterStatus(AlterState currentAlterStatus) {
      this.currentAlterStatus = currentAlterStatus;
    }

    public int getNumberOfOnlineRegions() {
      return numberOfOnlineRegions;
    }

    public void setNumberOfOnlineRegions(int numberOfRegions) {
      this.numberOfOnlineRegions = numberOfRegions;
    }

    public int getNumberOfRegionsProcessed() {
      return numberOfRegionsProcessed;
    }

    public void setNumberOfRegionsProcessed(int numberOfRegionsProcessed) {
      this.numberOfRegionsProcessed = numberOfRegionsProcessed;
    }

    public String getErrorCause() {
      return errorCause;
    }

    public void setErrorCause(String errorCause) {
      this.errorCause = errorCause;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public void update(AlterState state, int numberOfRegions, String errorCause) {
      this.currentAlterStatus = state;
      this.numberOfRegionsProcessed = numberOfRegions;
      this.errorCause = errorCause;
    }

    public void update(AlterState state, int numberOfRegions) {
      this.currentAlterStatus = state;
      this.numberOfRegionsProcessed = numberOfRegions;
    }

    public void update(AlterState state) {
      this.currentAlterStatus = state;
    }

    public void update(SchemaAlterStatus status) {
      this.currentAlterStatus = status.getCurrentAlterStatus();
      this.numberOfRegionsProcessed = status.getNumberOfRegionsProcessed();
      this.errorCause = status.getErrorCause();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      currentAlterStatus = AlterState.valueOf(in.readUTF());
      stamp = in.readLong();
      numberOfOnlineRegions = in.readInt();
      hostName = Bytes.toString(Bytes.readByteArray(in));
      numberOfRegionsProcessed = in.readInt();
      errorCause = Bytes.toString(Bytes.readByteArray(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(currentAlterStatus.name());
      out.writeLong(stamp);
      out.writeInt(numberOfOnlineRegions);
      Bytes.writeByteArray(out, Bytes.toBytes(hostName));
      out.writeInt(numberOfRegionsProcessed);
      Bytes.writeByteArray(out, Bytes.toBytes(errorCause));
    }

    @Override
    public String toString() {
      return
         " state= " + currentAlterStatus
        + ", ts= " + stamp
        + ", number of online regions = " + numberOfOnlineRegions
        + ", host= " + hostName + " processed regions = " + numberOfRegionsProcessed
        + ", errorCause = " + errorCause;
    }
  }

}
