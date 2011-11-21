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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.KeeperException;

public class MasterSchemaChangeTracker extends ZooKeeperNodeTracker {
  public static final Log LOG = LogFactory.getLog(MasterSchemaChangeTracker.class);
  private final MasterServices masterServices;
  // Used by tests only. Do not change this.
  private volatile int sleepTimeMillis = 0;
  // schema changes pending more than this time will be timed out.
  private long schemaChangeTimeoutMillis = 30000;

  /**
   * Constructs a new ZK node tracker.
   * <p/>
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public MasterSchemaChangeTracker(ZooKeeperWatcher watcher,
                                   Abortable abortable, MasterServices masterServices,
                                   long schemaChangeTimeoutMillis) {
    super(watcher, watcher.schemaZNode, abortable);
    this.masterServices = masterServices;
    this.schemaChangeTimeoutMillis = schemaChangeTimeoutMillis;
  }

  @Override
  public void start() {
    try {
      watcher.registerListener(this);
      List<String> tables =
          ZKUtil.listChildrenNoWatch(watcher, watcher.schemaZNode);
      processCompletedSchemaChanges(tables);
    } catch (KeeperException e) {
      LOG.error("MasterSchemaChangeTracker startup failed.", e);
      abortable.abort("MasterSchemaChangeTracker startup failed", e);
    }
  }

  private List<String> getCurrentTables() throws KeeperException {
    return
        ZKUtil.listChildrenNoWatch(watcher, watcher.schemaZNode);
  }

  /**
   * When a primary master crashes and the secondary master takes over
   * mid-flight during an alter process, the secondary should cleanup any completed
   * schema changes not handled by the previous master.
   * @param tables
   * @throws KeeperException
   */
  private void processCompletedSchemaChanges(List<String> tables)
      throws KeeperException {
    if (tables == null || tables.isEmpty()) {
      String msg = "No current schema change in progress. Skipping cleanup";
      LOG.debug(msg);
      return;
    }
    String msg = "Master seeing following tables undergoing schema change " +
        "process. Tables = " + tables;
    MonitoredTask status = TaskMonitor.get().createStatus(msg);
    LOG.debug(msg);
    for (String table : tables) {
      LOG.debug("Processing table = "+ table);
      status.setStatus("Processing table = "+ table);
      try {
        processTableNode(table);
      } catch (IOException e) {
        String errmsg = "IOException while processing completed schema changes."
            + " Cause = " + e.getCause();
        LOG.error(errmsg, e);
        status.setStatus(errmsg);
      }
    }
  }

  /**
   * Get current alter status for a table.
   * @param tableName
   * @return MasterAlterStatus
   * @throws KeeperException
   * @throws IOException
   */
  public MasterAlterStatus getMasterAlterStatus(String tableName)
      throws KeeperException, IOException {
    String path = getSchemaChangeNodePathForTable(tableName);
    byte[] state = ZKUtil.getData(watcher, path);
    if (state == null || state.length <= 0) {
      return null;
    }
    MasterAlterStatus mas = new MasterAlterStatus();
    Writables.getWritable(state, mas);
    return mas;
  }

  /**
   * Get RS specific alter status for a table & server
   * @param tableName
   * @param serverName
   * @return Region Server's Schema alter status
   * @throws KeeperException
   * @throws IOException
   */
  private SchemaChangeTracker.SchemaAlterStatus getRSSchemaAlterStatus(
      String tableName, String serverName)
      throws KeeperException, IOException {
    String childPath =
        getSchemaChangeNodePathForTableAndServer(tableName, serverName);
    byte[] childData = ZKUtil.getData(this.watcher, childPath);
    if (childData == null || childData.length <= 0) {
      return null;
    }
    SchemaChangeTracker.SchemaAlterStatus sas =
        new SchemaChangeTracker.SchemaAlterStatus();
    Writables.getWritable(childData, sas);
    LOG.debug("Schema Status data for server = " + serverName + " table = "
        + tableName + " == " + sas);
    return sas;
  }

  /**
   * Update the master's alter status based on all region server's response.
   * @param servers
   * @param tableName
   * @throws IOException
   */
  private void updateMasterAlterStatus(MasterAlterStatus mas,
                                       List<String> servers, String tableName)
      throws IOException, KeeperException {
    for (String serverName : servers) {
      SchemaChangeTracker.SchemaAlterStatus sas =
          getRSSchemaAlterStatus(tableName, serverName);
      if (sas != null) {
        mas.update(sas);
        LOG.debug("processTableNodeWithState:Updated Master Alter Status = "
            + mas + " for server = " + serverName);
      } else {
        LOG.debug("SchemaAlterStatus is NULL for table = " + tableName);
      }
    }
  }

  /**
   * If schema alter is handled for this table, then delete all the ZK nodes
   * created for this table.
   * @param tableName
   * @throws KeeperException
   */
  private void processTableNode(String tableName) throws KeeperException,
      IOException {
    LOG.debug("processTableNodeWithState. TableName = " + tableName);
    List<String> servers =
      ZKUtil.listChildrenAndWatchThem(watcher,
          getSchemaChangeNodePathForTable(tableName));
    MasterAlterStatus mas = getMasterAlterStatus(tableName);
    if (mas == null) {
      LOG.debug("MasterAlterStatus is NULL. Table = " + tableName);
      return;
    }
    updateMasterAlterStatus(mas, servers, tableName);
    LOG.debug("Current Alter status = " + mas);
    String nodePath = getSchemaChangeNodePathForTable(tableName);
    ZKUtil.updateExistingNodeData(this.watcher, nodePath,
        Writables.getBytes(mas), getZKNodeVersion(nodePath));
    processAlterStatus(mas, tableName, servers);
  }

  /**
   * Evaluate the master alter status and determine the current status.
   * @param alterStatus
   * @param tableName
   * @param servers
   * @param status
   */
  private void processAlterStatus(MasterAlterStatus alterStatus,
                                  String tableName, List<String> servers)
  throws KeeperException {
    if (alterStatus.getNumberOfRegionsToProcess()
        == alterStatus.getNumberOfRegionsProcessed()) {
      // schema change completed.
      String msg = "All region servers have successfully processed the " +
          "schema changes for table = " + tableName
          + " . Deleting the schema change node for table = "
          + tableName + " Region servers processed the schema change" +
          " request = " + alterStatus.getProcessedHosts()
          + " Total number of regions = " + alterStatus.getNumberOfRegionsToProcess()
          + " Processed regions = " + alterStatus.getNumberOfRegionsProcessed();
      MonitoredTask status = TaskMonitor.get().createStatus(
          "Checking alter schema request status for table = " + tableName);
      status.markComplete(msg);
      LOG.debug(msg);
      cleanProcessedTableNode(getSchemaChangeNodePathForTable(tableName));
    } else {
      if (alterStatus.getErrorCause() != null
          && alterStatus.getErrorCause().trim().length() > 0) {
        String msg = "Alter schema change failed "
            + "for table = " + tableName + " Number of online regions = "
            + alterStatus.getNumberOfRegionsToProcess() + " processed regions count = "
            + alterStatus.getNumberOfRegionsProcessed()
            + " Original list = " + alterStatus.hostsToProcess + " Processed servers = "
            + servers
            + " Error Cause = " + alterStatus.getErrorCause();
        MonitoredTask status = TaskMonitor.get().createStatus(
            "Checking alter schema request status for table = " + tableName);
        // we have errors.
        LOG.debug(msg);
        status.abort(msg);
      } else {
        String msg = "Not all region servers have processed the schema changes"
            + "for table = " + tableName + " Number of online regions = "
            + alterStatus.getNumberOfRegionsToProcess() + " processed regions count = "
            + alterStatus.getNumberOfRegionsProcessed()
            + " Original list = " + alterStatus.hostsToProcess + " Processed servers = "
            + servers + " Alter STate = "
            + alterStatus.getCurrentAlterStatus();
        LOG.debug(msg);
        // status.setStatus(msg);
      }
    }
  }

  /**
   * Check whether a in-flight schema change request has expired.
   * @param tableName
   * @return true is the schema change request expired.
   * @throws IOException
   */
  private boolean hasSchemaChangeExpiredFor(String tableName)
      throws IOException, KeeperException {
    MasterAlterStatus mas = getMasterAlterStatus(tableName);
    long createdTimeStamp = mas.getStamp();
    long duration = System.currentTimeMillis() - createdTimeStamp;
    LOG.debug("Created TimeStamp = " + createdTimeStamp
        + " duration = " + duration + " Table = " + tableName
        + " Master Alter Status = " + mas);
    return (duration > schemaChangeTimeoutMillis);
  }

  /**
   * Handle failed and expired schema changes. We simply delete all the
   * expired/failed schema change attempts. Why we should do this ?
   * 1) Keeping the failed/expired schema change nodes longer prohibits any
   *    future schema changes for the table.
   * 2) Any lingering expired/failed schema change requests will prohibit the
   *    load balancer from running.
   */
  public void handleFailedOrExpiredSchemaChanges() {
    try {
      List<String> tables = getCurrentTables();
      for (String table : tables) {
        String statmsg = "Cleaning failed or expired schema change requests. " +
            "current tables undergoing " +
            "schema change process = " + tables;
         MonitoredTask status = TaskMonitor.get().createStatus(statmsg);
        LOG.debug(statmsg);
        if (hasSchemaChangeExpiredFor(table)) {
          // time out.. currently, we abandon the in-flight schema change due to
          // time out.
          // Here, there are couple of options to consider. One could be to
          // attempt a retry of the schema change and see if it succeeds, and
          // another could be to simply rollback the schema change effort and
          // see if it succeeds.
          String msg = "Schema change for table = " + table + " has expired."
              + " Schema change for this table has been in progress for " +
              + schemaChangeTimeoutMillis +
              "Deleting the node now.";
          LOG.debug(msg);
          ZKUtil.deleteNodeRecursively(this.watcher,
              getSchemaChangeNodePathForTable(table));
        } else {
          String msg = "Schema change request is in progress for " +
              " table = " + table;
          LOG.debug(msg);
          status.setStatus(msg);
        }
      }
    } catch (IOException e) {
      String msg = "IOException during handleFailedExpiredSchemaChanges."
            + e.getCause();
      LOG.error(msg, e);
      TaskMonitor.get().createStatus(msg);
    } catch (KeeperException ke) {
      String msg = "KeeperException during handleFailedExpiredSchemaChanges."
          + ke.getCause();
      LOG.error(msg, ke);
      TaskMonitor.get().createStatus(msg);
    }
  }

  /**
   * Clean the nodes of completed schema change table.
   * @param path
   * @throws KeeperException
   */
  private void cleanProcessedTableNode(String path) throws KeeperException {
    if (sleepTimeMillis > 0) {
      try {
        LOG.debug("Master schema change tracker sleeping for "
            + sleepTimeMillis);
        Thread.sleep(sleepTimeMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    ZKUtil.deleteNodeRecursively(this.watcher, path);
    LOG.debug("Deleted all nodes for path " + path);

  }

  /**
   * Exclude a RS from schema change request (if applicable)
   * We will exclude a RS from schema change request processing if 1) RS
   * has online regions for the table AND 2) RS went down mid-flight
   * during schema change process. We don't have to deal with RS going
   * down mid-flight during a schema change as the online regions from
   * the dead RS will get reassigned to some other RS and the
   * process of reassign inherently takes care of the schema change as well.
   * @param serverName
   */
  public void excludeRegionServerForSchemaChanges(String serverName) {
    try {
      MonitoredTask status = TaskMonitor.get().createStatus(
        "Processing schema change exclusion for region server = " + serverName);
      List<String> tables =
            ZKUtil.listChildrenNoWatch(watcher, watcher.schemaZNode);
      if (tables == null || tables.isEmpty()) {
        String msg = "No schema change in progress. Skipping exclusion for " +
            "server = "+ serverName;
        LOG.debug(msg);
        status.setStatus(msg);
        return ;
      }
      for (String tableName : tables) {
        excludeRegionServer(tableName, serverName, status);
      }
    } catch(KeeperException ke) {
      LOG.error("KeeperException during excludeRegionServerForSchemaChanges", ke);
    } catch(IOException ioe) {
      LOG.error("IOException during excludeRegionServerForSchemaChanges", ioe);

    }
  }

  /**
   * Check whether a schema change is in progress for a given table on a
   * given RS.
   * @param tableName
   * @param serverName
   * @return TRUE is this RS is currently processing a schema change request
   * for the table.
   * @throws KeeperException
   */
  private boolean isSchemaChangeApplicableFor(String tableName,
                                              String serverName)
      throws KeeperException {
    List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher,
        getSchemaChangeNodePathForTable(tableName));
    return (servers.contains(serverName));
  }

  /**
   * Exclude a region server for a table (if applicable) from schema change processing.
   * @param tableName
   * @param serverName
   * @param status
   * @throws KeeperException
   * @throws IOException
   */
  private void excludeRegionServer(String tableName, String serverName,
                                   MonitoredTask status)
      throws KeeperException, IOException {
    if (isSchemaChangeApplicableFor(tableName, serverName)) {
      String msg = "Excluding RS " + serverName + " from schema change process" +
          " for table = " + tableName;
      LOG.debug(msg);
      status.setStatus(msg);
      SchemaChangeTracker.SchemaAlterStatus sas =
          getRSSchemaAlterStatus(tableName, serverName);
      if (sas == null) {
        LOG.debug("SchemaAlterStatus is NULL for table = " + tableName
            + " server = " + serverName);
        return;
      }
      // Set the status to IGNORED so we can process it accordingly.
      sas.setCurrentAlterStatus(
          SchemaChangeTracker.SchemaAlterStatus.AlterState.IGNORED);
      LOG.debug("Updating the current schema status to " + sas);
      String nodePath = getSchemaChangeNodePathForTableAndServer(tableName,
              serverName);
      ZKUtil.updateExistingNodeData(this.watcher,
          nodePath, Writables.getBytes(sas), getZKNodeVersion(nodePath));
    } else {
      LOG.debug("Skipping exclusion of RS " + serverName
          + " from schema change process"
          + " for table = " + tableName
          + " as it did not possess any online regions for the table");
    }
    processTableNode(tableName);
  }

  private int getZKNodeVersion(String nodePath) throws KeeperException {
    return ZKUtil.checkExists(this.watcher, nodePath);
  }

  /**
   * Create a new schema change ZK node.
   * @param tableName Table name that is getting altered
   * @throws KeeperException
   */
  public void createSchemaChangeNode(String tableName,
                                     int numberOfRegions)
      throws KeeperException, IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Creating schema change node for table = " + tableName);
    LOG.debug("Creating schema change node for table = "
        + tableName + " Path = "
        + getSchemaChangeNodePathForTable(tableName));
    if (doesSchemaChangeNodeExists(tableName)) {
      LOG.debug("Schema change node already exists for table = " + tableName
          + " Deleting the schema change node.");
      // If we already see a schema change node for this table we wait till the previous
      // alter process is complete. Ideally, we need not wait and we could simply delete
      // existing schema change node for this table and create new one. But then the
      // RS cloud will not be able to process concurrent schema updates for the same table
      // as they will be working with same set of online regions for this table. Meaning the
      // second alter change will not see any online regions (as they were being closed and
      // re opened by the first change) and will miss the second one.
      // We either handle this at the RS level using explicit locks while processing a table
      // or do it here. I prefer doing it here as it seems much simpler and cleaner.
      while(doesSchemaChangeNodeExists(tableName)) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    int rsCount = ZKUtil.getNumberOfChildren(this.watcher, watcher.rsZNode);
    // if number of online RS = 0, we should not do anything!
    if (rsCount <= 0) {
      String msg = "Master is not seeing any online region servers. Aborting the " +
          "schema change processing by region servers.";
      LOG.debug(msg);
      status.abort(msg);
    } else {
      LOG.debug("Master is seeing " + rsCount + " region servers online before " +
          "the schema change process.");
      MasterAlterStatus mas = new MasterAlterStatus(numberOfRegions,
          getActiveRegionServersAsString());
      LOG.debug("Master creating the master alter status = " + mas);
      ZKUtil.createSetData(this.watcher,
          getSchemaChangeNodePathForTable(tableName), Writables.getBytes(mas));
      status.markComplete("Created the ZK node for schema change. Current Alter Status = "
          + mas.toString());
      ZKUtil.listChildrenAndWatchThem(this.watcher,
          getSchemaChangeNodePathForTable(tableName));
    }
  }

  private String getActiveRegionServersAsString() {
    StringBuffer sbuf = new StringBuffer();
    List<ServerName> currentRS =
        masterServices.getRegionServerTracker().getOnlineServers();
    for (ServerName serverName : currentRS) {
      sbuf.append(serverName.getServerName());
      sbuf.append(" ");
    }
    LOG.debug("Current list of RS to process the schema change = "
        + sbuf.toString());
    return sbuf.toString();
  }

  /**
   * Create a new schema change ZK node.
   * @param tableName
   * @throws KeeperException
   */
  public boolean doesSchemaChangeNodeExists(String tableName)
      throws KeeperException {
    return ZKUtil.checkExists(watcher,
            getSchemaChangeNodePathForTable(tableName)) != -1;
  }

  /**
   * Check whether there are any schema change requests that are in progress now.
   * We simply assume that a schema change is in progress if we see a ZK schema node for
   * any table. We may revisit for fine grained checks such as check the current alter status
   * et al, but it is not required now.
   * @return
   */
  public boolean isSchemaChangeInProgress() {
    try {
      int schemaChangeCount = ZKUtil.getNumberOfChildren(this.watcher, watcher.schemaZNode);
      return schemaChangeCount > 0;
    } catch (KeeperException ke) {
      LOG.debug("KeeperException while getting current schema change progress.");
      // What do we do now??? currently reporting as false.
    }
    return false;
  }

  /**
   * We get notified when a RS processes/or completed the schema change request.
   * The path will be of the format /hbase/schema/<table name>
   * @param path full path of the node whose children have changed
   */
  @Override
  public void nodeChildrenChanged(String path) {
    String tableName = null;
    if (path.startsWith(watcher.schemaZNode) &&
        !path.equals(watcher.schemaZNode)) {
      try {
        LOG.debug("NodeChildrenChanged Path = " + path);
        tableName = path.substring(path.lastIndexOf("/")+1, path.length());
        processTableNode(tableName);
      } catch (KeeperException e) {
        TaskMonitor.get().createStatus(
        "MasterSchemaChangeTracker: ZK exception while processing " +
            " nodeChildrenChanged() event for table = " + tableName
            + " Cause = " + e.getCause());
        LOG.error("MasterSchemaChangeTracker: Unexpected zk exception getting"
            + " schema change nodes", e);
      } catch(IOException ioe) {
        TaskMonitor.get().createStatus(
        "MasterSchemaChangeTracker: ZK exception while processing " +
            " nodeChildrenChanged() event for table = " + tableName
            + " Cause = " + ioe.getCause());
        LOG.error("MasterSchemaChangeTracker: Unexpected IO exception getting"
            + " schema change nodes", ioe);
      }
    }
  }

  /**
   * We get notified as and when the RS cloud updates their ZK nodes with
   * progress information. The path will be of the format
   * /hbase/schema/<table name>/<RS host name>
   * @param path
   */
  @Override
  public void nodeDataChanged(String path) {
    String tableName = null;
    if (path.startsWith(watcher.schemaZNode) &&
        !path.equals(watcher.schemaZNode)) {
      try {
        LOG.debug("NodeDataChanged Path = " + path);
        String[] paths = path.split("/");
        tableName = paths[3];
        processTableNode(tableName);
      } catch (KeeperException e) {
        TaskMonitor.get().createStatus(
        "MasterSchemaChangeTracker: ZK exception while processing " +
            " nodeDataChanged() event for table = " + tableName
            + " Cause = " + e.getCause());
        LOG.error("MasterSchemaChangeTracker: Unexpected zk exception getting"
            + " schema change nodes", e);
      } catch(IOException ioe) {
        TaskMonitor.get().createStatus(
        "MasterSchemaChangeTracker: IO exception while processing " +
            " nodeDataChanged() event for table = " + tableName
            + " Cause = " + ioe.getCause());
        LOG.error("MasterSchemaChangeTracker: Unexpected IO exception getting"
            + " schema change nodes", ioe);

      }
    }
  }

  public String getSchemaChangeNodePathForTable(String tableName) {
    return ZKUtil.joinZNode(watcher.schemaZNode, tableName);
  }

  /**
   * Used only for tests. Do not use this. See TestInstantSchemaChange for more details
   * on how this is getting used. This is primarily used to delay the schema complete
   * processing by master so that we can test some complex scenarios such as
   * master failover.
   * @param sleepTimeMillis
   */
  public void setSleepTimeMillis(int sleepTimeMillis) {
    this.sleepTimeMillis = sleepTimeMillis;
  }

  private String getSchemaChangeNodePathForTableAndServer(
      String tableName, String regionServerName) {
    return ZKUtil.joinZNode(getSchemaChangeNodePathForTable(tableName),
        regionServerName);
  }


  /**
   * Holds the current alter state for a table. Alter state includes the
   * current alter status (INPROCESS, FAILURE or SUCCESS (success is not getting
   * used now.), timestamp of alter request, number of hosts online at the time
   * of alter request, number of online regions to process for the schema change
   * request, number of processed regions and a list of region servers that
   * actually processed the schema change request.
   *
   * Master keeps track of schema change requests using the alter status and
   * periodically updates the alter status based on RS cloud processings.
   */
  public static class MasterAlterStatus implements Writable {

    public enum AlterState {
      INPROCESS,        // Inprocess alter
      SUCCESS,          // completed alter
      FAILURE           // failure alter
    }

    private AlterState currentAlterStatus;
    // TimeStamp
    private long stamp;
    private int numberOfRegionsToProcess;
    private StringBuffer errorCause = new StringBuffer(" ");
    private StringBuffer processedHosts = new StringBuffer(" ");
    private String hostsToProcess;
    private int numberOfRegionsProcessed = 0;

    public MasterAlterStatus() {

    }

    public MasterAlterStatus(int numberOfRegions, String activeHosts) {
      this.numberOfRegionsToProcess = numberOfRegions;
      this.stamp = System.currentTimeMillis();
      this.currentAlterStatus = AlterState.INPROCESS;
      //this.rsToProcess = activeHosts;
      this.hostsToProcess = activeHosts;
    }

    public AlterState getCurrentAlterStatus() {
      return currentAlterStatus;
    }

    public void setCurrentAlterStatus(AlterState currentAlterStatus) {
      this.currentAlterStatus = currentAlterStatus;
    }

    public long getStamp() {
      return stamp;
    }

    public void setStamp(long stamp) {
      this.stamp = stamp;
    }

    public int getNumberOfRegionsToProcess() {
      return numberOfRegionsToProcess;
    }

    public void setNumberOfRegionsToProcess(int numberOfRegionsToProcess) {
      this.numberOfRegionsToProcess = numberOfRegionsToProcess;
    }

    public int getNumberOfRegionsProcessed() {
      return numberOfRegionsProcessed;
    }

    public void setNumberOfRegionsProcessed(int numberOfRegionsProcessed) {
      this.numberOfRegionsProcessed += numberOfRegionsProcessed;
    }

    public String getHostsToProcess() {
      return hostsToProcess;
    }

    public void setHostsToProcess(String hostsToProcess) {
      this.hostsToProcess = hostsToProcess;
    }

    public String getErrorCause() {
      return errorCause == null ? null : errorCause.toString();
    }

    public void setErrorCause(String errorCause) {
      if (errorCause == null || errorCause.trim().length() <= 0) {
        return;
      }
      if (this.errorCause == null) {
        this.errorCause = new StringBuffer(errorCause);
      } else {
        this.errorCause.append(errorCause);
      }
    }

    public String getProcessedHosts() {
      return processedHosts.toString();
    }

    public void setProcessedHosts(String processedHosts) {
      if (this.processedHosts == null) {
        this.processedHosts = new StringBuffer(processedHosts);
      } else {
        this.processedHosts.append(" ").append(processedHosts);
      }
    }

    /**
     * Ignore or exempt a RS from schema change processing.
     * Master will tweak the number of regions to process based on the
     * number of online regions on the target RS and also remove the
     * RS from list of hosts to process.
     * @param schemaAlterStatus
     */
    private void ignoreRSForSchemaChange(
        SchemaChangeTracker.SchemaAlterStatus schemaAlterStatus) {
      LOG.debug("Removing RS " + schemaAlterStatus.getHostName()
          + " from schema change process.");
      hostsToProcess =
          new String(hostsToProcess).replaceAll(schemaAlterStatus.getHostName(), "");
      int ignoreRegionsCount = schemaAlterStatus.getNumberOfOnlineRegions();
      LOG.debug("Current number of regions processed = "
          + this.numberOfRegionsProcessed + " deducting ignored = "
          + ignoreRegionsCount
          + " final = " + (this.numberOfRegionsToProcess-ignoreRegionsCount));
      if (this.numberOfRegionsToProcess > 0) {
        this.numberOfRegionsToProcess -= ignoreRegionsCount;
      } else {
        LOG.debug("Number of regions to process is less than zero. This is odd");
      }
    }

    /**
     * Update the master alter status for this table based on RS alter status.
     * @param schemaAlterStatus
     */
    public void update(SchemaChangeTracker.SchemaAlterStatus schemaAlterStatus) {
      this.setProcessedHosts(schemaAlterStatus.getHostName());
      SchemaChangeTracker.SchemaAlterStatus.AlterState rsState =
          schemaAlterStatus.getCurrentAlterStatus();
      switch(rsState) {
        case FAILURE:
          LOG.debug("Schema update failure Status = "
            + schemaAlterStatus);
          this.setCurrentAlterStatus(
              MasterAlterStatus.AlterState.FAILURE);
          this.setNumberOfRegionsProcessed(
              schemaAlterStatus.getNumberOfRegionsProcessed());
          this.setErrorCause(schemaAlterStatus.getErrorCause());
          break;
        case SUCCESS:
          LOG.debug("Schema update SUCCESS Status = "
            + schemaAlterStatus);
          this.setNumberOfRegionsProcessed(
              schemaAlterStatus.getNumberOfRegionsProcessed());
          this.setCurrentAlterStatus(MasterAlterStatus.AlterState.SUCCESS);
          break;
        case IGNORED:
          LOG.debug("Schema update IGNORED Updating regions to " +
              "process count. Status = "+ schemaAlterStatus);
          ignoreRSForSchemaChange(schemaAlterStatus);
          break;
        default:
            break;
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      currentAlterStatus = AlterState.valueOf(in.readUTF());
      stamp = in.readLong();
      numberOfRegionsToProcess = in.readInt();
      hostsToProcess = Bytes.toString(Bytes.readByteArray(in));
      processedHosts = new StringBuffer(Bytes.toString(Bytes.readByteArray(in)));
      errorCause = new StringBuffer(Bytes.toString(Bytes.readByteArray(in)));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(currentAlterStatus.name());
      out.writeLong(stamp);
      out.writeInt(numberOfRegionsToProcess);
      Bytes.writeByteArray(out, Bytes.toBytes(hostsToProcess));
      Bytes.writeByteArray(out, Bytes.toBytes(processedHosts.toString()));
      Bytes.writeByteArray(out, Bytes.toBytes(errorCause.toString()));
    }

    @Override
    public String toString() {
      return
         " state= " + currentAlterStatus
        + ", ts= " + stamp
        + ", number of regions to process = " + numberOfRegionsToProcess
        + ", number of regions processed = " + numberOfRegionsProcessed
        + ", hosts = " + hostsToProcess
        + " , processed hosts = " + processedHosts
        + " , errorCause = " + errorCause;
    }
  }
}
