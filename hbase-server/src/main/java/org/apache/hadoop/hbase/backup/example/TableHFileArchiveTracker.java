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
package org.apache.hadoop.hbase.backup.example;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Track HFile archiving state changes in ZooKeeper. Keeps track of the tables whose HFiles should
 * be kept in the archive.
 * <p>
 * {@link TableHFileArchiveTracker#start()} needs to be called to start monitoring for tables to
 * archive.
 */
@InterfaceAudience.Private
public final class TableHFileArchiveTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(TableHFileArchiveTracker.class);
  public static final String HFILE_ARCHIVE_ZNODE_PARENT = "hfilearchive";
  private HFileArchiveTableMonitor monitor;
  private String archiveHFileZNode;
  private boolean stopped = false;

  private TableHFileArchiveTracker(ZKWatcher watcher, HFileArchiveTableMonitor monitor) {
    super(watcher);
    watcher.registerListener(this);
    this.monitor = monitor;
    this.archiveHFileZNode = ZKTableArchiveClient.getArchiveZNode(watcher.getConfiguration(),
      watcher);
  }

  /**
   * Start monitoring for archive updates
   * @throws KeeperException on failure to find/create nodes
   */
  public void start() throws KeeperException {
    // if archiving is enabled, then read in the list of tables to archive
    LOG.debug("Starting hfile archive tracker...");
    this.checkEnabledAndUpdate();
    LOG.debug("Finished starting hfile archive tracker!");
  }

  @Override
  public void nodeCreated(String path) {
    // if it is the archive path
    if (!path.startsWith(archiveHFileZNode)) {
      return;
    }

    LOG.debug("Archive node: " + path + " created");
    // since we are already enabled, just update a single table
    String table = path.substring(archiveHFileZNode.length());

    // the top level node has come up, so read in all the tables
    if (table.length() == 0) {
      checkEnabledAndUpdate();
      return;
    }
    // find the table that needs to be archived
    try {
      addAndReWatchTable(path);
    } catch (KeeperException e) {
      LOG.warn("Couldn't read zookeeper data for table for path:" + path
          + ", not preserving a table.", e);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (!path.startsWith(archiveHFileZNode)) {
      return;
    }

    LOG.debug("Archive node: " + path + " children changed.");
    // a table was added to the archive
    try {
      updateWatchedTables();
    } catch (KeeperException e) {
      LOG.error("Failed to update tables to archive", e);
    }
  }

  /**
   * Add this table to the tracker and then read a watch on that node.
   * <p>
   * Handles situation where table is deleted in the time between the update and resetting the watch
   * by deleting the table via {@link #safeStopTrackingTable(String)}
   * @param tableZnode full zookeeper path to the table to be added
   * @throws KeeperException if an unexpected zk exception occurs
   */
  private void addAndReWatchTable(String tableZnode) throws KeeperException {
    getMonitor().addTable(ZKUtil.getNodeName(tableZnode));
    // re-add a watch to the table created
    // and check to make sure it wasn't deleted
    if (!ZKUtil.watchAndCheckExists(watcher, tableZnode)) {
      safeStopTrackingTable(tableZnode);
    }
  }

  /**
   * Stop tracking a table. Ensures that the table doesn't exist, but if it does, it attempts to add
   * the table back via {@link #addAndReWatchTable(String)} - its a 'safe' removal.
   * @param tableZnode full zookeeper path to the table to be added
   * @throws KeeperException if an unexpected zk exception occurs
   */
  private void safeStopTrackingTable(String tableZnode) throws KeeperException {
    getMonitor().removeTable(ZKUtil.getNodeName(tableZnode));
    // if the table exists, then add and rewatch it
    if (ZKUtil.checkExists(watcher, tableZnode) >= 0) {
      addAndReWatchTable(tableZnode);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (!path.startsWith(archiveHFileZNode)) {
      return;
    }

    LOG.debug("Archive node: " + path + " deleted");
    String table = path.substring(archiveHFileZNode.length());
    // if we stop archiving all tables
    if (table.length() == 0) {
      // make sure we have the tracker before deleting the archive
      // but if we don't, we don't care about delete
      clearTables();
      // watches are one-time events, so we need to renew our subscription to
      // the archive node and might as well check to make sure archiving
      // didn't come back on at the same time
      checkEnabledAndUpdate();
      return;
    }
    // just stop archiving one table
    // note that we don't attempt to add another watch for that table into zk.
    // We have no assurances that the table will be archived again (or even
    // exists for that matter), so its better not to add unnecessary load to
    // zk for watches. If the table is created again, then we will get the
    // notification in childrenChanaged.
    getMonitor().removeTable(ZKUtil.getNodeName(path));
  }

  /**
   * Sets the watch on the top-level archive znode, and then updates the monitor with the current
   * tables that should be archived (and ensures that those nodes are watched as well).
   */
  private void checkEnabledAndUpdate() {
    try {
      if (ZKUtil.watchAndCheckExists(watcher, archiveHFileZNode)) {
        LOG.debug(archiveHFileZNode + " znode does exist, checking for tables to archive");

        // update the tables we should backup, to get the most recent state.
        // This is safer than also watching for children and then hoping we get
        // all the updates as it makes sure we get and watch all the children
        updateWatchedTables();
      } else {
        LOG.debug("Archiving not currently enabled, waiting");
      }
    } catch (KeeperException e) {
      LOG.warn("Failed to watch for archiving znode", e);
    }
  }

  /**
   * Read the list of children under the archive znode as table names and then sets those tables to
   * the list of tables that we should archive
   * @throws KeeperException if there is an unexpected zk exception
   */
  private void updateWatchedTables() throws KeeperException {
    // get the children and watch for new children
    LOG.debug("Updating watches on tables to archive.");
    // get the children and add watches for each of the children
    List<String> tables = ZKUtil.listChildrenAndWatchThem(watcher, archiveHFileZNode);
    LOG.debug("Starting archive for tables:" + tables);
    // if archiving is still enabled
    if (tables != null && tables.size() > 0) {
      getMonitor().setArchiveTables(tables);
    } else {
      LOG.debug("No tables to archive.");
      // only if we currently have a tracker, then clear the archive
      clearTables();
    }
  }

  /**
   * Remove the currently archived tables.
   * <p>
   * Does some intelligent checking to make sure we don't prematurely create an archive tracker.
   */
  private void clearTables() {
    getMonitor().clearArchive();
  }

  /**
   * Determine if the given table should or should not allow its hfiles to be deleted
   * @param tableName name of the table to check
   * @return <tt>true</tt> if its store files should be retained, <tt>false</tt> otherwise
   */
  public boolean keepHFiles(String tableName) {
    return getMonitor().shouldArchiveTable(tableName);
  }

  /**
   * @return the tracker for which tables should be archived.
   */
  public final HFileArchiveTableMonitor getMonitor() {
    return this.monitor;
  }

  /**
   * Create an archive tracker for the passed in server
   * @param conf to read for zookeeper connection information
   * @return ZooKeeper tracker to monitor for this server if this server should archive hfiles for a
   *         given table
   * @throws IOException If a unexpected exception occurs
   * @throws ZooKeeperConnectionException if we can't reach zookeeper
   */
  public static TableHFileArchiveTracker create(Configuration conf)
      throws ZooKeeperConnectionException, IOException {
    ZKWatcher zkw = new ZKWatcher(conf, "hfileArchiveCleaner", null);
    return create(zkw, new HFileArchiveTableMonitor());
  }

  /**
   * Create an archive tracker with the special passed in table monitor. Should only be used in
   * special cases (e.g. testing)
   * @param zkw Watcher for the ZooKeeper cluster that we should track
   * @param monitor Monitor for which tables need hfile archiving
   * @return ZooKeeper tracker to monitor for this server if this server should archive hfiles for a
   *         given table
   */
  private static TableHFileArchiveTracker create(ZKWatcher zkw,
      HFileArchiveTableMonitor monitor) {
    return new TableHFileArchiveTracker(zkw, monitor);
  }

  public ZKWatcher getZooKeeperWatcher() {
    return this.watcher;
  }

  /**
   * Stop this tracker and the passed zookeeper
   */
  public void stop() {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.watcher.close();
  }
}
