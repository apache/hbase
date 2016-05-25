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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Client-side manager for which table's hfiles should be preserved for long-term archive.
 * @see ZKTableArchiveClient
 * @see HFileArchiveTableMonitor
 * @see LongTermArchivingHFileCleaner
 */
@InterfaceAudience.Private
class HFileArchiveManager {

  private final String archiveZnode;
  private static final Log LOG = LogFactory.getLog(HFileArchiveManager.class);
  private final ZooKeeperWatcher zooKeeper;
  private volatile boolean stopped = false;

  public HFileArchiveManager(Connection connection, Configuration conf)
      throws ZooKeeperConnectionException, IOException {
    this.zooKeeper = new ZooKeeperWatcher(conf, "hfileArchiveManager-on-" + connection.toString(),
        connection);
    this.archiveZnode = ZKTableArchiveClient.getArchiveZNode(this.zooKeeper.getConfiguration(),
      this.zooKeeper);
  }

  /**
   * Turn on auto-backups of HFiles on the specified table.
   * <p>
   * When HFiles would be deleted from the hfile archive, they are instead preserved.
   * @param table name of the table for which to preserve hfiles.
   * @return <tt>this</tt> for chaining.
   * @throws KeeperException if we can't reach zookeeper to update the hfile cleaner.
   */
  public HFileArchiveManager enableHFileBackup(byte[] table) throws KeeperException {
    enable(this.zooKeeper, table);
    return this;
  }

  /**
   * Stop retaining HFiles for the given table in the archive. HFiles will be cleaned up on the next
   * pass of the {@link HFileCleaner}, if the HFiles are retained by another cleaner.
   * @param table name of the table for which to disable hfile retention.
   * @return <tt>this</tt> for chaining.
   * @throws KeeperException if if we can't reach zookeeper to update the hfile cleaner.
   */
  public HFileArchiveManager disableHFileBackup(byte[] table) throws KeeperException {
      disable(this.zooKeeper, table);
    return this;
  }

  /**
   * Disable long-term archival of all hfiles for all tables in the cluster.
   * @return <tt>this</tt> for chaining.
   * @throws IOException if the number of attempts is exceeded
   */
  public HFileArchiveManager disableHFileBackup() throws IOException {
    LOG.debug("Disabling backups on all tables.");
    try {
      ZKUtil.deleteNodeRecursively(this.zooKeeper, archiveZnode);
      return this;
    } catch (KeeperException e) {
      throw new IOException("Unexpected ZK exception!", e);
    }
  }

  /**
   * Perform a best effort enable of hfile retention, which relies on zookeeper communicating the //
   * * change back to the hfile cleaner.
   * <p>
   * No attempt is made to make sure that backups are successfully created - it is inherently an
   * <b>asynchronous operation</b>.
   * @param zooKeeper watcher connection to zk cluster
   * @param table table name on which to enable archiving
   * @throws KeeperException
   */
  private void enable(ZooKeeperWatcher zooKeeper, byte[] table)
      throws KeeperException {
    LOG.debug("Ensuring archiving znode exists");
    ZKUtil.createAndFailSilent(zooKeeper, archiveZnode);

    // then add the table to the list of znodes to archive
    String tableNode = this.getTableNode(table);
    LOG.debug("Creating: " + tableNode + ", data: []");
    ZKUtil.createSetData(zooKeeper, tableNode, new byte[0]);
  }

  /**
   * Disable all archiving of files for a given table
   * <p>
   * Inherently an <b>asynchronous operation</b>.
   * @param zooKeeper watcher for the ZK cluster
   * @param table name of the table to disable
   * @throws KeeperException if an unexpected ZK connection issues occurs
   */
  private void disable(ZooKeeperWatcher zooKeeper, byte[] table) throws KeeperException {
    // ensure the latest state of the archive node is found
    zooKeeper.sync(archiveZnode);

    // if the top-level archive node is gone, then we are done
    if (ZKUtil.checkExists(zooKeeper, archiveZnode) < 0) {
      return;
    }
    // delete the table node, from the archive
    String tableNode = this.getTableNode(table);
    // make sure the table is the latest version so the delete takes
    zooKeeper.sync(tableNode);

    LOG.debug("Attempting to delete table node:" + tableNode);
    ZKUtil.deleteNodeRecursively(zooKeeper, tableNode);
  }

  public void stop() {
    if (!this.stopped) {
      this.stopped = true;
      LOG.debug("Stopping HFileArchiveManager...");
      this.zooKeeper.close();
    }
  }

  /**
   * Check to see if the table is currently marked for archiving
   * @param table name of the table to check
   * @return <tt>true</tt> if the archive znode for that table exists, <tt>false</tt> if not
   * @throws KeeperException if an unexpected zookeeper error occurs
   */
  public boolean isArchivingEnabled(byte[] table) throws KeeperException {
    String tableNode = this.getTableNode(table);
    return ZKUtil.checkExists(zooKeeper, tableNode) >= 0;
  }

  /**
   * Get the zookeeper node associated with archiving the given table
   * @param table name of the table to check
   * @return znode for the table's archive status
   */
  private String getTableNode(byte[] table) {
    return ZKUtil.joinZNode(archiveZnode, Bytes.toString(table));
  }
}
