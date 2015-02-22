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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Example class for how to use the table archiving coordinated via zookeeper
 */
@InterfaceAudience.Private
public class ZKTableArchiveClient extends Configured {

  /** Configuration key for the archive node. */
  private static final String ZOOKEEPER_ZNODE_HFILE_ARCHIVE_KEY = "zookeeper.znode.hfile.archive";
  private ClusterConnection connection;

  public ZKTableArchiveClient(Configuration conf, ClusterConnection connection) {
    super(conf);
    this.connection = connection;
  }

  /**
   * Turn on backups for all HFiles for the given table.
   * <p>
   * All deleted hfiles are moved to the archive directory under the table directory, rather than
   * being deleted.
   * <p>
   * If backups are already enabled for this table, does nothing.
   * <p>
   * If the table does not exist, the archiving the table's hfiles is still enabled as a future
   * table with that name may be created shortly.
   * @param table name of the table to start backing up
   * @throws IOException if an unexpected exception occurs
   * @throws KeeperException if zookeeper can't be reached
   */
  public void enableHFileBackupAsync(final byte[] table) throws IOException, KeeperException {
    createHFileArchiveManager().enableHFileBackup(table).stop();
  }

  /**
   * Disable hfile backups for the given table.
   * <p>
   * Previously backed up files are still retained (if present).
   * <p>
   * Asynchronous operation - some extra HFiles may be retained, in the archive directory after
   * disable is called, dependent on the latency in zookeeper to the servers.
   * @param table name of the table stop backing up
   * @throws IOException if an unexpected exception occurs
   * @throws KeeperException if zookeeper can't be reached
   */
  public void disableHFileBackup(String table) throws IOException, KeeperException {
    disableHFileBackup(Bytes.toBytes(table));
  }

  /**
   * Disable hfile backups for the given table.
   * <p>
   * Previously backed up files are still retained (if present).
   * <p>
   * Asynchronous operation - some extra HFiles may be retained, in the archive directory after
   * disable is called, dependent on the latency in zookeeper to the servers.
   * @param table name of the table stop backing up
   * @throws IOException if an unexpected exception occurs
   * @throws KeeperException if zookeeper can't be reached
   */
  public void disableHFileBackup(final byte[] table) throws IOException, KeeperException {
    createHFileArchiveManager().disableHFileBackup(table).stop();
  }

  /**
   * Disable hfile backups for all tables.
   * <p>
   * Previously backed up files are still retained (if present).
   * <p>
   * Asynchronous operation - some extra HFiles may be retained, in the archive directory after
   * disable is called, dependent on the latency in zookeeper to the servers.
   * @throws IOException if an unexpected exception occurs
   * @throws KeeperException if zookeeper can't be reached
   */
  public void disableHFileBackup() throws IOException, KeeperException {
    createHFileArchiveManager().disableHFileBackup().stop();
  }

  /**
   * Determine if archiving is enabled (but not necessarily fully propagated) for a table
   * @param table name of the table to check
   * @return <tt>true</tt> if it is, <tt>false</tt> otherwise
   * @throws IOException if a connection to ZooKeeper cannot be established
   * @throws KeeperException
   */
  public boolean getArchivingEnabled(byte[] table) throws IOException, KeeperException {
    HFileArchiveManager manager = createHFileArchiveManager();
    try {
      return manager.isArchivingEnabled(table);
    } finally {
      manager.stop();
    }
  }

  /**
   * Determine if archiving is enabled (but not necessarily fully propagated) for a table
   * @param table name of the table to check
   * @return <tt>true</tt> if it is, <tt>false</tt> otherwise
   * @throws IOException if an unexpected network issue occurs
   * @throws KeeperException if zookeeper can't be reached
   */
  public boolean getArchivingEnabled(String table) throws IOException, KeeperException {
    return getArchivingEnabled(Bytes.toBytes(table));
  }

  /**
   * @return A new {@link HFileArchiveManager} to manage which tables' hfiles should be archived
   *         rather than deleted.
   * @throws KeeperException if we can't reach zookeeper
   * @throws IOException if an unexpected network issue occurs
   */
  private synchronized HFileArchiveManager createHFileArchiveManager() throws KeeperException,
      IOException {
    return new HFileArchiveManager(this.connection, this.getConf());
  }

  /**
   * @param conf conf to read for the base archive node
   * @param zooKeeper zookeeper to used for building the full path
   * @return get the znode for long-term archival of a table for
   */
  public static String getArchiveZNode(Configuration conf, ZooKeeperWatcher zooKeeper) {
    return ZKUtil.joinZNode(zooKeeper.baseZNode, conf.get(ZOOKEEPER_ZNODE_HFILE_ARCHIVE_KEY,
      TableHFileArchiveTracker.HFILE_ARCHIVE_ZNODE_PARENT));
  }
}
