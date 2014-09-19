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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * The zookeeper used for MOB.
 * This zookeeper is used to synchronize the HBase major compaction and sweep tool.
 * The structure of the nodes for mob in zookeeper.
 * |--baseNode
 *     |--MOB
 *         |--tableName:columnFamilyName-lock // locks for the mob column family
 *         |--tableName:columnFamilyName-sweeper // when a sweep tool runs, such a node is added
 *         |--tableName:columnFamilyName-majorCompaction
 *              |--UUID //when a major compaction occurs, such a node is added.
 * In order to synchronize the operations between the sweep tool and HBase major compaction, these
 * actions need to acquire the tableName:columnFamilyName-lock before the sweep tool and major
 * compaction run.
 * In sweep tool.
 * 1. If it acquires the lock successfully. It check whether the sweeper node exists, if exist the
 * current running is aborted. If not it it checks whether there're major compaction nodes, if yes
 * the current running is aborted, if not it adds a sweep node to the zookeeper.
 * 2. If it could not obtain the lock, the current running is aborted.
 * In the HBase compaction.
 * 1. If it's a minor compaction, continue the compaction.
 * 2. If it's a major compaction, it acquires a lock in zookeeper.
 *    A. If it obtains the lock, it checks whether there's sweep node, if yes it converts itself
 *    to a minor one and continue, if no it adds a major compaction node to the zookeeper.
 *    B. If it could not obtain the lock, it converts itself to a minor one and continue the
 *    compaction.
 */
@InterfaceAudience.Private
public class MobZookeeper {
  // TODO Will remove this class before the mob is merged back to master.
  private static final Log LOG = LogFactory.getLog(MobZookeeper.class);

  private ZooKeeperWatcher zkw;
  private String mobZnode;
  private static final String LOCK_EPHEMERAL = "-lock";
  private static final String SWEEPER_EPHEMERAL = "-sweeper";
  private static final String MAJOR_COMPACTION_EPHEMERAL = "-majorCompaction";

  private MobZookeeper(Configuration conf, String identifier) throws IOException,
      KeeperException {
    this.zkw = new ZooKeeperWatcher(conf, identifier, new DummyMobAbortable());
    mobZnode = ZKUtil.joinZNode(zkw.baseZNode, "MOB");
    if (ZKUtil.checkExists(zkw, mobZnode) == -1) {
      ZKUtil.createWithParents(zkw, mobZnode);
    }
  }

  /**
   * Creates an new instance of MobZookeeper.
   * @param conf The current configuration.
   * @param identifier string that is passed to RecoverableZookeeper to be used as
   * identifier for this instance.
   * @return A new instance of MobZookeeper.
   * @throws IOException
   * @throws KeeperException
   */
  public static MobZookeeper newInstance(Configuration conf, String identifier) throws IOException,
      KeeperException {
    return new MobZookeeper(conf, identifier);
  }

  /**
   * Acquire a lock on the current column family.
   * All the threads try to access the column family acquire a lock which is actually create an
   * ephemeral node in the zookeeper.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   * @return True if the lock is obtained successfully. Otherwise false is returned.
   */
  public boolean lockColumnFamily(String tableName, String familyName) {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    boolean locked = false;
    try {
      locked = ZKUtil.createEphemeralNodeAndWatch(zkw,
          ZKUtil.joinZNode(mobZnode, znodeName + LOCK_EPHEMERAL), null);
      if (LOG.isDebugEnabled()) {
        LOG.debug(locked ? "Locked the column family " + znodeName
            : "Can not lock the column family " + znodeName);
      }
    } catch (KeeperException e) {
      LOG.error("Fail to lock the column family " + znodeName, e);
    }
    return locked;
  }

  /**
   * Release the lock on the current column family.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   */
  public void unlockColumnFamily(String tableName, String familyName) {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unlocking the column family " + znodeName);
    }
    try {
      ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(mobZnode, znodeName + LOCK_EPHEMERAL));
    } catch (KeeperException e) {
      LOG.warn("Fail to unlock the column family " + znodeName, e);
    }
  }

  /**
   * Adds a node to zookeeper which indicates that a sweep tool is running.
   * @param tableName The current table name.
   * @param familyName The current columnFamilyName name.
   * @param data the data of the ephemeral node.
   * @return True if the node is created successfully. Otherwise false is returned.
   */
  public boolean addSweeperZNode(String tableName, String familyName, byte[] data) {
    boolean add = false;
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    try {
      add = ZKUtil.createEphemeralNodeAndWatch(zkw,
          ZKUtil.joinZNode(mobZnode, znodeName + SWEEPER_EPHEMERAL), data);
      if (LOG.isDebugEnabled()) {
        LOG.debug(add ? "Added a znode for sweeper " + znodeName
            : "Cannot add a znode for sweeper " + znodeName);
      }
    } catch (KeeperException e) {
      LOG.error("Fail to add a znode for sweeper " + znodeName, e);
    }
    return add;
  }

  /**
   * Gets the path of the sweeper znode in zookeeper.
   * @param tableName The current table name.
   * @param familyName The current columnFamilyName name.
   * @return The path of the sweeper znode in zookeper.
   */
  public String getSweeperZNodePath(String tableName, String familyName) {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    return ZKUtil.joinZNode(mobZnode, znodeName + SWEEPER_EPHEMERAL);
  }

  /**
   * Deletes the node from zookeeper which indicates that a sweep tool is finished.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   */
  public void deleteSweeperZNode(String tableName, String familyName) {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    try {
      ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(mobZnode, znodeName + SWEEPER_EPHEMERAL));
    } catch (KeeperException e) {
      LOG.error("Fail to delete a znode for sweeper " + znodeName, e);
    }
  }

  /**
   * Checks whether the znode exists in the Zookeeper.
   * If the node exists, it means a sweep tool is running.
   * Otherwise, the sweep tool is not.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   * @return True if this node doesn't exist. Otherwise false is returned.
   * @throws KeeperException
   */
  public boolean isSweeperZNodeExist(String tableName, String familyName) throws KeeperException {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    return ZKUtil.checkExists(zkw, ZKUtil.joinZNode(mobZnode, znodeName + SWEEPER_EPHEMERAL)) >= 0;
  }

  /**
   * Checks whether there're major compactions nodes in the zookeeper.
   * If there're such nodes, it means there're major compactions in progress now.
   * Otherwise there're not.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   * @return True if there're major compactions in progress. Otherwise false is returned.
   * @throws KeeperException
   */
  public boolean hasMajorCompactionChildren(String tableName, String familyName)
      throws KeeperException {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    String mcPath = ZKUtil.joinZNode(mobZnode, znodeName + MAJOR_COMPACTION_EPHEMERAL);
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, mcPath);
    return children != null && !children.isEmpty();
  }

  /**
   * Creates a node of a major compaction to the Zookeeper.
   * Before a HBase major compaction, such a node is created to the Zookeeper. It tells others that
   * there're major compaction in progress, the sweep tool could not be run at this time.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   * @param compactionName The current compaction name.
   * @return True if the node is created successfully. Otherwise false is returned.
   * @throws KeeperException
   */
  public boolean addMajorCompactionZNode(String tableName, String familyName,
      String compactionName) throws KeeperException {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    String mcPath = ZKUtil.joinZNode(mobZnode, znodeName + MAJOR_COMPACTION_EPHEMERAL);
    ZKUtil.createNodeIfNotExistsAndWatch(zkw, mcPath, null);
    String eachMcPath = ZKUtil.joinZNode(mcPath, compactionName);
    return ZKUtil.createEphemeralNodeAndWatch(zkw, eachMcPath, null);
  }

  /**
   * Deletes a major compaction node from the Zookeeper.
   * @param tableName The current table name.
   * @param familyName The current column family name.
   * @param compactionName The current compaction name.
   * @throws KeeperException
   */
  public void deleteMajorCompactionZNode(String tableName, String familyName,
      String compactionName) throws KeeperException {
    String znodeName = MobUtils.getColumnFamilyZNodeName(tableName, familyName);
    String mcPath = ZKUtil.joinZNode(mobZnode, znodeName + MAJOR_COMPACTION_EPHEMERAL);
    String eachMcPath = ZKUtil.joinZNode(mcPath, compactionName);
    ZKUtil.deleteNode(zkw, eachMcPath);
  }

  /**
   * Closes the MobZookeeper.
   */
  public void close() {
    this.zkw.close();
  }

  /**
   * An dummy abortable. It's used for the MobZookeeper.
   */
  public static class DummyMobAbortable implements Abortable {

    private boolean abort = false;

    public void abort(String why, Throwable e) {
      abort = true;
    }

    public boolean isAborted() {
      return abort;
    }

  }
}
