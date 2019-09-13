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
package org.apache.hadoop.hbase.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * Acts as the single ZooKeeper Watcher.  One instance of this is instantiated
 * for each Master, RegionServer, and client process.
 *
 * <p>This is the only class that implements {@link Watcher}.  Other internal
 * classes which need to be notified of ZooKeeper events must register with
 * the local instance of this watcher via {@link #registerListener}.
 *
 * <p>This class also holds and manages the connection to ZooKeeper.  Code to
 * deal with connection related events and exceptions are handled here.
 */
@InterfaceAudience.Private
public class ZooKeeperWatcher implements Watcher, Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(ZooKeeperWatcher.class);

  // Identifier for this watcher (for logging only).  It is made of the prefix
  // passed on construction and the zookeeper sessionid.
  private String prefix;
  private String identifier;

  // zookeeper quorum
  private String quorum;

  // zookeeper connection
  private final RecoverableZooKeeper recoverableZooKeeper;

  // abortable in case of zk failure
  protected Abortable abortable;
  // Used if abortable is null
  private boolean aborted = false;

  // listeners to be notified
  private final List<ZooKeeperListener> listeners =
    new CopyOnWriteArrayList<ZooKeeperListener>();

  // Used by ZKUtil:waitForZKConnectionIfAuthenticating to wait for SASL
  // negotiation to complete
  public CountDownLatch saslLatch = new CountDownLatch(1);

  // node names

  // base znode for this cluster
  public String baseZNode;
  //znodes containing the locations of the servers hosting the meta replicas
  private Map<Integer,String> metaReplicaZnodes = new HashMap<Integer, String>();
  // znode containing ephemeral nodes of the regionservers
  public String rsZNode;
  // znode containing ephemeral nodes of the draining regionservers
  public String drainingZNode;
  // znode of currently active master
  private String masterAddressZNode;
  // znode of this master in backup master directory, if not the active master
  public String backupMasterAddressesZNode;
  // znode containing the current cluster state
  public String clusterStateZNode;
  // znode used for region transitioning and assignment
  public String assignmentZNode;
  // znode used for table disabling/enabling
  public String tableZNode;
  // znode containing the unique cluster ID
  public String clusterIdZNode;
  // znode used for log splitting work assignment
  public String splitLogZNode;
  // znode containing the state of the load balancer
  public String balancerZNode;
  // znode containing the state of region normalizer
  private String regionNormalizerZNode;
  // znode containing the state of all switches, currently there are split and merge child node.
  private String switchZNode;
  // znode containing the lock for the tables
  public String tableLockZNode;
  // znode containing the state of the snapshot auto-cleanup
  String snapshotCleanupZNode;
  // znode containing the state of recovering regions
  public String recoveringRegionsZNode;
  // znode containing namespace descriptors
  public static String namespaceZNode = "namespace";
  // znode of indicating master maintenance mode
  public static String masterMaintZNode = "masterMaintenance";

  // Certain ZooKeeper nodes need to be world-readable
  public static final ArrayList<ACL> CREATOR_ALL_AND_WORLD_READABLE =
    new ArrayList<ACL>() { {
      add(new ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE));
      add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
    }};

  public final static String META_ZNODE_PREFIX = "meta-region-server";
  private static final String DEFAULT_SNAPSHOT_CLEANUP_ZNODE = "snapshot-cleanup";

  private final Configuration conf;

  /* A pattern that matches a Kerberos name, borrowed from Hadoop's KerberosName */
  private static final Pattern NAME_PATTERN = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param identifier string that is passed to RecoverableZookeeper to be used as
   * identifier for this instance. Use null for default.
   * @throws IOException
   * @throws ZooKeeperConnectionException
   */
  public ZooKeeperWatcher(Configuration conf, String identifier,
      Abortable abortable) throws ZooKeeperConnectionException, IOException {
    this(conf, identifier, abortable, false);
  }

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param conf
   * @param identifier string that is passed to RecoverableZookeeper to be used as identifier for
   *          this instance. Use null for default.
   * @param abortable Can be null if there is on error there is no host to abort: e.g. client
   *          context.
   * @param canCreateBaseZNode
   * @throws IOException
   * @throws ZooKeeperConnectionException
   */
  public ZooKeeperWatcher(Configuration conf, String identifier,
      Abortable abortable, boolean canCreateBaseZNode)
  throws IOException, ZooKeeperConnectionException {
    this.conf = conf;
    this.quorum = ZKConfig.getZKQuorumServersString(conf);
    this.prefix = identifier;
    // Identifier will get the sessionid appended later below down when we
    // handle the syncconnect event.
    this.identifier = identifier + "0x0";
    this.abortable = abortable;
    setNodeNames(conf);
    PendingWatcher pendingWatcher = new PendingWatcher();
    this.recoverableZooKeeper = ZKUtil.connect(conf, quorum, pendingWatcher, identifier);
    pendingWatcher.prepare(this);
    if (canCreateBaseZNode) {
      try {
        createBaseZNodes();
      } catch (ZooKeeperConnectionException zce) {
        try {
          this.recoverableZooKeeper.close();
        } catch (InterruptedException ie) {
          LOG.debug("Encountered InterruptedException when closing " + this.recoverableZooKeeper);
          Thread.currentThread().interrupt();
        }
        throw zce;
      }
    }
  }

  private void createBaseZNodes() throws ZooKeeperConnectionException {
    try {
      // Create all the necessary "directories" of znodes
      ZKUtil.createWithParents(this, baseZNode);
      if (conf.getBoolean("hbase.assignment.usezk", true)) {
        ZKUtil.createAndFailSilent(this, assignmentZNode);
      }
      ZKUtil.createAndFailSilent(this, rsZNode);
      ZKUtil.createAndFailSilent(this, drainingZNode);
      ZKUtil.createAndFailSilent(this, tableZNode);
      ZKUtil.createAndFailSilent(this, splitLogZNode);
      ZKUtil.createAndFailSilent(this, backupMasterAddressesZNode);
      ZKUtil.createAndFailSilent(this, tableLockZNode);
      ZKUtil.createAndFailSilent(this, recoveringRegionsZNode);
      ZKUtil.createAndFailSilent(this, masterMaintZNode);
    } catch (KeeperException e) {
      throw new ZooKeeperConnectionException(
          prefix("Unexpected KeeperException creating base node"), e);
    }
  }

  /** Returns whether the znode is supposed to be readable by the client
   * and DOES NOT contain sensitive information (world readable).*/
  public boolean isClientReadable(String node) {
    // Developer notice: These znodes are world readable. DO NOT add more znodes here UNLESS
    // all clients need to access this data to work. Using zk for sharing data to clients (other
    // than service lookup case is not a recommended design pattern.
    return
        node.equals(baseZNode) ||
        isAnyMetaReplicaZnode(node) ||
        node.equals(getMasterAddressZNode()) ||
        node.equals(clusterIdZNode)||
        node.equals(rsZNode) ||
        // /hbase/table and /hbase/table/foo is allowed, /hbase/table-lock is not
        node.equals(tableZNode) ||
        node.startsWith(tableZNode + "/");
  }

  /**
   * On master start, we check the znode ACLs under the root directory and set the ACLs properly
   * if needed. If the cluster goes from an unsecure setup to a secure setup, this step is needed
   * so that the existing znodes created with open permissions are now changed with restrictive
   * perms.
   */
  public void checkAndSetZNodeAcls() {
    if (!ZKUtil.isSecureZooKeeper(getConfiguration())) {
      LOG.info("not a secure deployment, proceeding");
      return;
    }

    // Check the base znodes permission first. Only do the recursion if base znode's perms are not
    // correct.
    try {
      List<ACL> actualAcls = recoverableZooKeeper.getAcl(baseZNode, new Stat());

      if (!isBaseZnodeAclSetup(actualAcls)) {
        LOG.info("setting znode ACLs");
        setZnodeAclsRecursive(baseZNode);
      }
    } catch(KeeperException.NoNodeException nne) {
      return;
    } catch(InterruptedException ie) {
      interruptedExceptionNoThrow(ie, false);
    } catch (IOException|KeeperException e) {
      LOG.warn("Received exception while checking and setting zookeeper ACLs", e);
    }
  }

  /**
   * Set the znode perms recursively. This will do post-order recursion, so that baseZnode ACLs
   * will be set last in case the master fails in between.
   * @param znode
   */
  private void setZnodeAclsRecursive(String znode) throws KeeperException, InterruptedException {
    List<String> children = recoverableZooKeeper.getChildren(znode, false);

    for (String child : children) {
      setZnodeAclsRecursive(ZKUtil.joinZNode(znode, child));
    }
    List<ACL> acls = ZKUtil.createACL(this, znode, true);
    LOG.info("Setting ACLs for znode:" + znode + " , acl:" + acls);
    recoverableZooKeeper.setAcl(znode, acls, -1);
  }

  /**
   * Checks whether the ACLs returned from the base znode (/hbase) is set for secure setup.
   * @param acls acls from zookeeper
   * @return whether ACLs are set for the base znode
   * @throws IOException
   */
  private boolean isBaseZnodeAclSetup(List<ACL> acls) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking znode ACLs");
    }
    String[] superUsers = conf.getStrings(Superusers.SUPERUSER_CONF_KEY);
    // Check whether ACL set for all superusers
    if (superUsers != null && !checkACLForSuperUsers(superUsers, acls)) {
      return false;
    }

    // this assumes that current authenticated user is the same as zookeeper client user
    // configured via JAAS
    String hbaseUser = UserGroupInformation.getCurrentUser().getShortUserName();

    if (acls.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ACL is empty");
      }
      return false;
    }

    for (ACL acl : acls) {
      int perms = acl.getPerms();
      Id id = acl.getId();
      // We should only set at most 3 possible ACLs for 3 Ids. One for everyone, one for superuser
      // and one for the hbase user
      if (Ids.ANYONE_ID_UNSAFE.equals(id)) {
        if (perms != Perms.READ) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("permissions for '%s' are not correct: have 0x%x, want 0x%x",
              id, perms, Perms.READ));
          }
          return false;
        }
      } else if (superUsers != null && isSuperUserId(superUsers, id)) {
        if (perms != Perms.ALL) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("permissions for '%s' are not correct: have 0x%x, want 0x%x",
              id, perms, Perms.ALL));
          }
          return false;
        }
      } else if ("sasl".equals(id.getScheme())) {
        String name = id.getId();
        // If ZooKeeper recorded the Kerberos full name in the ACL, use only the shortname
        Matcher match = NAME_PATTERN.matcher(name);
        if (match.matches()) {
          name = match.group(1);
        }
        if (name.equals(hbaseUser)) {
          if (perms != Perms.ALL) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format("permissions for '%s' are not correct: have 0x%x, want 0x%x",
                id, perms, Perms.ALL));
            }
            return false;
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unexpected shortname in SASL ACL: " + id);
          }
          return false;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("unexpected ACL id '" + id + "'");
        }
        return false;
      }
    }
    return true;
  }
  
  /*
   * Validate whether ACL set for all superusers.
   */
  private boolean checkACLForSuperUsers(String[] superUsers, List<ACL> acls) {
    for (String user : superUsers) {
      boolean hasAccess = false;
      // TODO: Validate super group members also when ZK supports setting node ACL for groups.
      if (!AuthUtil.isGroupPrincipal(user)) {
        for (ACL acl : acls) {
          if (user.equals(acl.getId().getId())) {
            if (acl.getPerms() == Perms.ALL) {
              hasAccess = true;
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(
                  "superuser '%s' does not have correct permissions: have 0x%x, want 0x%x",
                  acl.getId().getId(), acl.getPerms(), Perms.ALL));
              }
            }
            break;
          }
        }
        if (!hasAccess) {
          return false;
        }
      }
    }
    return true;
  }
  
  /*
   * Validate whether ACL ID is superuser.
   */
  public static boolean isSuperUserId(String[] superUsers, Id id) {
    for (String user : superUsers) {
      // TODO: Validate super group members also when ZK supports setting node ACL for groups.
      if (!AuthUtil.isGroupPrincipal(user) && new Id("sasl", user).equals(id)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return this.identifier + ", quorum=" + quorum + ", baseZNode=" + baseZNode;
  }

  /**
   * Adds this instance's identifier as a prefix to the passed <code>str</code>
   * @param str String to amend.
   * @return A new string with this instance's identifier as prefix: e.g.
   * if passed 'hello world', the returned string could be
   */
  public String prefix(final String str) {
    return this.toString() + " " + str;
  }

  /**
   * Set the local variable node names using the specified configuration.
   */
  private void setNodeNames(Configuration conf) {
    baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    metaReplicaZnodes.put(0, ZKUtil.joinZNode(baseZNode,
           conf.get("zookeeper.znode.metaserver", "meta-region-server")));
    int numMetaReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
            HConstants.DEFAULT_META_REPLICA_NUM);
    for (int i = 1; i < numMetaReplicas; i++) {
      String str = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + i);
      metaReplicaZnodes.put(i, str);
    }
    rsZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.rs", "rs"));
    drainingZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.draining.rs", "draining"));
    masterAddressZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.master", "master"));
    backupMasterAddressesZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.backup.masters", "backup-masters"));
    clusterStateZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.state", "running"));
    assignmentZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.unassigned", "region-in-transition"));
    tableZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.tableEnableDisable", "table"));
    clusterIdZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.clusterId", "hbaseid"));
    splitLogZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.splitlog", HConstants.SPLIT_LOGDIR_NAME));
    balancerZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.balancer", "balancer"));
    regionNormalizerZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.regionNormalizer", "normalizer"));
    switchZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.switch", "switch"));
    tableLockZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.tableLock", "table-lock"));
    snapshotCleanupZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.snapshot.cleanup", DEFAULT_SNAPSHOT_CLEANUP_ZNODE));
    recoveringRegionsZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.recovering.regions", "recovering-regions"));
    namespaceZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.namespace", "namespace"));
    masterMaintZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.masterMaintenance", "master-maintenance"));
  }

  /**
   * Is the znode of any meta replica
   * @param node
   * @return true or false
   */
  public boolean isAnyMetaReplicaZnode(String node) {
    if (metaReplicaZnodes.values().contains(node)) {
      return true;
    }
    return false;
  }

  /**
   * Is it the default meta replica's znode
   * @param node
   * @return true or false
   */
  public boolean isDefaultMetaReplicaZnode(String node) {
    if (getZNodeForReplica(HRegionInfo.DEFAULT_REPLICA_ID).equals(node)) {
      return true;
    }
    return false;
  }

  /**
   * Get the znodes corresponding to the meta replicas from ZK
   * @return list of znodes
   * @throws KeeperException
   */
  public List<String> getMetaReplicaNodes() throws KeeperException {
    List<String> childrenOfBaseNode = ZKUtil.listChildrenNoWatch(this, baseZNode);
    List<String> metaReplicaNodes = new ArrayList<String>(2);
    if (childrenOfBaseNode != null) {
      String pattern = conf.get("zookeeper.znode.metaserver","meta-region-server");
      for (String child : childrenOfBaseNode) {
        if (child.startsWith(pattern)) metaReplicaNodes.add(child);
      }
    }
    return metaReplicaNodes;
  }

  /**
   * Get the znode string corresponding to a replicaId
   * @param replicaId
   * @return znode
   */
  public String getZNodeForReplica(int replicaId) {
    String str = metaReplicaZnodes.get(replicaId);
    // return a newly created path but don't update the cache of paths
    // This is mostly needed for tests that attempt to create meta replicas
    // from outside the master
    if (str == null) {
      str = ZKUtil.joinZNode(baseZNode,
          conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + replicaId);
    }
    return str;
  }

  /**
   * Parse the meta replicaId from the passed znode
   * @param znode
   * @return replicaId
   */
  public int getMetaReplicaIdFromZnode(String znode) {
    String pattern = conf.get("zookeeper.znode.metaserver","meta-region-server");
    if (znode.equals(pattern)) return HRegionInfo.DEFAULT_REPLICA_ID;
    // the non-default replicas are of the pattern meta-region-server-<replicaId>
    String nonDefaultPattern = pattern + "-";
    return Integer.parseInt(znode.substring(nonDefaultPattern.length()));
  }

  /**
   * Register the specified listener to receive ZooKeeper events.
   * @param listener
   */
  public void registerListener(ZooKeeperListener listener) {
    listeners.add(listener);
  }

  /**
   * Register the specified listener to receive ZooKeeper events and add it as
   * the first in the list of current listeners.
   * @param listener
   */
  public void registerListenerFirst(ZooKeeperListener listener) {
    listeners.add(0, listener);
  }

  public void unregisterListener(ZooKeeperListener listener) {
    listeners.remove(listener);
  }

  /**
   * Clean all existing listeners
   */
  public void unregisterAllListeners() {
    listeners.clear();
  }

  /**
   * Get a copy of current registered listeners
   */
  public List<ZooKeeperListener> getListeners() {
    return new ArrayList<ZooKeeperListener>(listeners);
  }

  /**
   * @return The number of currently registered listeners
   */
  public int getNumberOfListeners() {
    return listeners.size();
  }

  /**
   * Get the connection to ZooKeeper.
   * @return connection reference to zookeeper
   */
  public RecoverableZooKeeper getRecoverableZooKeeper() {
    return recoverableZooKeeper;
  }

  public void reconnectAfterExpiration() throws IOException, KeeperException, InterruptedException {
    recoverableZooKeeper.reconnectAfterExpiration();
  }

  /**
   * Get the quorum address of this instance.
   * @return quorum string of this zookeeper connection instance
   */
  public String getQuorum() {
    return quorum;
  }

  /**
   * @return the base znode of this zookeeper connection instance.
   */
  public String getBaseZNode() {
    return baseZNode;
  }

  /**
   * Method called from ZooKeeper for events and connection status.
   * <p>
   * Valid events are passed along to listeners.  Connection status changes
   * are dealt with locally.
   */
  @Override
  public void process(WatchedEvent event) {
    LOG.debug(prefix("Received ZooKeeper Event, " +
        "type=" + event.getType() + ", " +
        "state=" + event.getState() + ", " +
        "path=" + event.getPath()));

    switch(event.getType()) {

      // If event type is NONE, this is a connection status change
      case None: {
        connectionEvent(event);
        break;
      }

      // Otherwise pass along to the listeners

      case NodeCreated: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeCreated(event.getPath());
        }
        break;
      }

      case NodeDeleted: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDeleted(event.getPath());
        }
        break;
      }

      case NodeDataChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDataChanged(event.getPath());
        }
        break;
      }

      case NodeChildrenChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeChildrenChanged(event.getPath());
        }
        break;
      }
    }
  }

  // Connection management

  /**
   * Called when there is a connection-related event via the Watcher callback.
   * <p>
   * If Disconnected or Expired, this should shutdown the cluster. But, since
   * we send a KeeperException.SessionExpiredException along with the abort
   * call, it's possible for the Abortable to catch it and try to create a new
   * session with ZooKeeper. This is what the client does in HCM.
   * <p>
   * @param event
   */
  private void connectionEvent(WatchedEvent event) {
    switch(event.getState()) {
      case SyncConnected:
        this.identifier = this.prefix + "-0x" +
          Long.toHexString(this.recoverableZooKeeper.getSessionId());
        // Update our identifier.  Otherwise ignore.
        LOG.debug(this.identifier + " connected");
        break;

      // Abort the server if Disconnected or Expired
      case Disconnected:
        LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
        break;

      case Expired:
        String msg = prefix(this.identifier + " received expired from " +
          "ZooKeeper, aborting");
        // TODO: One thought is to add call to ZooKeeperListener so say,
        // ZooKeeperNodeTracker can zero out its data values.
        if (this.abortable != null) {
          this.abortable.abort(msg, new KeeperException.SessionExpiredException());
        }
        break;

      case ConnectedReadOnly:
      case SaslAuthenticated:
      case AuthFailed:
        break;

      default:
        throw new IllegalStateException("Received event is not valid: " + event.getState());
    }
  }

  /**
   * Forces a synchronization of this ZooKeeper client connection.
   * <p>
   * Executing this method before running other methods will ensure that the
   * subsequent operations are up-to-date and consistent as of the time that
   * the sync is complete.
   * <p>
   * This is used for compareAndSwap type operations where we need to read the
   * data of an existing node and delete or transition that node, utilizing the
   * previously read version and data.  We want to ensure that the version read
   * is up-to-date from when we begin the operation.
   */
  public void sync(String path) throws KeeperException {
    this.recoverableZooKeeper.sync(path, null, null);
  }

  /**
   * Handles KeeperExceptions in client calls.
   * <p>
   * This may be temporary but for now this gives one place to deal with these.
   * <p>
   * TODO: Currently this method rethrows the exception to let the caller handle
   * <p>
   * @param ke
   * @throws KeeperException
   */
  public void keeperException(KeeperException ke)
  throws KeeperException {
    LOG.error(prefix("Received unexpected KeeperException, re-throwing exception"), ke);
    throw ke;
  }

  /**
   * Handles InterruptedExceptions in client calls.
   * @param ie the InterruptedException instance thrown
   * @throws KeeperException the exception to throw, transformed from the InterruptedException
   */
  public void interruptedException(InterruptedException ie) throws KeeperException {
    interruptedExceptionNoThrow(ie, true);
    // Throw a system error exception to let upper level handle it
    throw new KeeperException.SystemErrorException();
  }

  /**
   * Log the InterruptedException and interrupt current thread
   * @param ie The IterruptedException to log
   * @param throwLater Whether we will throw the exception latter
   */
  public void interruptedExceptionNoThrow(InterruptedException ie, boolean throwLater) {
    LOG.debug(prefix("Received InterruptedException, will interrupt current thread"
        + (throwLater ? " and rethrow a SystemErrorException" : "")),
      ie);
    // At least preserve interrupt.
    Thread.currentThread().interrupt();
  }

  /**
   * Close the connection to ZooKeeper.
   *
   */
  @Override
  public void close() {
    try {
      recoverableZooKeeper.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.abortable != null) this.abortable.abort(why, e);
    else this.aborted = true;
  }

  @Override
  public boolean isAborted() {
    return this.abortable == null? this.aborted: this.abortable.isAborted();
  }

  /**
   * @return Path to the currently active master.
   */
  public String getMasterAddressZNode() {
    return this.masterAddressZNode;
  }

  /**
   * @return ZooKeeper znode for region normalizer state
   */
  public String getRegionNormalizerZNode() {
    return regionNormalizerZNode;
  }

  /**
   *  @return ZK node for switch
   * */
  public String getSwitchZNode() {
    return switchZNode;
  }
}
