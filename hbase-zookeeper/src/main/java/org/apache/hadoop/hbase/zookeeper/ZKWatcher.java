/*
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
public class ZKWatcher implements Watcher, Abortable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZKWatcher.class);

  // Identifier for this watcher (for logging only).  It is made of the prefix
  // passed on construction and the zookeeper sessionid.
  private final String prefix;
  private String identifier;

  // zookeeper quorum
  private final String quorum;

  // zookeeper connection
  private final RecoverableZooKeeper recoverableZooKeeper;

  // abortable in case of zk failure
  protected Abortable abortable;
  // Used if abortable is null
  private boolean aborted = false;

  private final ZNodePaths znodePaths;

  // listeners to be notified
  private final List<ZKListener> listeners = new CopyOnWriteArrayList<>();

  // Single threaded executor pool that processes event notifications from Zookeeper. Events are
  // processed in the order in which they arrive (pool backed by an unbounded fifo queue). We do
  // this to decouple the event processing from Zookeeper's ClientCnxn's EventThread context.
  // EventThread internally runs a single while loop to serially process all the events. When events
  // are processed by the listeners in the same thread, that blocks the EventThread from processing
  // subsequent events. Processing events in a separate thread frees up the event thread to continue
  // and further prevents deadlocks if the process method itself makes other zookeeper calls.
  // It is ok to do it in a single thread because the Zookeeper ClientCnxn already serializes the
  // requests using a single while loop and hence there is no performance degradation.
  private final ExecutorService zkEventProcessor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat("zk-event-processor-pool-%d").setDaemon(true)
      .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

  private final Configuration conf;

  private final long zkSyncTimeout;

  /* A pattern that matches a Kerberos name, borrowed from Hadoop's KerberosName */
  private static final Pattern NAME_PATTERN = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param identifier string that is passed to RecoverableZookeeper to be used as
   *                   identifier for this instance. Use null for default.
   * @throws IOException if the connection to ZooKeeper fails
   * @throws ZooKeeperConnectionException if the client can't connect to ZooKeeper
   */
  public ZKWatcher(Configuration conf, String identifier,
                   Abortable abortable) throws ZooKeeperConnectionException, IOException {
    this(conf, identifier, abortable, false);
  }

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param conf the configuration to use
   * @param identifier string that is passed to RecoverableZookeeper to be used as identifier for
   *          this instance. Use null for default.
   * @param abortable Can be null if there is on error there is no host to abort: e.g. client
   *          context.
   * @param canCreateBaseZNode true if a base ZNode can be created
   * @throws IOException if the connection to ZooKeeper fails
   * @throws ZooKeeperConnectionException if the client can't connect to ZooKeeper
   */
  public ZKWatcher(Configuration conf, String identifier,
                   Abortable abortable, boolean canCreateBaseZNode)
    throws IOException, ZooKeeperConnectionException {
    this(conf, identifier, abortable, canCreateBaseZNode, false);
  }

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param conf the configuration to use
   * @param identifier string that is passed to RecoverableZookeeper to be used as identifier for
   *          this instance. Use null for default.
   * @param abortable Can be null if there is on error there is no host to abort: e.g. client
   *          context.
   * @param canCreateBaseZNode true if a base ZNode can be created
   * @param clientZK whether this watcher is set to access client ZK
   * @throws IOException if the connection to ZooKeeper fails
   * @throws ZooKeeperConnectionException if the connection to Zookeeper fails when create base
   *           ZNodes
   */
  public ZKWatcher(Configuration conf, String identifier, Abortable abortable,
      boolean canCreateBaseZNode, boolean clientZK)
      throws IOException, ZooKeeperConnectionException {
    this.conf = conf;
    if (clientZK) {
      String clientZkQuorumServers = ZKConfig.getClientZKQuorumServersString(conf);
      String serverZkQuorumServers = ZKConfig.getZKQuorumServersString(conf);
      if (clientZkQuorumServers != null) {
        if (clientZkQuorumServers.equals(serverZkQuorumServers)) {
          // Don't allow same settings to avoid dead loop when master trying
          // to sync meta information from server ZK to client ZK
          throw new IllegalArgumentException(
              "The quorum settings for client ZK should be different from those for server");
        }
        this.quorum = clientZkQuorumServers;
      } else {
        this.quorum = serverZkQuorumServers;
      }
    } else {
      this.quorum = ZKConfig.getZKQuorumServersString(conf);
    }
    this.prefix = identifier;
    // Identifier will get the sessionid appended later below down when we
    // handle the syncconnect event.
    this.identifier = identifier + "0x0";
    this.abortable = abortable;
    this.znodePaths = new ZNodePaths(conf);
    PendingWatcher pendingWatcher = new PendingWatcher();
    this.recoverableZooKeeper =
      RecoverableZooKeeper.connect(conf, quorum, pendingWatcher, identifier);
    pendingWatcher.prepare(this);
    if (canCreateBaseZNode) {
      try {
        createBaseZNodes();
      } catch (ZooKeeperConnectionException zce) {
        try {
          this.recoverableZooKeeper.close();
        } catch (InterruptedException ie) {
          LOG.debug("Encountered InterruptedException when closing {}", this.recoverableZooKeeper);
          Thread.currentThread().interrupt();
        }
        throw zce;
      }
    }
    this.zkSyncTimeout = conf.getLong(HConstants.ZK_SYNC_BLOCKING_TIMEOUT_MS,
        HConstants.ZK_SYNC_BLOCKING_TIMEOUT_DEFAULT_MS);
  }

  public List<ACL> createACL(String node) {
    return createACL(node, ZKAuthentication.isSecureZooKeeper(getConfiguration()));
  }

  public List<ACL> createACL(String node, boolean isSecureZooKeeper) {
    if (!node.startsWith(getZNodePaths().baseZNode)) {
      return Ids.OPEN_ACL_UNSAFE;
    }
    if (isSecureZooKeeper) {
      ArrayList<ACL> acls = new ArrayList<>();
      // add permission to hbase supper user
      String[] superUsers = getConfiguration().getStrings(Superusers.SUPERUSER_CONF_KEY);
      String hbaseUser = null;
      try {
        hbaseUser = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        LOG.debug("Could not acquire current User.", e);
      }
      if (superUsers != null) {
        List<String> groups = new ArrayList<>();
        for (String user : superUsers) {
          if (AuthUtil.isGroupPrincipal(user)) {
            // TODO: Set node ACL for groups when ZK supports this feature
            groups.add(user);
          } else {
            if(!user.equals(hbaseUser)) {
              acls.add(new ACL(Perms.ALL, new Id("sasl", user)));
            }
          }
        }
        if (!groups.isEmpty()) {
          LOG.warn("Znode ACL setting for group {} is skipped, ZooKeeper doesn't support this " +
            "feature presently.", groups);
        }
      }
      // Certain znodes are accessed directly by the client,
      // so they must be readable by non-authenticated clients
      if (getZNodePaths().isClientReadable(node)) {
        acls.addAll(Ids.CREATOR_ALL_ACL);
        acls.addAll(Ids.READ_ACL_UNSAFE);
      } else {
        acls.addAll(Ids.CREATOR_ALL_ACL);
      }
      return acls;
    } else {
      return Ids.OPEN_ACL_UNSAFE;
    }
  }

  private void createBaseZNodes() throws ZooKeeperConnectionException {
    try {
      // Create all the necessary "directories" of znodes
      ZKUtil.createWithParents(this, znodePaths.baseZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.rsZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.drainingZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.tableZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.splitLogZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.backupMasterAddressesZNode);
      ZKUtil.createAndFailSilent(this, znodePaths.masterMaintZNode);
    } catch (KeeperException e) {
      throw new ZooKeeperConnectionException(
          prefix("Unexpected KeeperException creating base node"), e);
    }
  }

  /**
   * On master start, we check the znode ACLs under the root directory and set the ACLs properly
   * if needed. If the cluster goes from an unsecure setup to a secure setup, this step is needed
   * so that the existing znodes created with open permissions are now changed with restrictive
   * perms.
   */
  public void checkAndSetZNodeAcls() {
    if (!ZKAuthentication.isSecureZooKeeper(getConfiguration())) {
      LOG.info("not a secure deployment, proceeding");
      return;
    }

    // Check the base znodes permission first. Only do the recursion if base znode's perms are not
    // correct.
    try {
      List<ACL> actualAcls = recoverableZooKeeper.getAcl(znodePaths.baseZNode, new Stat());

      if (!isBaseZnodeAclSetup(actualAcls)) {
        LOG.info("setting znode ACLs");
        setZnodeAclsRecursive(znodePaths.baseZNode);
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
   * @param znode the ZNode to set the permissions for
   */
  private void setZnodeAclsRecursive(String znode) throws KeeperException, InterruptedException {
    List<String> children = recoverableZooKeeper.getChildren(znode, false);

    for (String child : children) {
      setZnodeAclsRecursive(ZNodePaths.joinZNode(znode, child));
    }
    List<ACL> acls = createACL(znode, true);
    LOG.info("Setting ACLs for znode:{} , acl:{}", znode, acls);
    recoverableZooKeeper.setAcl(znode, acls, -1);
  }

  /**
   * Checks whether the ACLs returned from the base znode (/hbase) is set for secure setup.
   * @param acls acls from zookeeper
   * @return whether ACLs are set for the base znode
   * @throws IOException if getting the current user fails
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
            LOG.debug("Unexpected shortname in SASL ACL: {}", id);
          }
          return false;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("unexpected ACL id '{}'", id);
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
    return this.identifier + ", quorum=" + quorum + ", baseZNode=" + znodePaths.baseZNode;
  }

  /**
   * Adds this instance's identifier as a prefix to the passed <code>str</code>
   * @param str String to amend.
   * @return A new string with this instance's identifier as prefix: e.g.
   *         if passed 'hello world', the returned string could be
   */
  public String prefix(final String str) {
    return this.toString() + " " + str;
  }

  /**
   * Get the znodes corresponding to the meta replicas from ZK
   * @return list of znodes
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public List<String> getMetaReplicaNodes() throws KeeperException {
    List<String> childrenOfBaseNode = ZKUtil.listChildrenNoWatch(this, znodePaths.baseZNode);
    return filterMetaReplicaNodes(childrenOfBaseNode);
  }

  /**
   * Same as {@link #getMetaReplicaNodes()} except that this also registers a watcher on base znode
   * for subsequent CREATE/DELETE operations on child nodes.
   */
  public List<String> getMetaReplicaNodesAndWatchChildren() throws KeeperException {
    List<String> childrenOfBaseNode =
        ZKUtil.listChildrenAndWatchForNewChildren(this, znodePaths.baseZNode);
    return filterMetaReplicaNodes(childrenOfBaseNode);
  }

  /**
   * @param nodes Input list of znodes
   * @return Filtered list of znodes from nodes that belong to meta replica(s).
   */
  private List<String> filterMetaReplicaNodes(List<String> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      return new ArrayList<>();
    }
    List<String> metaReplicaNodes = new ArrayList<>(2);
    String pattern = conf.get(ZNodePaths.META_ZNODE_PREFIX_CONF_KEY, ZNodePaths.META_ZNODE_PREFIX);
    for (String child : nodes) {
      if (child.startsWith(pattern)) {
        metaReplicaNodes.add(child);
      }
    }
    return metaReplicaNodes;
  }

  /**
   * Register the specified listener to receive ZooKeeper events.
   * @param listener the listener to register
   */
  public void registerListener(ZKListener listener) {
    listeners.add(listener);
  }

  /**
   * Register the specified listener to receive ZooKeeper events and add it as
   * the first in the list of current listeners.
   * @param listener the listener to register
   */
  public void registerListenerFirst(ZKListener listener) {
    listeners.add(0, listener);
  }

  public void unregisterListener(ZKListener listener) {
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
  public List<ZKListener> getListeners() {
    return new ArrayList<>(listeners);
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
   * Get the znodePaths.
   * <p>
   * Mainly used for mocking as mockito can not mock a field access.
   */
  public ZNodePaths getZNodePaths() {
    return znodePaths;
  }

  private void processEvent(WatchedEvent event) {
    switch(event.getType()) {
      // If event type is NONE, this is a connection status change
      case None: {
        connectionEvent(event);
        break;
      }

      // Otherwise pass along to the listeners
      case NodeCreated: {
        for(ZKListener listener : listeners) {
          listener.nodeCreated(event.getPath());
        }
        break;
      }

      case NodeDeleted: {
        for(ZKListener listener : listeners) {
          listener.nodeDeleted(event.getPath());
        }
        break;
      }

      case NodeDataChanged: {
        for(ZKListener listener : listeners) {
          listener.nodeDataChanged(event.getPath());
        }
        break;
      }

      case NodeChildrenChanged: {
        for(ZKListener listener : listeners) {
          listener.nodeChildrenChanged(event.getPath());
        }
        break;
      }
      default:
        LOG.error("Invalid event of type {} received for path {}. Ignoring.",
            event.getState(), event.getPath());
    }
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
    zkEventProcessor.submit(() -> processEvent(event));
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
   * @param event the connection-related event
   */
  private void connectionEvent(WatchedEvent event) {
    switch(event.getState()) {
      case SyncConnected:
        this.identifier = this.prefix + "-0x" +
          Long.toHexString(this.recoverableZooKeeper.getSessionId());
        // Update our identifier.  Otherwise ignore.
        LOG.debug("{} connected", this.identifier);
        break;

      // Abort the server if Disconnected or Expired
      case Disconnected:
        LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
        break;

      case Closed:
        LOG.debug(prefix("ZooKeeper client closed, ignoring"));
        break;

      case Expired:
        String msg = prefix(this.identifier + " received expired from " +
          "ZooKeeper, aborting");
        // TODO: One thought is to add call to ZKListener so say,
        // ZKNodeTracker can zero out its data values.
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
   * Forces a synchronization of this ZooKeeper client connection within a timeout. Enforcing a
   * timeout lets the callers fail-fast rather than wait forever for the sync to finish.
   * <p>
   * Executing this method before running other methods will ensure that the
   * subsequent operations are up-to-date and consistent as of the time that
   * the sync is complete.
   * <p>
   * This is used for compareAndSwap type operations where we need to read the
   * data of an existing node and delete or transition that node, utilizing the
   * previously read version and data.  We want to ensure that the version read
   * is up-to-date from when we begin the operation.
   * <p>
   */
  public void syncOrTimeout(String path) throws KeeperException {
    final CountDownLatch latch = new CountDownLatch(1);
    long startTime = EnvironmentEdgeManager.currentTime();
    this.recoverableZooKeeper.sync(path, (i, s, o) -> latch.countDown(), null);
    try {
      if (!latch.await(zkSyncTimeout, TimeUnit.MILLISECONDS)) {
        LOG.warn("sync() operation to ZK timed out. Configured timeout: {}ms. This usually points "
            + "to a ZK side issue. Check ZK server logs and metrics.", zkSyncTimeout);
        throw new KeeperException.RequestTimeoutException();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for ZK sync() to finish.", e);
      Thread.currentThread().interrupt();
      return;
    }
    if (LOG.isDebugEnabled()) {
      // TODO: Switch to a metric once server side ZK watcher metrics are implemented. This is a
      // useful metric to have since the latency of sync() impacts the callers.
      LOG.debug("ZK sync() operation took {}ms", EnvironmentEdgeManager.currentTime() - startTime);
    }
  }

  /**
   * Handles KeeperExceptions in client calls.
   * <p>
   * This may be temporary but for now this gives one place to deal with these.
   * <p>
   * TODO: Currently this method rethrows the exception to let the caller handle
   * <p>
   * @param ke the exception to rethrow
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public void keeperException(KeeperException ke) throws KeeperException {
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
    KeeperException keeperException = new KeeperException.SystemErrorException();
    keeperException.initCause(ie);
    throw keeperException;
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
    } finally {
      zkEventProcessor.shutdownNow();
    }
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.abortable != null) {
      this.abortable.abort(why, e);
    } else {
      this.aborted = true;
    }
  }

  @Override
  public boolean isAborted() {
    return this.abortable == null? this.aborted: this.abortable.isAborted();
  }
}
