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

package org.apache.hadoop.hbase.rsgroup;

import com.google.protobuf.ServiceException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.DefaultVisitorBase;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * This is an implementation of {@link RSGroupInfoManager} which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 *
 * <h2>Concurrency</h2>
 * RSGroup state is kept locally in Maps. There is a rsgroup name to cached
 * RSGroupInfo Map at {@link #rsGroupMap} and a Map of tables to the name of the
 * rsgroup they belong too (in {@link #tableMap}). These Maps are persisted to the
 * hbase:rsgroup table (and cached in zk) on each modification.
 *
 * <p>Mutations on state are synchronized but reads can continue without having
 * to wait on an instance monitor, mutations do wholesale replace of the Maps on
 * update -- Copy-On-Write; the local Maps of state are read-only, just-in-case
 * (see flushConfig).
 *
 * <p>Reads must not block else there is a danger we'll deadlock.
 *
 * <p>Clients of this class, the {@link RSGroupAdminEndpoint} for example, want to query and
 * then act on the results of the query modifying cache in zookeeper without another thread
 * making intermediate modifications. These clients synchronize on the 'this' instance so
 * no other has access concurrently. Reads must be able to continue concurrently.
 */
@InterfaceAudience.Private
final class RSGroupInfoManagerImpl implements RSGroupInfoManager {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupInfoManagerImpl.class);

  /** Table descriptor for <code>hbase:rsgroup</code> catalog table */
  private final static HTableDescriptor RSGROUP_TABLE_DESC;
  static {
    RSGROUP_TABLE_DESC = new HTableDescriptor(RSGROUP_TABLE_NAME);
    RSGROUP_TABLE_DESC.addFamily(new HColumnDescriptor(META_FAMILY_BYTES));
    RSGROUP_TABLE_DESC.setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName());
    try {
      RSGROUP_TABLE_DESC.addCoprocessor(
        MultiRowMutationEndpoint.class.getName(),
          null, Coprocessor.PRIORITY_SYSTEM, null);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  // There two Maps are immutable and wholesale replaced on each modification
  // so are safe to access concurrently. See class comment.
  private volatile Map<String, RSGroupInfo> rsGroupMap = Collections.emptyMap();
  private volatile Map<TableName, String> tableMap = Collections.emptyMap();

  private final MasterServices masterServices;
  private Table rsGroupTable;
  private final ClusterConnection conn;
  private final ZKWatcher watcher;
  private final RSGroupStartupWorker rsGroupStartupWorker = new RSGroupStartupWorker();
  // contains list of groups that were last flushed to persistent store
  private Set<String> prevRSGroups = new HashSet<>();
  private final ServerEventsListenerThread serverEventsListenerThread =
      new ServerEventsListenerThread();
  private FailedOpenUpdaterThread failedOpenUpdaterThread;

  private RSGroupInfoManagerImpl(MasterServices masterServices) throws IOException {
    this.masterServices = masterServices;
    this.watcher = masterServices.getZooKeeper();
    this.conn = masterServices.getClusterConnection();
  }

  private synchronized void init() throws IOException{
    refresh();
    serverEventsListenerThread.start();
    masterServices.getServerManager().registerListener(serverEventsListenerThread);
    failedOpenUpdaterThread = new FailedOpenUpdaterThread(masterServices.getConfiguration());
    failedOpenUpdaterThread.start();
    masterServices.getServerManager().registerListener(failedOpenUpdaterThread);
  }

  static RSGroupInfoManager getInstance(MasterServices master) throws IOException {
    RSGroupInfoManagerImpl instance = new RSGroupInfoManagerImpl(master);
    instance.init();
    return instance;
  }

  public void start(){
    // create system table of rsgroup
    rsGroupStartupWorker.start();
  }

  @Override
  public synchronized void addRSGroup(RSGroupInfo rsGroupInfo) throws IOException {
    checkGroupName(rsGroupInfo.getName());
    if (rsGroupMap.get(rsGroupInfo.getName()) != null ||
        rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+ rsGroupInfo.getName());
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(rsGroupInfo.getName(), rsGroupInfo);
    flushConfig(newGroupMap);
  }

  private RSGroupInfo getRSGroupInfo(final String groupName) throws DoNotRetryIOException {
    RSGroupInfo rsGroupInfo = getRSGroup(groupName);
    if (rsGroupInfo == null) {
      throw new DoNotRetryIOException("RSGroup " + groupName + " does not exist");
    }
    return rsGroupInfo;
  }

  @Override
  public synchronized Set<Address> moveServers(Set<Address> servers, String srcGroup,
      String dstGroup) throws IOException {
    RSGroupInfo src = getRSGroupInfo(srcGroup);
    RSGroupInfo dst = getRSGroupInfo(dstGroup);
    // If destination is 'default' rsgroup, only add servers that are online. If not online, drop
    // it. If not 'default' group, add server to 'dst' rsgroup EVEN IF IT IS NOT online (could be a
    // rsgroup of dead servers that are to come back later).
    Set<Address> onlineServers = dst.getName().equals(RSGroupInfo.DEFAULT_GROUP)?
        Utility.getOnlineServers(this.masterServices): null;
    for (Address el: servers) {
      src.removeServer(el);
      if (onlineServers != null) {
        if (!onlineServers.contains(el)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Dropping " + el + " during move-to-default rsgroup because not online");
          }
          continue;
        }
      }
      dst.addServer(el);
    }
    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);
    flushConfig(newGroupMap);
    return dst.getServers();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException {
    for (RSGroupInfo info: rsGroupMap.values()) {
      if (info.containsServer(serverHostPort)) {
        return info;
      }
    }
    return null;
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) {
    return rsGroupMap.get(groupName);
  }

  @Override
  public String getRSGroupOfTable(TableName tableName) {
    return tableMap.get(tableName);
  }

  @Override
  public synchronized void moveTables(Set<TableName> tableNames, String groupName)
      throws IOException {
    if (groupName != null && !rsGroupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist");
    }

    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    for(TableName tableName: tableNames) {
      if (tableMap.containsKey(tableName)) {
        RSGroupInfo src = new RSGroupInfo(newGroupMap.get(tableMap.get(tableName)));
        src.removeTable(tableName);
        newGroupMap.put(src.getName(), src);
      }
      if(groupName != null) {
        RSGroupInfo dst = new RSGroupInfo(newGroupMap.get(groupName));
        dst.addTable(tableName);
        newGroupMap.put(dst.getName(), dst);
      }
    }
    flushConfig(newGroupMap);
  }

  @Override
  public synchronized void removeRSGroup(String groupName) throws IOException {
    if (!rsGroupMap.containsKey(groupName) || groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group " + groupName + " does not exist or is a reserved "
          + "group");
    }
    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(groupName);
    flushConfig(newGroupMap);
  }

  @Override
  public List<RSGroupInfo> listRSGroups() {
    return Lists.newLinkedList(rsGroupMap.values());
  }

  @Override
  public boolean isOnline() {
    return rsGroupStartupWorker.isOnline();
  }

  @Override
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables,
                                   String srcGroup, String dstGroup) throws IOException {
    //get server's group
    RSGroupInfo srcGroupInfo = getRSGroupInfo(srcGroup);
    RSGroupInfo dstGroupInfo = getRSGroupInfo(dstGroup);

    //move servers
    for (Address el: servers) {
      srcGroupInfo.removeServer(el);
      dstGroupInfo.addServer(el);
    }
    //move tables
    for(TableName tableName: tables) {
      srcGroupInfo.removeTable(tableName);
      dstGroupInfo.addTable(tableName);
    }

    //flush changed groupinfo
    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(srcGroupInfo.getName(), srcGroupInfo);
    newGroupMap.put(dstGroupInfo.getName(), dstGroupInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public synchronized void removeServers(Set<Address> servers) throws IOException {
    Map<String, RSGroupInfo> rsGroupInfos = new HashMap<String, RSGroupInfo>();
    for (Address el: servers) {
      RSGroupInfo rsGroupInfo = getRSGroupOfServer(el);
      if (rsGroupInfo != null) {
        RSGroupInfo newRsGroupInfo = rsGroupInfos.get(rsGroupInfo.getName());
        if (newRsGroupInfo == null) {
          rsGroupInfo.removeServer(el);
          rsGroupInfos.put(rsGroupInfo.getName(), rsGroupInfo);
        } else {
          newRsGroupInfo.removeServer(el);
          rsGroupInfos.put(newRsGroupInfo.getName(), newRsGroupInfo);
        }
      }else {
        LOG.warn("Server " + el + " does not belong to any rsgroup.");
      }
    }

    if (rsGroupInfos.size() > 0) {
      Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
      newGroupMap.putAll(rsGroupInfos);
      flushConfig(newGroupMap);
    }
  }

  List<RSGroupInfo> retrieveGroupListFromGroupTable() throws IOException {
    List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
    for (Result result : rsGroupTable.getScanner(new Scan())) {
      RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo.parseFrom(
              result.getValue(META_FAMILY_BYTES, META_QUALIFIER_BYTES));
      rsGroupInfoList.add(RSGroupProtobufUtil.toGroupInfo(proto));
    }
    return rsGroupInfoList;
  }

  List<RSGroupInfo> retrieveGroupListFromZookeeper() throws IOException {
    String groupBasePath = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, rsGroupZNode);
    List<RSGroupInfo> RSGroupInfoList = Lists.newArrayList();
    //Overwrite any info stored by table, this takes precedence
    try {
      if(ZKUtil.checkExists(watcher, groupBasePath) != -1) {
        List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(watcher, groupBasePath);
        if (children == null) {
          return RSGroupInfoList;
        }
        for(String znode: children) {
          byte[] data = ZKUtil.getData(watcher, ZNodePaths.joinZNode(groupBasePath, znode));
          if(data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis = new ByteArrayInputStream(
                data, ProtobufUtil.lengthOfPBMagic(), data.length);
            RSGroupInfoList.add(RSGroupProtobufUtil.toGroupInfo(
                RSGroupProtos.RSGroupInfo.parseFrom(bis)));
          }
        }
        LOG.debug("Read ZK GroupInfo count:" + RSGroupInfoList.size());
      }
    } catch (KeeperException|DeserializationException|InterruptedException e) {
      throw new IOException("Failed to read rsGroupZNode",e);
    }
    return RSGroupInfoList;
  }

  @Override
  public void refresh() throws IOException {
    refresh(false);
  }

  /**
   * Read rsgroup info from the source of truth, the hbase:rsgroup table.
   * Update zk cache. Called on startup of the manager.
   */
  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<RSGroupInfo> groupList = new LinkedList<>();

    // Overwrite anything read from zk, group table is source of truth
    // if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refreshing in Online mode.");
      if (rsGroupTable == null) {
        rsGroupTable = conn.getTable(RSGROUP_TABLE_NAME);
      }
      groupList.addAll(retrieveGroupListFromGroupTable());
    } else {
      LOG.debug("Refreshing in Offline mode.");
      groupList.addAll(retrieveGroupListFromZookeeper());
    }

    // refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<>();
    for(String entry: masterServices.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }
    for (RSGroupInfo group: groupList) {
      if(!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }

    // This is added to the last of the list so it overwrites the 'default' rsgroup loaded
    // from region group table or zk
    groupList.add(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, getDefaultServers(),
        orphanTables));

    // populate the data
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap();
    HashMap<TableName, String> newTableMap = Maps.newHashMap();
    for (RSGroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
      for(TableName table: group.getTables()) {
        newTableMap.put(table, group.getName());
      }
    }
    resetRSGroupAndTableMaps(newGroupMap, newTableMap);
    updateCacheOfRSGroups(rsGroupMap.keySet());
  }

  private synchronized Map<TableName,String> flushConfigTable(Map<String,RSGroupInfo> groupMap)
      throws IOException {
    Map<TableName,String> newTableMap = Maps.newHashMap();
    List<Mutation> mutations = Lists.newArrayList();

    // populate deletes
    for(String groupName : prevRSGroups) {
      if(!groupMap.containsKey(groupName)) {
        Delete d = new Delete(Bytes.toBytes(groupName));
        mutations.add(d);
      }
    }

    // populate puts
    for(RSGroupInfo RSGroupInfo : groupMap.values()) {
      RSGroupProtos.RSGroupInfo proto = RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo);
      Put p = new Put(Bytes.toBytes(RSGroupInfo.getName()));
      p.addColumn(META_FAMILY_BYTES, META_QUALIFIER_BYTES, proto.toByteArray());
      mutations.add(p);
      for(TableName entry: RSGroupInfo.getTables()) {
        newTableMap.put(entry, RSGroupInfo.getName());
      }
    }

    if(mutations.size() > 0) {
      multiMutate(mutations);
    }
    return newTableMap;
  }

  private synchronized void flushConfig()
  throws IOException {
    flushConfig(this.rsGroupMap);
  }

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap)
  throws IOException {
    Map<TableName, String> newTableMap;

    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for default servers
    if (!isOnline()) {
      Map<String, RSGroupInfo> m = Maps.newHashMap(rsGroupMap);
      RSGroupInfo oldDefaultGroup = m.remove(RSGroupInfo.DEFAULT_GROUP);
      RSGroupInfo newDefaultGroup = newGroupMap.remove(RSGroupInfo.DEFAULT_GROUP);
      if (!m.equals(newGroupMap) ||
          !oldDefaultGroup.getTables().equals(newDefaultGroup.getTables())) {
        throw new IOException("Only default servers can be updated during offline mode");
      }
      newGroupMap.put(RSGroupInfo.DEFAULT_GROUP, newDefaultGroup);
      rsGroupMap = newGroupMap;
      return;
    }

    newTableMap = flushConfigTable(newGroupMap);

    // Make changes visible after having been persisted to the source of truth
    resetRSGroupAndTableMaps(newGroupMap, newTableMap);

    try {
      String groupBasePath = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, rsGroupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufMagic.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<>(newGroupMap.size());
      for(String groupName : prevRSGroups) {
        if(!newGroupMap.containsKey(groupName)) {
          String znode = ZNodePaths.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }


      for (RSGroupInfo RSGroupInfo : newGroupMap.values()) {
        String znode = ZNodePaths.joinZNode(groupBasePath, RSGroupInfo.getName());
        RSGroupProtos.RSGroupInfo proto = RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo);
        LOG.debug("Updating znode: "+znode);
        ZKUtil.createAndFailSilent(watcher, znode);
        zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        zkOps.add(ZKUtil.ZKUtilOp.createAndFailSilent(znode,
            ProtobufUtil.prependPBMagic(proto.toByteArray())));
      }
      LOG.debug("Writing ZK GroupInfo count: " + zkOps.size());

      ZKUtil.multiOrSequential(watcher, zkOps, false);
    } catch (KeeperException e) {
      LOG.error("Failed to write to rsGroupZNode", e);
      masterServices.abort("Failed to write to rsGroupZNode", e);
      throw new IOException("Failed to write to rsGroupZNode",e);
    }
    updateCacheOfRSGroups(newGroupMap.keySet());
  }

  /**
   * Make changes visible.
   * Caller must be synchronized on 'this'.
   */
  private void resetRSGroupAndTableMaps(Map<String, RSGroupInfo> newRSGroupMap,
      Map<TableName, String> newTableMap) {
    // Make maps Immutable.
    this.rsGroupMap = Collections.unmodifiableMap(newRSGroupMap);
    this.tableMap = Collections.unmodifiableMap(newTableMap);
  }

  /**
   * Update cache of rsgroups.
   * Caller must be synchronized on 'this'.
   * @param currentGroups Current list of Groups.
   */
  private void updateCacheOfRSGroups(final Set<String> currentGroups) {
    this.prevRSGroups.clear();
    this.prevRSGroups.addAll(currentGroups);
  }

  // Called by getDefaultServers. Presume it has lock in place.
  private List<ServerName> getOnlineRS() throws IOException {
    if (masterServices != null) {
      return masterServices.getServerManager().getOnlineServersList();
    }
    LOG.debug("Reading online RS from zookeeper");
    List<ServerName> servers = new LinkedList<>();
    try {
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.getZNodePaths().rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
    return servers;
  }

  // Called by ServerEventsListenerThread. Presume it has lock on this manager when it runs.
  private SortedSet<Address> getDefaultServers() throws IOException {
    SortedSet<Address> defaultServers = Sets.newTreeSet();
    for (ServerName serverName : getOnlineRS()) {
      Address server =
          Address.fromParts(serverName.getHostname(), serverName.getPort());
      boolean found = false;
      for(RSGroupInfo rsgi: listRSGroups()) {
        if(!RSGroupInfo.DEFAULT_GROUP.equals(rsgi.getName()) &&
            rsgi.containsServer(server)) {
          found = true;
          break;
        }
      }
      if (!found) {
        defaultServers.add(server);
      }
    }
    return defaultServers;
  }

  // Called by ServerEventsListenerThread. Synchronize on this because redoing
  // the rsGroupMap then writing it out.
  private synchronized void updateDefaultServers(SortedSet<Address> servers) throws IOException {
    RSGroupInfo info = rsGroupMap.get(RSGroupInfo.DEFAULT_GROUP);
    RSGroupInfo newInfo = new RSGroupInfo(info.getName(), servers, info.getTables());
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(newInfo.getName(), newInfo);
    flushConfig(newGroupMap);
  }

  // Called by FailedOpenUpdaterThread
  private void updateFailedAssignments() {
    // Kick all regions in FAILED_OPEN state
    List<RegionInfo> stuckAssignments = Lists.newArrayList();
    for (RegionStateNode state:
        masterServices.getAssignmentManager().getRegionStates().getRegionsInTransition()) {
      if (state.isStuck()) {
        stuckAssignments.add(state.getRegionInfo());
      }
    }
    for (RegionInfo region: stuckAssignments) {
      LOG.info("Retrying assignment of " + region);
      try {
        masterServices.getAssignmentManager().unassign(region);
      } catch (IOException e) {
        LOG.warn("Unable to reassign " + region, e);
      }
    }
  }

  /**
   * Calls {@link RSGroupInfoManagerImpl#updateDefaultServers(SortedSet)} to update list of known
   * servers. Notifications about server changes are received by registering {@link ServerListener}.
   * As a listener, we need to return immediately, so the real work of updating the servers is
   * done asynchronously in this thread.
   */
  private class ServerEventsListenerThread extends Thread implements ServerListener {
    private final Logger LOG = LoggerFactory.getLogger(ServerEventsListenerThread.class);
    private boolean changed = false;

    ServerEventsListenerThread() {
      setDaemon(true);
    }

    @Override
    public void serverAdded(ServerName serverName) {
      serverChanged();
    }

    @Override
    public void serverRemoved(ServerName serverName) {
      serverChanged();
    }

    private synchronized void serverChanged() {
      changed = true;
      this.notify();
    }

    @Override
    public void run() {
      setName(ServerEventsListenerThread.class.getName() + "-" + masterServices.getServerName());
      SortedSet<Address> prevDefaultServers = new TreeSet<>();
      while(isMasterRunning(masterServices)) {
        try {
          LOG.info("Updating default servers.");
          SortedSet<Address> servers = RSGroupInfoManagerImpl.this.getDefaultServers();
          if (!servers.equals(prevDefaultServers)) {
            RSGroupInfoManagerImpl.this.updateDefaultServers(servers);
            prevDefaultServers = servers;
            LOG.info("Updated with servers: "+servers.size());
          }
          try {
            synchronized (this) {
              while (!changed) {
                wait();
              }
              changed = false;
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
          }
        } catch (IOException e) {
          LOG.warn("Failed to update default servers", e);
        }
      }
    }
  }

  private class FailedOpenUpdaterThread extends Thread implements ServerListener {
    private final long waitInterval;
    private volatile boolean hasChanged = false;

    public FailedOpenUpdaterThread(Configuration conf) {
      this.waitInterval = conf.getLong(REASSIGN_WAIT_INTERVAL_KEY,
        DEFAULT_REASSIGN_WAIT_INTERVAL);
      setDaemon(true);
    }

    @Override
    public void serverAdded(ServerName serverName) {
      serverChanged();
    }

    @Override
    public void serverRemoved(ServerName serverName) {
    }

    @Override
    public void run() {
      while (isMasterRunning(masterServices)) {
        boolean interrupted = false;
        try {
          synchronized (this) {
            while (!hasChanged) {
              wait();
            }
            hasChanged = false;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
          interrupted = true;
        }
        if (!isMasterRunning(masterServices) || interrupted) {
          continue;
        }

        // First, wait a while in case more servers are about to rejoin the cluster
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
        }
        if (!isMasterRunning(masterServices)) {
          continue;
        }

        // Kick all regions in FAILED_OPEN state
        updateFailedAssignments();
      }
    }

    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
        this.notify();
      }
    }
  }

  private class RSGroupStartupWorker extends Thread {
    private final Logger LOG = LoggerFactory.getLogger(RSGroupStartupWorker.class);
    private volatile boolean online = false;

    RSGroupStartupWorker() {
      setDaemon(true);
    }

    @Override
    public void run() {
      setName(RSGroupStartupWorker.class.getName() + "-" + masterServices.getServerName());
      if (waitForGroupTableOnline()) {
        LOG.info("GroupBasedLoadBalancer is now online");
      }
    }

    private boolean waitForGroupTableOnline() {
      final List<RegionInfo> foundRegions = new LinkedList<>();
      final List<RegionInfo> assignedRegions = new LinkedList<>();
      final AtomicBoolean found = new AtomicBoolean(false);
      final TableStateManager tsm = masterServices.getTableStateManager();
      boolean createSent = false;
      while (!found.get() && isMasterRunning(masterServices)) {
        foundRegions.clear();
        assignedRegions.clear();
        found.set(true);
        try {
          conn.getTable(TableName.NAMESPACE_TABLE_NAME);
          conn.getTable(RSGROUP_TABLE_NAME);
          boolean rootMetaFound =
              masterServices.getMetaTableLocator().verifyMetaRegionLocation(
                  conn, masterServices.getZooKeeper(), 1);
          final AtomicBoolean nsFound = new AtomicBoolean(false);
          if (rootMetaFound) {
            MetaTableAccessor.Visitor visitor = new DefaultVisitorBase() {
              @Override
              public boolean visitInternal(Result row) throws IOException {
                RegionInfo info = MetaTableAccessor.getRegionInfo(row);
                if (info != null) {
                  Cell serverCell =
                      row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                          HConstants.SERVER_QUALIFIER);
                  if (RSGROUP_TABLE_NAME.equals(info.getTable()) && serverCell != null) {
                    ServerName sn =
                        ServerName.parseVersionedServerName(CellUtil.cloneValue(serverCell));
                    if (sn == null) {
                      found.set(false);
                    } else if (tsm.isTableState(RSGROUP_TABLE_NAME, TableState.State.ENABLED)) {
                      try {
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest request =
                            RequestConverter.buildGetRequest(info.getRegionName(),
                                new Get(ROW_KEY));
                        rs.get(null, request);
                        assignedRegions.add(info);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                    foundRegions.add(info);
                  }
                  if (TableName.NAMESPACE_TABLE_NAME.equals(info.getTable())) {
                    Cell cell = row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                        HConstants.SERVER_QUALIFIER);
                    ServerName sn = null;
                    if(cell != null) {
                      sn = ServerName.parseVersionedServerName(CellUtil.cloneValue(cell));
                    }
                    if (sn == null) {
                      nsFound.set(false);
                    } else if (tsm.isTableState(TableName.NAMESPACE_TABLE_NAME,
                        TableState.State.ENABLED)) {
                      try {
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest request =
                            RequestConverter.buildGetRequest(info.getRegionName(),
                                new Get(ROW_KEY));
                        rs.get(null, request);
                        nsFound.set(true);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                  }
                }
                return true;
              }
            };
            MetaTableAccessor.fullScanRegions(conn, visitor);
            // if no regions in meta then we have to create the table
            if (foundRegions.size() < 1 && rootMetaFound && !createSent && nsFound.get()) {
              createRSGroupTable();
              createSent = true;
            }
            LOG.info("RSGroup table=" + RSGROUP_TABLE_NAME + " isOnline=" + found.get()
                + ", regionCount=" + foundRegions.size() + ", assignCount="
                + assignedRegions.size() + ", rootMetaFound=" + rootMetaFound);
            found.set(found.get() && assignedRegions.size() == foundRegions.size()
                && foundRegions.size() > 0);
          } else {
            LOG.info("Waiting for catalog tables to come online");
            found.set(false);
          }
          if (found.get()) {
            LOG.debug("With group table online, refreshing cached information.");
            RSGroupInfoManagerImpl.this.refresh(true);
            online = true;
            //flush any inconsistencies between ZK and HTable
            RSGroupInfoManagerImpl.this.flushConfig();
          }
        } catch (RuntimeException e) {
          throw e;
        } catch(Exception e) {
          found.set(false);
          LOG.warn("Failed to perform check", e);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted", e);
        }
      }
      return found.get();
    }

    private void createRSGroupTable() throws IOException {
      Long procId = masterServices.createSystemTable(RSGROUP_TABLE_DESC);
      // wait for region to be online
      int tries = 600;
      while (!(masterServices.getMasterProcedureExecutor().isFinished(procId))
          && masterServices.getMasterProcedureExecutor().isRunning()
          && tries > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new IOException("Wait interrupted ", e);
        }
        tries--;
      }
      if(tries <= 0) {
        throw new IOException("Failed to create group table in a given time.");
      } else {
        Procedure<?> result = masterServices.getMasterProcedureExecutor().getResult(procId);
        if (result != null && result.isFailed()) {
          throw new IOException("Failed to create group table. " +
            result.getException().unwrapRemoteIOException());
        }
      }
    }

    public boolean isOnline() {
      return online;
    }
  }

  private static boolean isMasterRunning(MasterServices masterServices) {
    return !masterServices.isAborted() && !masterServices.isStopped();
  }

  private void multiMutate(List<Mutation> mutations) throws IOException {
    CoprocessorRpcChannel channel = rsGroupTable.coprocessorService(ROW_KEY);
    MultiRowMutationProtos.MutateRowsRequest.Builder mmrBuilder
      = MultiRowMutationProtos.MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        mmrBuilder.addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
            org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT,
            mutation));
      } else if (mutation instanceof Delete) {
        mmrBuilder.addMutationRequest(
            org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
                org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.
                  MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException("multiMutate doesn't support "
          + mutation.getClass().getName());
      }
    }

    MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
      MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
    try {
      service.mutateRows(null, mmrBuilder.build());
    } catch (ServiceException ex) {
      ProtobufUtil.toIOException(ex);
    }
  }

  private void checkGroupName(String groupName) throws ConstraintException {
    if (!groupName.matches("[a-zA-Z0-9_]+")) {
      throw new ConstraintException("RSGroup name should only contain alphanumeric characters");
    }
  }
}
