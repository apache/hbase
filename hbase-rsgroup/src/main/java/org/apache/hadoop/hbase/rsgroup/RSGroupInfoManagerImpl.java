/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.rsgroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This is an implementation of {@link RSGroupInfoManager}. Which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 */
public class RSGroupInfoManagerImpl implements RSGroupInfoManager, ServerListener {
  private static final Log LOG = LogFactory.getLog(RSGroupInfoManagerImpl.class);

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

  private volatile Map<String, RSGroupInfo> rsGroupMap;
  private volatile Map<TableName, String> tableMap;
  private MasterServices master;
  private ClusterConnection conn;
  private ZooKeeperWatcher watcher;
  private RSGroupStartupWorker rsGroupStartupWorker;
  // contains list of groups that were last flushed to persistent store
  private volatile Set<String> prevRSGroups;
  private RSGroupSerDe rsGroupSerDe;
  private DefaultServerUpdater defaultServerUpdater;
  private boolean isInit = false;

  public RSGroupInfoManagerImpl(MasterServices master) throws IOException {
    this.rsGroupMap = Collections.emptyMap();
    this.tableMap = Collections.emptyMap();
    rsGroupSerDe = new RSGroupSerDe();
    this.master = master;
    this.watcher = master.getZooKeeper();
    this.conn = master.getConnection();
    prevRSGroups = new HashSet<String>();
  }

  public void init() throws IOException{
    rsGroupStartupWorker = new RSGroupStartupWorker(this, master, conn);
    refresh();
    defaultServerUpdater = new DefaultServerUpdater(this);
    Threads.setDaemonThreadRunning(defaultServerUpdater);
    master.getServerManager().registerListener(this);
    isInit = true;
  }

  boolean isInit() {
    return isInit;
  }

  public void start(){
    // create system table of rsgroup
    rsGroupStartupWorker.start();
  }

  /**
   * Adds the group.
   *
   * @param rsGroupInfo the group name
   */
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

  @Override
  public synchronized Set<Address> moveServers(Set<Address> servers, String srcGroup,
                                          String dstGroup) throws IOException {
    if (servers == null) {
      throw new ConstraintException("The list of servers to move cannot be null.");
    }
    Set<Address> movedServers = Sets.newHashSet();
    if (!rsGroupMap.containsKey(srcGroup)) {
      throw new DoNotRetryIOException("Group "+srcGroup+" does not exist");
    }
    if (!rsGroupMap.containsKey(dstGroup)) {
      throw new DoNotRetryIOException("Group "+dstGroup+" does not exist");
    }

    RSGroupInfo src = new RSGroupInfo(getRSGroup(srcGroup));
    RSGroupInfo dst = new RSGroupInfo(getRSGroup(dstGroup));
    for(Address el: servers) {
      if (src.removeServer(el)) {
        movedServers.add(el);
      }
      dst.addServer(el);
    }

    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);

    flushConfig(newGroupMap);
    return movedServers;
  }

  /**
   * Gets the group info of server.
   *
   * @param server the server
   * @return An instance of GroupInfo.
   */
  @Override
  public RSGroupInfo getRSGroupOfServer(Address server) throws IOException {
    for (RSGroupInfo info : rsGroupMap.values()) {
      if (info.containsServer(server)){
        return info;
      }
    }
    return null;
  }

  /**
   * Gets the group information.
   *
   * @param groupName
   *          the group name
   * @return An instance of GroupInfo
   */
  @Override
  public RSGroupInfo getRSGroup(String groupName) throws IOException {
    return rsGroupMap.get(groupName);
  }

  /**
   * Gets the group name for a given table
   *
   * @param tableName the table name
   * @return group name, or null if not found
   */
  @Override
  public String getRSGroupOfTable(TableName tableName) throws IOException {
    return tableMap.get(tableName);
  }

  @Override
  public synchronized void moveTables(
      Set<TableName> tableNames, String groupName) throws IOException {
    // Check if rsGroup contains the destination rsgroup
    if (groupName != null && !rsGroupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist");
    }

    // Make a copy of rsGroupMap to update
    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);

    // Remove tables from their original rsgroups
    // and update the copy of rsGroupMap
    for(TableName tableName: tableNames) {
      if (tableMap.containsKey(tableName)) {
        RSGroupInfo src = new RSGroupInfo(newGroupMap.get(tableMap.get(tableName)));
        src.removeTable(tableName);
        newGroupMap.put(src.getName(), src);
      }
    }

    // Add tables to the destination rsgroup
    // and update the copy of rsGroupMap
    if (groupName != null) {
      RSGroupInfo dstGroup = new RSGroupInfo(newGroupMap.get(groupName));
      dstGroup.addAllTables(tableNames);
      newGroupMap.put(dstGroup.getName(), dstGroup);
    }

    // Flush according to the updated copy of rsGroupMap
    flushConfig(newGroupMap);
  }


  /**
   * Delete a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  @Override
  public synchronized void removeRSGroup(String groupName) throws IOException {
    if (!rsGroupMap.containsKey(groupName) || groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a reserved group");
    }
    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(groupName);
    flushConfig(newGroupMap);
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    List<RSGroupInfo> list = Lists.newLinkedList(rsGroupMap.values());
    return list;
  }

  @Override
  public boolean isOnline() {
    return rsGroupStartupWorker.isOnline();
  }

  @Override
  public synchronized void refresh() throws IOException {
    refresh(false);
  }

  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<RSGroupInfo> groupList = new LinkedList<RSGroupInfo>();

    // overwrite anything read from zk, group table is source of truth
    // if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refreshing in Online mode.");
      try (Table rsGroupTable = conn.getTable(RSGROUP_TABLE_NAME)) {
        groupList.addAll(rsGroupSerDe.retrieveGroupList(rsGroupTable));
      }
    } else {
      LOG.debug("Refreshing in Offline mode.");
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, rsGroupZNode);
      groupList.addAll(rsGroupSerDe.retrieveGroupList(watcher, groupBasePath));
    }

    // refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<TableName>();
    for(String entry: master.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }

    for (RSGroupInfo group: groupList) {
      if(!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }

    // This is added to the last of the list
    // so it overwrites the default group loaded
    // from region group table or zk
    groupList.add(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP,
        Sets.newHashSet(getDefaultServers()),
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
    rsGroupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);

    prevRSGroups.clear();
    prevRSGroups.addAll(rsGroupMap.keySet());
  }

  private synchronized Map<TableName,String> flushConfigTable(Map<String,RSGroupInfo> newGroupMap)
      throws IOException {
    Map<TableName,String> newTableMap = Maps.newHashMap();
    List<Mutation> mutations = Lists.newArrayList();

    // populate deletes
    for(String groupName : prevRSGroups) {
      if(!newGroupMap.containsKey(groupName)) {
        Delete d = new Delete(Bytes.toBytes(groupName));
        mutations.add(d);
      }
    }

    // populate puts
    for(RSGroupInfo RSGroupInfo : newGroupMap.values()) {
      RSGroupProtos.RSGroupInfo proto = RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo);
      Put p = new Put(Bytes.toBytes(RSGroupInfo.getName()));
      p.addColumn(META_FAMILY_BYTES,
          META_QUALIFIER_BYTES,
          proto.toByteArray());
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

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap) throws IOException {
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

    // make changes visible since it has been
    // persisted in the source of truth
    rsGroupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);


    try {
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, rsGroupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufUtil.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<ZKUtil.ZKUtilOp>(newGroupMap.size());
      for(String groupName : prevRSGroups) {
        if(!newGroupMap.containsKey(groupName)) {
          String znode = ZKUtil.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }


      for(RSGroupInfo RSGroupInfo : newGroupMap.values()) {
        String znode = ZKUtil.joinZNode(groupBasePath, RSGroupInfo.getName());
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
      master.abort("Failed to write to rsGroupZNode", e);
      throw new IOException("Failed to write to rsGroupZNode",e);
    }

    prevRSGroups.clear();
    prevRSGroups.addAll(newGroupMap.keySet());
  }

  private List<ServerName> getOnlineRS() throws IOException {
    if (master != null) {
      return master.getServerManager().getOnlineServersList();
    }
    try {
      LOG.debug("Reading online RS from zookeeper");
      List<ServerName> servers = new LinkedList<ServerName>();
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
  }

  private List<Address> getDefaultServers() throws IOException {
    List<Address> defaultServers = new LinkedList<Address>();
    for(ServerName server : getOnlineRS()) {
      Address address = Address.fromParts(server.getHostname(), server.getPort());
      boolean found = false;
      for(RSGroupInfo info : rsGroupMap.values()) {
        if(!RSGroupInfo.DEFAULT_GROUP.equals(info.getName()) &&
            info.containsServer(address)) {
          found = true;
          break;
        }
      }
      if(!found) {
        defaultServers.add(address);
      }
    }
    return defaultServers;
  }

  private synchronized void updateDefaultServers(
      Set<Address> server) throws IOException {
    RSGroupInfo info = rsGroupMap.get(RSGroupInfo.DEFAULT_GROUP);
    RSGroupInfo newInfo = new RSGroupInfo(info.getName(), server, info.getTables());
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(newInfo.getName(), newInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public void serverAdded(ServerName serverName) {
    defaultServerUpdater.serverChanged();
  }

  @Override
  public void serverRemoved(ServerName serverName) {
    defaultServerUpdater.serverChanged();
  }

  private static class DefaultServerUpdater extends Thread {
    private static final Log LOG = LogFactory.getLog(DefaultServerUpdater.class);
    private RSGroupInfoManagerImpl mgr;
    private volatile boolean hasChanged = false;

    public DefaultServerUpdater(RSGroupInfoManagerImpl mgr) {
      this.mgr = mgr;
      setName(DefaultServerUpdater.class.getName()+"-" + mgr.master.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      List<Address> prevDefaultServers = new LinkedList<Address>();
      while (!mgr.master.isAborted() && !mgr.master.isStopped()) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Updating default servers");
          }
          List<Address> servers = mgr.getDefaultServers();
          Collections.sort(servers, new Comparator<Address>() {
            @Override
            public int compare(Address o1, Address o2) {
              int diff = o1.getHostname().compareTo(o2.getHostname());
              if (diff != 0) {
                return diff;
              }
              return o1.getPort() - o2.getPort();
            }
          });
          if(!servers.equals(prevDefaultServers)) {
            mgr.updateDefaultServers(Sets.<Address>newHashSet(servers));
            prevDefaultServers = servers;
            LOG.info("Updated with servers: "+servers.size());
          }
          try {
            synchronized (this) {
              while (!hasChanged) {
                wait();
              }
              hasChanged = false;
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
          }
        } catch (IOException e) {
          LOG.warn("Failed to update default servers", e);
        }
      }
    }

    // Called for both server additions and removals
    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
        this.notify();
      }
    }
  }

  @Override
  public void waiting() {

  }

  private static class RSGroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(RSGroupStartupWorker.class);

    private volatile boolean isOnline = false;
    private MasterServices masterServices;
    private RSGroupInfoManagerImpl groupInfoManager;
    private ClusterConnection conn;

    public RSGroupStartupWorker(RSGroupInfoManagerImpl groupInfoManager,
                                MasterServices masterServices,
                                ClusterConnection conn) {
      this.masterServices = masterServices;
      this.groupInfoManager = groupInfoManager;
      this.conn = conn;
      setName(RSGroupStartupWorker.class.getName()+"-"+masterServices.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      if(waitForGroupTableOnline()) {
        LOG.info("GroupBasedLoadBalancer is now online");
      }
    }

    public boolean waitForGroupTableOnline() {
      final List<HRegionInfo> foundRegions = new LinkedList<HRegionInfo>();
      final List<HRegionInfo> assignedRegions = new LinkedList<HRegionInfo>();
      final AtomicBoolean found = new AtomicBoolean(false);
      final TableStateManager tsm =
          masterServices.getAssignmentManager().getTableStateManager();
      boolean createSent = false;
      while (!found.get() && isMasterRunning()) {
        foundRegions.clear();
        assignedRegions.clear();
        found.set(true);
        try {
          boolean rootMetaFound =
              masterServices.getMetaTableLocator().verifyMetaRegionLocation(
                  conn,
                  masterServices.getZooKeeper(),
                  1);
          final AtomicBoolean nsFound = new AtomicBoolean(false);
          if (rootMetaFound) {

            MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
              @Override
              public boolean visit(Result row) throws IOException {

                HRegionInfo info = MetaTableAccessor.getHRegionInfo(row);
                if (info != null) {
                  Cell serverCell =
                      row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                          HConstants.SERVER_QUALIFIER);
                  if (RSGROUP_TABLE_NAME.equals(info.getTable()) && serverCell != null) {
                    ServerName sn =
                        ServerName.parseVersionedServerName(CellUtil.cloneValue(serverCell));
                    if (sn == null) {
                      found.set(false);
                    } else if (tsm.isTableState(RSGROUP_TABLE_NAME,
                        ZooKeeperProtos.Table.State.ENABLED)) {
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
                        ZooKeeperProtos.Table.State.ENABLED)) {
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
            MetaTableAccessor.fullScan(conn, visitor);
            // if no regions in meta then we have to create the table
            if (foundRegions.size() < 1 && rootMetaFound && !createSent && nsFound.get()) {
              groupInfoManager.createGroupTable(masterServices);
              createSent = true;
            }
            LOG.info("Group table: " + RSGROUP_TABLE_NAME + " isOnline: " + found.get()
                + ", regionCount: " + foundRegions.size() + ", assignCount: "
                + assignedRegions.size() + ", rootMetaFound: "+rootMetaFound);
            found.set(found.get() && assignedRegions.size() == foundRegions.size()
                && foundRegions.size() > 0);
          } else {
            LOG.info("Waiting for catalog tables to come online");
            found.set(false);
          }
          if (found.get()) {
            LOG.debug("With group table online, refreshing cached information.");
            groupInfoManager.refresh(true);
            isOnline = true;
            //flush any inconsistencies between ZK and HTable
            groupInfoManager.flushConfig(groupInfoManager.rsGroupMap);
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

    public boolean isOnline() {
      return isOnline;
    }

    private boolean isMasterRunning() {
      return !masterServices.isAborted() && !masterServices.isStopped();
    }
  }

  private void createGroupTable(MasterServices masterServices) throws IOException {
    HRegionInfo[] newRegions =
        ModifyRegionUtils.createHRegionInfos(RSGROUP_TABLE_DESC, null);
    ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
    masterServices.getMasterProcedureExecutor().submitProcedure(
        new CreateTableProcedure(
            masterServices.getMasterProcedureExecutor().getEnvironment(),
            RSGROUP_TABLE_DESC,
            newRegions,
            latch));
    latch.await();
    // wait for region to be online
    int tries = 600;
    while(masterServices.getAssignmentManager().getRegionStates()
        .getRegionServerOfRegion(newRegions[0]) == null && tries > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if(tries <= 0) {
      throw new IOException("Failed to create group table.");
    }
  }

  private void multiMutate(List<Mutation> mutations)
      throws IOException {
    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        mrmBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        mrmBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException("multiMutate doesn't support "
          + mutation.getClass().getName());
      }
    }
    MutateRowsRequest mrm = mrmBuilder.build();
    // Be robust against movement of the rsgroup table
    // TODO: Why is this necessary sometimes? Should we be using our own connection?
    conn.clearRegionCache(RSGROUP_TABLE_NAME);
    try (Table rsGroupTable = conn.getTable(RSGROUP_TABLE_NAME)) {
      CoprocessorRpcChannel channel = rsGroupTable.coprocessorService(ROW_KEY);
      MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
          MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
      try {
        service.mutateRows(null, mrm);
      } catch (ServiceException ex) {
        ProtobufUtil.toIOException(ex);
      }
    }
  }

  private void checkGroupName(String groupName) throws ConstraintException {
    if(!groupName.matches("[a-zA-Z0-9_]+")) {
      throw new ConstraintException("Group name should only contain alphanumeric characters");
    }
  }

  @Override
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String srcGroup,
      String dstGroup) throws IOException {
    //get server's group
    RSGroupInfo srcGroupInfo = getRSGroup(srcGroup);
    RSGroupInfo dstGroupInfo = getRSGroup(dstGroup);

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
      } else {
        LOG.warn("Server " + el + " does not belong to any rsgroup.");
      }
    }
    if (rsGroupInfos.size() > 0) {
      Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
      newGroupMap.putAll(rsGroupInfos);
      flushConfig(newGroupMap);
    }
  }
}
