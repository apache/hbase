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
import com.google.common.net.HostAndPort;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.MetaTableAccessor.DefaultVisitorBase;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This is an implementation of {@link RSGroupInfoManager}. Which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 */
@InterfaceAudience.Private
public class RSGroupInfoManagerImpl implements RSGroupInfoManager, ServerListener {
  private static final Log LOG = LogFactory.getLog(RSGroupInfoManagerImpl.class);

  /** Table descriptor for <code>hbase:rsgroup</code> catalog table */
  private final static HTableDescriptor RSGROUP_TABLE_DESC;
  static {
    RSGROUP_TABLE_DESC = new HTableDescriptor(RSGROUP_TABLE_NAME_BYTES);
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
  private Table rsGroupTable;
  private ClusterConnection conn;
  private ZooKeeperWatcher watcher;
  private RSGroupStartupWorker rsGroupStartupWorker;
  // contains list of groups that were last flushed to persistent store
  private volatile Set<String> prevRSGroups;
  private RSGroupSerDe rsGroupSerDe;
  private DefaultServerUpdater defaultServerUpdater;
  private boolean isInit = false;


  public RSGroupInfoManagerImpl(MasterServices master) throws IOException {
    this.rsGroupMap = Collections.EMPTY_MAP;
    this.tableMap = Collections.EMPTY_MAP;
    rsGroupSerDe = new RSGroupSerDe();
    this.master = master;
    this.watcher = master.getZooKeeper();
    this.conn = master.getClusterConnection();
    prevRSGroups = new HashSet<String>();
  }

  public void init() throws IOException{
    rsGroupStartupWorker = new RSGroupStartupWorker(this, master, conn);
    refresh();
    rsGroupStartupWorker.start();
    defaultServerUpdater = new DefaultServerUpdater(this);
    master.getServerManager().registerListener(this);
    defaultServerUpdater.start();
    isInit = true;
  }

  boolean isInit() {
    return isInit;
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
        rsGroupInfo.getName().equals(rsGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+ rsGroupInfo.getName());
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(rsGroupInfo.getName(), rsGroupInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public synchronized boolean moveServers(Set<HostAndPort> hostPorts, String srcGroup,
                                          String dstGroup) throws IOException {
    if (!rsGroupMap.containsKey(srcGroup)) {
      throw new DoNotRetryIOException("Group "+srcGroup+" does not exist");
    }
    if (!rsGroupMap.containsKey(dstGroup)) {
      throw new DoNotRetryIOException("Group "+dstGroup+" does not exist");
    }

    RSGroupInfo src = new RSGroupInfo(getRSGroup(srcGroup));
    RSGroupInfo dst = new RSGroupInfo(getRSGroup(dstGroup));
    boolean foundOne = false;
    for(HostAndPort el: hostPorts) {
      foundOne = src.removeServer(el) || foundOne;
      dst.addServer(el);
    }

    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);

    flushConfig(newGroupMap);
    return foundOne;
  }

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of GroupInfo.
   */
  @Override
  public RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException {
    for (RSGroupInfo info : rsGroupMap.values()) {
      if (info.containsServer(hostPort)){
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
    RSGroupInfo RSGroupInfo = rsGroupMap.get(groupName);
    return RSGroupInfo;
  }



  @Override
  public String getRSGroupOfTable(TableName tableName) throws IOException {
    return tableMap.get(tableName);
  }

  @Override
  public synchronized void moveTables(
      Set<TableName> tableNames, String groupName) throws IOException {
    if (groupName != null && !rsGroupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a special group");
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
      if (rsGroupTable == null) {
        rsGroupTable = conn.getTable(RSGROUP_TABLE_NAME);
      }
      groupList.addAll(rsGroupSerDe.retrieveGroupList(rsGroupTable));
    } else {
      LOG.debug("Refershing in Offline mode.");
      String groupBasePath = ZKUtil.joinZNode(watcher.znodePaths.baseZNode, rsGroupZNode);
      groupList.addAll(rsGroupSerDe.retrieveGroupList(watcher, groupBasePath));
    }

    // refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<TableName>();
    for(String entry: master.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }

    List<TableName> specialTables;
    if(!master.isInitialized()) {
      specialTables = new ArrayList<TableName>();
      specialTables.add(AccessControlLists.ACL_TABLE_NAME);
      specialTables.add(TableName.META_TABLE_NAME);
      specialTables.add(TableName.NAMESPACE_TABLE_NAME);
      specialTables.add(RSGROUP_TABLE_NAME);
    } else {
      specialTables =
          master.listTableNamesByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    }

    for(TableName table : specialTables) {
      orphanTables.add(table);
    }
    for(RSGroupInfo group: groupList) {
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
      RSGroupProtos.RSGroupInfo proto = RSGroupSerDe.toProtoGroupInfo(RSGroupInfo);
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
      String groupBasePath = ZKUtil.joinZNode(watcher.znodePaths.baseZNode, rsGroupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufMagic.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<ZKUtil.ZKUtilOp>(newGroupMap.size());
      for(String groupName : prevRSGroups) {
        if(!newGroupMap.containsKey(groupName)) {
          String znode = ZKUtil.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }


      for(RSGroupInfo RSGroupInfo : newGroupMap.values()) {
        String znode = ZKUtil.joinZNode(groupBasePath, RSGroupInfo.getName());
        RSGroupProtos.RSGroupInfo proto = RSGroupSerDe.toProtoGroupInfo(RSGroupInfo);
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
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.znodePaths.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
  }

  private List<HostAndPort> getDefaultServers() throws IOException {
    List<HostAndPort> defaultServers = new LinkedList<HostAndPort>();
    for(ServerName server : getOnlineRS()) {
      HostAndPort hostPort = HostAndPort.fromParts(server.getHostname(), server.getPort());
      boolean found = false;
      for(RSGroupInfo RSGroupInfo : rsGroupMap.values()) {
        if(!RSGroupInfo.DEFAULT_GROUP.equals(RSGroupInfo.getName()) &&
            RSGroupInfo.containsServer(hostPort)) {
          found = true;
          break;
        }
      }
      if(!found) {
        defaultServers.add(hostPort);
      }
    }
    return defaultServers;
  }

  private synchronized void updateDefaultServers(
      Set<HostAndPort> hostPort) throws IOException {
    RSGroupInfo info = rsGroupMap.get(RSGroupInfo.DEFAULT_GROUP);
    RSGroupInfo newInfo = new RSGroupInfo(info.getName(), hostPort, info.getTables());
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
    private boolean hasChanged = false;

    public DefaultServerUpdater(RSGroupInfoManagerImpl mgr) {
      this.mgr = mgr;
    }

    @Override
    public void run() {
      List<HostAndPort> prevDefaultServers = new LinkedList<HostAndPort>();
      while(!mgr.master.isAborted() || !mgr.master.isStopped()) {
        try {
          LOG.info("Updating default servers.");
          List<HostAndPort> servers = mgr.getDefaultServers();
          Collections.sort(servers, new Comparator<HostAndPort>() {
            @Override
            public int compare(HostAndPort o1, HostAndPort o2) {
              int diff = o1.getHostText().compareTo(o2.getHostText());
              if (diff != 0) {
                return diff;
              }
              return o1.getPort() - o2.getPort();
            }
          });
          if(!servers.equals(prevDefaultServers)) {
            mgr.updateDefaultServers(Sets.<HostAndPort>newHashSet(servers));
            prevDefaultServers = servers;
            LOG.info("Updated with servers: "+servers.size());
          }
          try {
            synchronized (this) {
              if(!hasChanged) {
                wait();
              }
              hasChanged = false;
            }
          } catch (InterruptedException e) {
          }
        } catch (IOException e) {
          LOG.warn("Failed to update default servers", e);
        }
      }
    }

    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
        this.notify();
      }
    }
  }


  private static class RSGroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(RSGroupStartupWorker.class);

    private Configuration conf;
    private volatile boolean isOnline = false;
    private MasterServices masterServices;
    private RSGroupInfoManagerImpl groupInfoManager;
    private ClusterConnection conn;

    public RSGroupStartupWorker(RSGroupInfoManagerImpl groupInfoManager,
                                MasterServices masterServices,
                                ClusterConnection conn) {
      this.conf = masterServices.getConfiguration();
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
      final TableStateManager tsm = masterServices.getTableStateManager();
      boolean createSent = false;
      while (!found.get() && isMasterRunning()) {
        foundRegions.clear();
        assignedRegions.clear();
        found.set(true);
        try {
          final Table nsTable = conn.getTable(TableName.NAMESPACE_TABLE_NAME);
          final Table groupTable = conn.getTable(RSGROUP_TABLE_NAME);
          boolean rootMetaFound =
              masterServices.getMetaTableLocator().verifyMetaRegionLocation(
                  conn,
                  masterServices.getZooKeeper(),
                  1);
          final AtomicBoolean nsFound = new AtomicBoolean(false);
          if (rootMetaFound) {

            MetaTableAccessor.Visitor visitor = new DefaultVisitorBase() {
              @Override
              public boolean visitInternal(Result row) throws IOException {

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
                    if (tsm.isTableState(TableName.NAMESPACE_TABLE_NAME,
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
    Long procId = masterServices.createSystemTable(RSGROUP_TABLE_DESC);
    // wait for region to be online
    int tries = 600;
    while (!(masterServices.getMasterProcedureExecutor().isFinished(procId))
        && masterServices.getMasterProcedureExecutor().isRunning()
        && tries > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if(tries <= 0) {
      throw new IOException("Failed to create group table in a given time.");
    } else {
      ProcedureInfo result = masterServices.getMasterProcedureExecutor().getResult(procId);
      if (result != null && result.isFailed()) {
        throw new IOException("Failed to create group table. " + result.getExceptionFullMessage());
      }
    }
  }

  private void multiMutate(List<Mutation> mutations)
      throws IOException {
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
    if(!groupName.matches("[a-zA-Z0-9_]+")) {
      throw new ConstraintException("Group name should only contain alphanumeric characters");
    }
  }
}
