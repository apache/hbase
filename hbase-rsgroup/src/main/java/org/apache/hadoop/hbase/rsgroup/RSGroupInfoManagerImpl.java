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
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.ClusterSchema;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.util.Shell;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an implementation of {@link RSGroupInfoManager} which makes use of an HBase table as the
 * persistence store for the group information. It also makes use of zookeeper to store group
 * information needed for bootstrapping during offline mode.
 * <h2>Concurrency</h2> RSGroup state is kept locally in Maps. There is a rsgroup name to cached
 * RSGroupInfo Map at {@link #rsGroupMap} and a Map of tables to the name of the rsgroup they belong
 * too (in {@link #tableMap}). These Maps are persisted to the hbase:rsgroup table (and cached in
 * zk) on each modification.
 * <p>
 * Mutations on state are synchronized but reads can continue without having to wait on an instance
 * monitor, mutations do wholesale replace of the Maps on update -- Copy-On-Write; the local Maps of
 * state are read-only, just-in-case (see flushConfig).
 * <p>
 * Reads must not block else there is a danger we'll deadlock.
 * <p>
 * Clients of this class, the {@link RSGroupAdminEndpoint} for example, want to query and then act
 * on the results of the query modifying cache in zookeeper without another thread making
 * intermediate modifications. These clients synchronize on the 'this' instance so no other has
 * access concurrently. Reads must be able to continue concurrently.
 */
@InterfaceAudience.Private
final class RSGroupInfoManagerImpl implements RSGroupInfoManager {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupInfoManagerImpl.class);

  /** Table descriptor for <code>hbase:rsgroup</code> catalog table */
  private static final TableDescriptor RSGROUP_TABLE_DESC;
  static {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(RSGROUP_TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(META_FAMILY_BYTES))
      .setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName());
    try {
      builder.setCoprocessor(
        CoprocessorDescriptorBuilder.newBuilder(MultiRowMutationEndpoint.class.getName())
          .setPriority(Coprocessor.PRIORITY_SYSTEM).build());
    } catch (IOException ex) {
      throw new Error(ex);
    }
    RSGROUP_TABLE_DESC = builder.build();
  }

  // There two Maps are immutable and wholesale replaced on each modification
  // so are safe to access concurrently. See class comment.
  private volatile Map<String, RSGroupInfo> rsGroupMap = Collections.emptyMap();
  private volatile Map<TableName, String> tableMap = Collections.emptyMap();

  private final MasterServices masterServices;
  private final Connection conn;
  private final ZKWatcher watcher;
  private final RSGroupStartupWorker rsGroupStartupWorker;
  // contains list of groups that were last flushed to persistent store
  private Set<String> prevRSGroups = new HashSet<>();
  private final ServerEventsListenerThread serverEventsListenerThread =
    new ServerEventsListenerThread();

  /** Get rsgroup table mapping script */
  RSGroupMappingScript script;

  // Package visibility for testing
  static class RSGroupMappingScript {

    static final String RS_GROUP_MAPPING_SCRIPT = "hbase.rsgroup.table.mapping.script";
    static final String RS_GROUP_MAPPING_SCRIPT_TIMEOUT =
      "hbase.rsgroup.table.mapping.script.timeout";

    private Shell.ShellCommandExecutor rsgroupMappingScript;

    RSGroupMappingScript(Configuration conf) {
      String script = conf.get(RS_GROUP_MAPPING_SCRIPT);
      if (script == null || script.isEmpty()) {
        return;
      }

      rsgroupMappingScript = new Shell.ShellCommandExecutor(
        new String[] { script, "", "" }, null, null,
        conf.getLong(RS_GROUP_MAPPING_SCRIPT_TIMEOUT, 5000) // 5 seconds
      );
    }

    String getRSGroup(String namespace, String tablename) {
      if (rsgroupMappingScript == null) {
        return null;
      }
      String[] exec = rsgroupMappingScript.getExecString();
      exec[1] = namespace;
      exec[2] = tablename;
      try {
        rsgroupMappingScript.execute();
      } catch (IOException e) {
        LOG.error("Failed to get RSGroup from script for table {}:{}", namespace, tablename, e);
        return null;
      }
      return rsgroupMappingScript.getOutput().trim();
    }
  }

  private RSGroupInfoManagerImpl(MasterServices masterServices) throws IOException {
    this.masterServices = masterServices;
    this.watcher = masterServices.getZooKeeper();
    this.conn = masterServices.getConnection();
    this.rsGroupStartupWorker = new RSGroupStartupWorker();
    script = new RSGroupMappingScript(masterServices.getConfiguration());
  }

  private synchronized void init() throws IOException {
    refresh();
    serverEventsListenerThread.start();
    masterServices.getServerManager().registerListener(serverEventsListenerThread);
  }

  static RSGroupInfoManager getInstance(MasterServices master) throws IOException {
    RSGroupInfoManagerImpl instance = new RSGroupInfoManagerImpl(master);
    instance.init();
    return instance;
  }

  public void start() {
    // create system table of rsgroup
    rsGroupStartupWorker.start();
  }

  @Override
  public synchronized void addRSGroup(RSGroupInfo rsGroupInfo) throws IOException {
    checkGroupName(rsGroupInfo.getName());
    if (rsGroupMap.get(rsGroupInfo.getName()) != null ||
      rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: " + rsGroupInfo.getName());
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

  /**
   * @param master the master to get online servers for
   * @return Set of online Servers named for their hostname and port (not ServerName).
   */
  private static Set<Address> getOnlineServers(final MasterServices master) {
    Set<Address> onlineServers = new HashSet<Address>();
    if (master == null) {
      return onlineServers;
    }

    for (ServerName server : master.getServerManager().getOnlineServers().keySet()) {
      onlineServers.add(server.getAddress());
    }
    return onlineServers;
  }

  @Override
  public synchronized Set<Address> moveServers(Set<Address> servers, String srcGroup,
      String dstGroup) throws IOException {
    RSGroupInfo src = getRSGroupInfo(srcGroup);
    RSGroupInfo dst = getRSGroupInfo(dstGroup);
    Set<Address> movedServers = new HashSet<>();
    // If destination is 'default' rsgroup, only add servers that are online. If not online, drop
    // it. If not 'default' group, add server to 'dst' rsgroup EVEN IF IT IS NOT online (could be a
    // rsgroup of dead servers that are to come back later).
    Set<Address> onlineServers =
      dst.getName().equals(RSGroupInfo.DEFAULT_GROUP) ? getOnlineServers(this.masterServices)
        : null;
    for (Address el : servers) {
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
      movedServers.add(el);
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);
    flushConfig(newGroupMap);
    return movedServers;
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException {
    for (RSGroupInfo info : rsGroupMap.values()) {
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
    // Check if rsGroup contains the destination rsgroup
    if (groupName != null && !rsGroupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group " + groupName + " does not exist");
    }

    // Make a copy of rsGroupMap to update
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);

    // Remove tables from their original rsgroups
    // and update the copy of rsGroupMap
    for (TableName tableName : tableNames) {
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

  @Override
  public synchronized void removeRSGroup(String groupName) throws IOException {
    if (!rsGroupMap.containsKey(groupName) || groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException(
        "Group " + groupName + " does not exist or is a reserved " + "group");
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
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
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String srcGroup,
      String dstGroup) throws IOException {
    // get server's group
    RSGroupInfo srcGroupInfo = getRSGroupInfo(srcGroup);
    RSGroupInfo dstGroupInfo = getRSGroupInfo(dstGroup);

    // move servers
    for (Address el : servers) {
      srcGroupInfo.removeServer(el);
      dstGroupInfo.addServer(el);
    }
    // move tables
    for (TableName tableName : tables) {
      srcGroupInfo.removeTable(tableName);
      dstGroupInfo.addTable(tableName);
    }

    // flush changed groupinfo
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(srcGroupInfo.getName(), srcGroupInfo);
    newGroupMap.put(dstGroupInfo.getName(), dstGroupInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public synchronized void removeServers(Set<Address> servers) throws IOException {
    Map<String, RSGroupInfo> rsGroupInfos = new HashMap<String, RSGroupInfo>();
    for (Address el : servers) {
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

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    checkGroupName(oldName);
    checkGroupName(newName);
    if (oldName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new ConstraintException("Can't rename default rsgroup");
    }
    RSGroupInfo oldGroup = getRSGroup(oldName);
    if (oldGroup == null) {
      throw new ConstraintException("RSGroup " + oldName + " does not exist");
    }
    if (rsGroupMap.containsKey(newName)) {
      throw new ConstraintException("Group already exists: " + newName);
    }

    Map<String,RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(oldName);
    RSGroupInfo newGroup = new RSGroupInfo(newName,
      (SortedSet<Address>) oldGroup.getServers(), oldGroup.getTables());
    newGroupMap.put(newName, newGroup);
    flushConfig(newGroupMap);
  }

  /**
   * Will try to get the rsgroup from {@code tableMap} first
   * then try to get the rsgroup from {@code script}
   * try to get the rsgroup from the {@link NamespaceDescriptor} lastly.
   * If still not present, return default group.
   */
  @Override
  public RSGroupInfo determineRSGroupInfoForTable(TableName tableName)
    throws IOException {
    RSGroupInfo groupFromOldRSGroupInfo = getRSGroup(getRSGroupOfTable(tableName));
    if (groupFromOldRSGroupInfo != null) {
      return groupFromOldRSGroupInfo;
    }
    // RSGroup information determined by administrator.
    RSGroupInfo groupDeterminedByAdmin = getRSGroup(
      script.getRSGroup(tableName.getNamespaceAsString(), tableName.getQualifierAsString()));
    if (groupDeterminedByAdmin != null) {
      return groupDeterminedByAdmin;
    }
    // Finally, we will try to fall back to namespace as rsgroup if exists
    ClusterSchema clusterSchema = masterServices.getClusterSchema();
    if (clusterSchema == null) {
      if (TableName.isMetaTableName(tableName)) {
        LOG.info("Can not get the namespace rs group config for meta table, since the" +
          " meta table is not online yet, will use default group to assign meta first");
      } else {
        LOG.warn("ClusterSchema is null, can only use default rsgroup, should not happen?");
      }
    } else {
      NamespaceDescriptor nd = clusterSchema.getNamespace(tableName.getNamespaceAsString());
      RSGroupInfo groupNameOfNs =
        getRSGroup(nd.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP));
      if (groupNameOfNs != null) {
        return groupNameOfNs;
      }
    }
    return getRSGroup(RSGroupInfo.DEFAULT_GROUP);
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    if (RSGroupInfo.DEFAULT_GROUP.equals(groupName)) {
      // We do not persist anything of default group, therefore, it is not supported to update
      // default group's configuration which lost once master down.
      throw new ConstraintException("configuration of " + RSGroupInfo.DEFAULT_GROUP
          + " can't be stored persistently");
    }
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    new HashSet<>(rsGroupInfo.getConfiguration().keySet())
        .forEach(rsGroupInfo::removeConfiguration);
    configuration.forEach(rsGroupInfo::setConfiguration);
    flushConfig();
  }

  List<RSGroupInfo> retrieveGroupListFromGroupTable() throws IOException {
    List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
    try (Table table = conn.getTable(RSGROUP_TABLE_NAME);
        ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo
          .parseFrom(result.getValue(META_FAMILY_BYTES, META_QUALIFIER_BYTES));
        rsGroupInfoList.add(RSGroupProtobufUtil.toGroupInfo(proto));
      }
    }
    return rsGroupInfoList;
  }

  List<RSGroupInfo> retrieveGroupListFromZookeeper() throws IOException {
    String groupBasePath = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, rsGroupZNode);
    List<RSGroupInfo> RSGroupInfoList = Lists.newArrayList();
    // Overwrite any info stored by table, this takes precedence
    try {
      if (ZKUtil.checkExists(watcher, groupBasePath) != -1) {
        List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(watcher, groupBasePath);
        if (children == null) {
          return RSGroupInfoList;
        }
        for (String znode : children) {
          byte[] data = ZKUtil.getData(watcher, ZNodePaths.joinZNode(groupBasePath, znode));
          if (data != null && data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis =
              new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
            RSGroupInfoList
              .add(RSGroupProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
          }
        }
        LOG.debug("Read ZK GroupInfo count:" + RSGroupInfoList.size());
      }
    } catch (KeeperException | DeserializationException | InterruptedException e) {
      throw new IOException("Failed to read rsGroupZNode", e);
    }
    return RSGroupInfoList;
  }

  @Override
  public void refresh() throws IOException {
    refresh(false);
  }

  /**
   * Read rsgroup info from the source of truth, the hbase:rsgroup table. Update zk cache. Called on
   * startup of the manager.
   */
  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<RSGroupInfo> groupList = new LinkedList<>();

    // Overwrite anything read from zk, group table is source of truth
    // if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refreshing in Online mode.");
      groupList.addAll(retrieveGroupListFromGroupTable());
    } else {
      LOG.debug("Refreshing in Offline mode.");
      groupList.addAll(retrieveGroupListFromZookeeper());
    }

    // refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<>();
    for (String entry : masterServices.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }
    for (RSGroupInfo group : groupList) {
      if (!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }

    // This is added to the last of the list so it overwrites the 'default' rsgroup loaded
    // from region group table or zk
    groupList.add(
        new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, getDefaultServers(groupList), orphanTables));

    // populate the data
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap();
    HashMap<TableName, String> newTableMap = Maps.newHashMap();
    for (RSGroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
      for (TableName table : group.getTables()) {
        newTableMap.put(table, group.getName());
      }
    }
    resetRSGroupAndTableMaps(newGroupMap, newTableMap);
    updateCacheOfRSGroups(rsGroupMap.keySet());
  }

  private synchronized Map<TableName, String> flushConfigTable(Map<String, RSGroupInfo> groupMap)
      throws IOException {
    Map<TableName, String> newTableMap = Maps.newHashMap();
    List<Mutation> mutations = Lists.newArrayList();

    // populate deletes
    for (String groupName : prevRSGroups) {
      if (!groupMap.containsKey(groupName)) {
        Delete d = new Delete(Bytes.toBytes(groupName));
        mutations.add(d);
      }
    }

    // populate puts
    for (RSGroupInfo RSGroupInfo : groupMap.values()) {
      RSGroupProtos.RSGroupInfo proto = RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo);
      Put p = new Put(Bytes.toBytes(RSGroupInfo.getName()));
      p.addColumn(META_FAMILY_BYTES, META_QUALIFIER_BYTES, proto.toByteArray());
      mutations.add(p);
      for (TableName entry : RSGroupInfo.getTables()) {
        newTableMap.put(entry, RSGroupInfo.getName());
      }
    }

    if (mutations.size() > 0) {
      multiMutate(mutations);
    }
    return newTableMap;
  }

  private synchronized void flushConfig() throws IOException {
    flushConfig(this.rsGroupMap);
  }

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap) throws IOException {
    Map<TableName, String> newTableMap;

    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for servers in default group
    if (!isOnline()) {
      if (newGroupMap == this.rsGroupMap) {
        // When newGroupMap is this.rsGroupMap itself,
        // do not need to check default group and other groups as followed
        return;
      }

      Map<String, RSGroupInfo> oldGroupMap = Maps.newHashMap(rsGroupMap);
      RSGroupInfo oldDefaultGroup = oldGroupMap.remove(RSGroupInfo.DEFAULT_GROUP);
      RSGroupInfo newDefaultGroup = newGroupMap.remove(RSGroupInfo.DEFAULT_GROUP);
      if (!oldGroupMap.equals(newGroupMap) /* compare both tables and servers in other groups */ ||
          !oldDefaultGroup.getTables().equals(newDefaultGroup.getTables())
          /* compare tables in default group */) {
        throw new IOException("Only servers in default group can be updated during offline mode");
      }

      // Restore newGroupMap by putting its default group back
      newGroupMap.put(RSGroupInfo.DEFAULT_GROUP, newDefaultGroup);

      // Refresh rsGroupMap
      // according to the inputted newGroupMap (an updated copy of rsGroupMap)
      rsGroupMap = newGroupMap;

      // Do not need to update tableMap
      // because only the update on servers in default group is allowed above,
      // or IOException will be thrown
      return;
    }

    /* For online mode, persist to Zookeeper */
    newTableMap = flushConfigTable(newGroupMap);

    // Make changes visible after having been persisted to the source of truth
    resetRSGroupAndTableMaps(newGroupMap, newTableMap);

    try {
      String groupBasePath = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, rsGroupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufMagic.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<>(newGroupMap.size());
      for (String groupName : prevRSGroups) {
        if (!newGroupMap.containsKey(groupName)) {
          String znode = ZNodePaths.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }

      for (RSGroupInfo RSGroupInfo : newGroupMap.values()) {
        String znode = ZNodePaths.joinZNode(groupBasePath, RSGroupInfo.getName());
        RSGroupProtos.RSGroupInfo proto = RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo);
        LOG.debug("Updating znode: " + znode);
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
      throw new IOException("Failed to write to rsGroupZNode", e);
    }
    updateCacheOfRSGroups(newGroupMap.keySet());
  }

  /**
   * Make changes visible. Caller must be synchronized on 'this'.
   */
  private void resetRSGroupAndTableMaps(Map<String, RSGroupInfo> newRSGroupMap,
      Map<TableName, String> newTableMap) {
    // Make maps Immutable.
    this.rsGroupMap = Collections.unmodifiableMap(newRSGroupMap);
    this.tableMap = Collections.unmodifiableMap(newTableMap);
  }

  /**
   * Update cache of rsgroups. Caller must be synchronized on 'this'.
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
      for (String el : ZKUtil.listChildrenNoWatch(watcher, watcher.getZNodePaths().rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
    return servers;
  }

  // Called by ServerEventsListenerThread. Presume it has lock on this manager when it runs.
  private SortedSet<Address> getDefaultServers() throws IOException {
    return getDefaultServers(listRSGroups());
  }

  // Called by ServerEventsListenerThread. Presume it has lock on this manager when it runs.
  private SortedSet<Address> getDefaultServers(List<RSGroupInfo> rsGroupInfoList)
      throws IOException {
    // Build a list of servers in other groups than default group, from rsGroupMap
    Set<Address> serversInOtherGroup = new HashSet<>();
    for (RSGroupInfo group : rsGroupInfoList) {
      if (!RSGroupInfo.DEFAULT_GROUP.equals(group.getName())) { // not default group
        serversInOtherGroup.addAll(group.getServers());
      }
    }

    // Get all online servers from Zookeeper and find out servers in default group
    SortedSet<Address> defaultServers = Sets.newTreeSet();
    for (ServerName serverName : getOnlineRS()) {
      Address server = Address.fromParts(serverName.getHostname(), serverName.getPort());
      if (!serversInOtherGroup.contains(server)) { // not in other groups
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

  /**
   * Calls {@link RSGroupInfoManagerImpl#updateDefaultServers(SortedSet)} to update list of known
   * servers. Notifications about server changes are received by registering {@link ServerListener}.
   * As a listener, we need to return immediately, so the real work of updating the servers is done
   * asynchronously in this thread.
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
      while (isMasterRunning(masterServices)) {
        try {
          LOG.info("Updating default servers.");
          SortedSet<Address> servers = RSGroupInfoManagerImpl.this.getDefaultServers();
          if (!servers.equals(prevDefaultServers)) {
            RSGroupInfoManagerImpl.this.updateDefaultServers(servers);
            prevDefaultServers = servers;
            LOG.info("Updated with servers: " + servers.size());
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

  private class RSGroupStartupWorker extends Thread {
    private final Logger LOG = LoggerFactory.getLogger(RSGroupStartupWorker.class);
    private volatile boolean online = false;

    RSGroupStartupWorker() {
      super(RSGroupStartupWorker.class.getName() + "-" + masterServices.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      if (waitForGroupTableOnline()) {
        LOG.info("GroupBasedLoadBalancer is now online");
      } else {
        LOG.warn("Quit without making region group table online");
      }
    }

    private boolean waitForGroupTableOnline() {
      while (isMasterRunning(masterServices)) {
        try {
          TableStateManager tsm = masterServices.getTableStateManager();
          if (!tsm.isTablePresent(RSGROUP_TABLE_NAME)) {
            createRSGroupTable();
          }
          // try reading from the table
          try (Table table = conn.getTable(RSGROUP_TABLE_NAME)) {
            table.get(new Get(ROW_KEY));
          }
          LOG.info(
            "RSGroup table=" + RSGROUP_TABLE_NAME + " is online, refreshing cached information");
          RSGroupInfoManagerImpl.this.refresh(true);
          online = true;
          // flush any inconsistencies between ZK and HTable
          RSGroupInfoManagerImpl.this.flushConfig();
          return true;
        } catch (Exception e) {
          LOG.warn("Failed to perform check", e);
          // 100ms is short so let's just ignore the interrupt
          Threads.sleepWithoutInterrupt(100);
        }
      }
      return false;
    }

    private void createRSGroupTable() throws IOException {
      OptionalLong optProcId = masterServices.getProcedures().stream()
        .filter(p -> p instanceof CreateTableProcedure).map(p -> (CreateTableProcedure) p)
        .filter(p -> p.getTableName().equals(RSGROUP_TABLE_NAME)).mapToLong(Procedure::getProcId)
        .findFirst();
      long procId;
      if (optProcId.isPresent()) {
        procId = optProcId.getAsLong();
      } else {
        procId = masterServices.createSystemTable(RSGROUP_TABLE_DESC);
      }
      // wait for region to be online
      int tries = 600;
      while (!(masterServices.getMasterProcedureExecutor().isFinished(procId)) &&
        masterServices.getMasterProcedureExecutor().isRunning() && tries > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new IOException("Wait interrupted ", e);
        }
        tries--;
      }
      if (tries <= 0) {
        throw new IOException("Failed to create group table in a given time.");
      } else {
        Procedure<?> result = masterServices.getMasterProcedureExecutor().getResult(procId);
        if (result != null && result.isFailed()) {
          throw new IOException(
            "Failed to create group table. " + MasterProcedureUtil.unwrapRemoteIOException(result));
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
    try (Table table = conn.getTable(RSGROUP_TABLE_NAME)) {
      CoprocessorRpcChannel channel = table.coprocessorService(ROW_KEY);
      MultiRowMutationProtos.MutateRowsRequest.Builder mmrBuilder =
        MultiRowMutationProtos.MutateRowsRequest.newBuilder();
      for (Mutation mutation : mutations) {
        if (mutation instanceof Put) {
          mmrBuilder.addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
            org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT,
            mutation));
        } else if (mutation instanceof Delete) {
          mmrBuilder.addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
            org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.DELETE,
            mutation));
        } else {
          throw new DoNotRetryIOException(
            "multiMutate doesn't support " + mutation.getClass().getName());
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
  }

  private void checkGroupName(String groupName) throws ConstraintException {
    if (!groupName.matches("[a-zA-Z0-9_]+")) {
      throw new ConstraintException("RSGroup name should only contain alphanumeric characters");
    }
  }
}
