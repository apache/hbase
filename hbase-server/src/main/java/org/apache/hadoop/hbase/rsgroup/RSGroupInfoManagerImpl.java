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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This is an implementation of {@link RSGroupInfoManager} which makes use of an HBase table as the
 * persistence store for the group information. It also makes use of zookeeper to store group
 * information needed for bootstrapping during offline mode.
 * <h2>Concurrency</h2> RSGroup state is kept locally in Maps. There is a rsgroup name to cached
 * RSGroupInfo Map at {@link #rsGroupMap} and a Map of tables to the name of the rsgroup they belong
 * too (in {@link #tableMap}). These Maps are persisted to the hbase:rsgroup table (and cached in
 * zk) on each modification.
 * <p/>
 * Mutations on state are synchronized but reads can continue without having to wait on an instance
 * monitor, mutations do wholesale replace of the Maps on update -- Copy-On-Write; the local Maps of
 * state are read-only, just-in-case (see flushConfig).
 * <p/>
 * Reads must not block else there is a danger we'll deadlock.
 * <p/>
 * Clients of this class, the {@link RSGroupAdminEndpoint} for example, want to query and then act
 * on the results of the query modifying cache in zookeeper without another thread making
 * intermediate modifications. These clients synchronize on the 'this' instance so no other has
 * access concurrently. Reads must be able to continue concurrently.
 */
@InterfaceAudience.Private
final class RSGroupInfoManagerImpl implements RSGroupInfoManager {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupInfoManagerImpl.class);

  // Assigned before user tables
  @VisibleForTesting
  static final TableName RSGROUP_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");

  private static final String RS_GROUP_ZNODE = "rsgroup";

  @VisibleForTesting
  static final byte[] META_FAMILY_BYTES = Bytes.toBytes("m");

  @VisibleForTesting
  static final byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");

  @VisibleForTesting
  static final String MIGRATE_THREAD_NAME = "Migrate-RSGroup-Tables";

  private static final byte[] ROW_KEY = { 0 };

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
  private static final class RSGroupInfoHolder {
    final ImmutableMap<String, RSGroupInfo> groupName2Group;
    final ImmutableMap<TableName, RSGroupInfo> tableName2Group;

    RSGroupInfoHolder() {
      this(Collections.emptyMap());
    }

    RSGroupInfoHolder(Map<String, RSGroupInfo> rsGroupMap) {
      ImmutableMap.Builder<String, RSGroupInfo> group2Name2GroupBuilder = ImmutableMap.builder();
      ImmutableMap.Builder<TableName, RSGroupInfo> tableName2GroupBuilder = ImmutableMap.builder();
      rsGroupMap.forEach((groupName, rsGroupInfo) -> {
        group2Name2GroupBuilder.put(groupName, rsGroupInfo);
        if (!groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
          rsGroupInfo.getTables()
            .forEach(tableName -> tableName2GroupBuilder.put(tableName, rsGroupInfo));
        }
      });
      this.groupName2Group = group2Name2GroupBuilder.build();
      this.tableName2Group = tableName2GroupBuilder.build();
    }
  }

  private volatile RSGroupInfoHolder holder = new RSGroupInfoHolder();

  private final MasterServices masterServices;
  private final AsyncClusterConnection conn;
  private final ZKWatcher watcher;
  private final RSGroupStartupWorker rsGroupStartupWorker;
  // contains list of groups that were last flushed to persistent store
  private Set<String> prevRSGroups = new HashSet<>();
  private final ServerEventsListenerThread serverEventsListenerThread =
    new ServerEventsListenerThread();

  private RSGroupInfoManagerImpl(MasterServices masterServices) throws IOException {
    this.masterServices = masterServices;
    this.watcher = masterServices.getZooKeeper();
    this.conn = masterServices.getAsyncClusterConnection();
    this.rsGroupStartupWorker = new RSGroupStartupWorker();
  }


  private synchronized void init() throws IOException {
    refresh(false);
    serverEventsListenerThread.start();
    masterServices.getServerManager().registerListener(serverEventsListenerThread);
    migrate();
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
    Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
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
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(holder.groupName2Group);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);
    flushConfig(newGroupMap);
    return dst.getServers();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException {
    for (RSGroupInfo info : holder.groupName2Group.values()) {
      if (info.containsServer(serverHostPort)) {
        return info;
      }
    }
    return null;
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) {
    return holder.groupName2Group.get(groupName);
  }

  @Override
  public synchronized void removeRSGroup(String groupName) throws IOException {
    Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
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
    return Lists.newArrayList(holder.groupName2Group.values());
  }

  @Override
  public boolean isOnline() {
    return rsGroupStartupWorker.isOnline();
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
      Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(holder.groupName2Group);
      newGroupMap.putAll(rsGroupInfos);
      flushConfig(newGroupMap);
    }
  }

  private List<RSGroupInfo> retrieveGroupListFromGroupTable() throws IOException {
    List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
    AsyncTable<?> table = conn.getTable(RSGROUP_TABLE_NAME);
    try (ResultScanner scanner = table.getScanner(META_FAMILY_BYTES, META_QUALIFIER_BYTES)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo
            .parseFrom(result.getValue(META_FAMILY_BYTES, META_QUALIFIER_BYTES));
        rsGroupInfoList.add(ProtobufUtil.toGroupInfo(proto));
      }
    }
    return rsGroupInfoList;
  }

  private List<RSGroupInfo> retrieveGroupListFromZookeeper() throws IOException {
    String groupBasePath = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, RS_GROUP_ZNODE);
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
          if (data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis =
              new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
            RSGroupInfoList
              .add(ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
          }
        }
        LOG.debug("Read ZK GroupInfo count:" + RSGroupInfoList.size());
      }
    } catch (KeeperException | DeserializationException | InterruptedException e) {
      throw new IOException("Failed to read rsGroupZNode", e);
    }
    return RSGroupInfoList;
  }

  private void migrate(Collection<RSGroupInfo> groupList) {
    TableDescriptors tds = masterServices.getTableDescriptors();
    for (RSGroupInfo groupInfo : groupList) {
      if (groupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        continue;
      }
      SortedSet<TableName> failedTables = new TreeSet<>();
      for (TableName tableName : groupInfo.getTables()) {
        LOG.debug("Migrating {} in group {}", tableName, groupInfo.getName());
        TableDescriptor oldTd;
        try {
          oldTd = tds.get(tableName);
        } catch (IOException e) {
          LOG.warn("Failed to migrate {} in group {}", tableName, groupInfo.getName(), e);
          failedTables.add(tableName);
          continue;
        }
        if (oldTd == null) {
          continue;
        }
        if (oldTd.getRegionServerGroup().isPresent()) {
          // either we have already migrated it or that user has set the rs group using the new
          // code which will set the group directly on table descriptor, skip.
          LOG.debug("Skip migrating {} since it is already in group {}", tableName,
            oldTd.getRegionServerGroup().get());
          continue;
        }
        TableDescriptor newTd = TableDescriptorBuilder.newBuilder(oldTd)
          .setRegionServerGroup(groupInfo.getName()).build();
        // This is a bit tricky. Since we know that the region server group config in
        // TableDescriptor will only be used at master side, it is fine to just update the table
        // descriptor on file system and also the cache, without reopening all the regions. This
        // will be much faster than the normal modifyTable. And when upgrading, we will update
        // master first and then region server, so after all the region servers has been reopened,
        // the new TableDescriptor will be loaded.
        try {
          tds.add(newTd);
        } catch (IOException e) {
          LOG.warn("Failed to migrate {} in group {}", tableName, groupInfo.getName(), e);
          failedTables.add(tableName);
          continue;
        }
      }
      LOG.debug("Done migrating {}, failed tables {}", groupInfo.getName(), failedTables);
      synchronized (RSGroupInfoManagerImpl.this) {
        Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
        RSGroupInfo currentInfo = rsGroupMap.get(groupInfo.getName());
        if (currentInfo != null) {
          RSGroupInfo newInfo =
            new RSGroupInfo(currentInfo.getName(), currentInfo.getServers(), failedTables);
          Map<String, RSGroupInfo> newGroupMap = new HashMap<>(rsGroupMap);
          newGroupMap.put(groupInfo.getName(), newInfo);
          try {
            flushConfig(newGroupMap);
          } catch (IOException e) {
            LOG.warn("Failed to persist rs group {}", newInfo.getName(), e);
          }
        }
      }
    }
  }

  // Migrate the table rs group info from RSGroupInfo into the table descriptor
  // Notice that we do not want to block the initialize so this will be done in background, and
  // during the migrating, the rs group info maybe incomplete and cause region to be misplaced.
  private void migrate() {
    Thread migrateThread = new Thread(MIGRATE_THREAD_NAME) {

      @Override
      public void run() {
        LOG.info("Start migrating table rs group config");
        while (!masterServices.isStopped()) {
          Collection<RSGroupInfo> groups = holder.groupName2Group.values();
          boolean hasTables = groups.stream().anyMatch(r -> !r.getTables().isEmpty());
          if (!hasTables) {
            break;
          }
          migrate(groups);
        }
        LOG.info("Done migrating table rs group info");
      }
    };
    migrateThread.setDaemon(true);
    migrateThread.start();
  }

  /**
   * Read rsgroup info from the source of truth, the hbase:rsgroup table. Update zk cache. Called on
   * startup of the manager.
   */
  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<RSGroupInfo> groupList = new ArrayList<>();

    // Overwrite anything read from zk, group table is source of truth
    // if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refreshing in Online mode.");
      groupList.addAll(retrieveGroupListFromGroupTable());
    } else {
      LOG.debug("Refreshing in Offline mode.");
      groupList.addAll(retrieveGroupListFromZookeeper());
    }

    // This is added to the last of the list so it overwrites the 'default' rsgroup loaded
    // from region group table or zk
    groupList.add(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, getDefaultServers()));

    // populate the data
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap();
    for (RSGroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
    }
    resetRSGroupMap(newGroupMap);
    updateCacheOfRSGroups(newGroupMap.keySet());
  }

  private void flushConfigTable(Map<String, RSGroupInfo> groupMap) throws IOException {
    List<Mutation> mutations = Lists.newArrayList();

    // populate deletes
    for (String groupName : prevRSGroups) {
      if (!groupMap.containsKey(groupName)) {
        Delete d = new Delete(Bytes.toBytes(groupName));
        mutations.add(d);
      }
    }

    // populate puts
    for (RSGroupInfo gi : groupMap.values()) {
      if (!gi.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        RSGroupProtos.RSGroupInfo proto = ProtobufUtil.toProtoGroupInfo(gi);
        Put p = new Put(Bytes.toBytes(gi.getName()));
        p.addColumn(META_FAMILY_BYTES, META_QUALIFIER_BYTES, proto.toByteArray());
        mutations.add(p);
      }
    }

    if (mutations.size() > 0) {
      multiMutate(mutations);
    }
  }

  private synchronized void flushConfig() throws IOException {
    flushConfig(holder.groupName2Group);
  }

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap) throws IOException {
    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for servers in default group
    if (!isOnline()) {
      if (newGroupMap == holder.groupName2Group) {
        // When newGroupMap is this.rsGroupMap itself,
        // do not need to check default group and other groups as followed
        return;
      }

      Map<String, RSGroupInfo> oldGroupMap = Maps.newHashMap(holder.groupName2Group);
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
      this.holder = new RSGroupInfoHolder(newGroupMap);

      // Do not need to update tableMap
      // because only the update on servers in default group is allowed above,
      // or IOException will be thrown
      return;
    }

    /* For online mode, persist to hbase:rsgroup and Zookeeper */
    flushConfigTable(newGroupMap);

    // Make changes visible after having been persisted to the source of truth
    resetRSGroupMap(newGroupMap);
    saveRSGroupMapToZK(newGroupMap);
    updateCacheOfRSGroups(newGroupMap.keySet());
  }

  private void saveRSGroupMapToZK(Map<String, RSGroupInfo> newGroupMap) throws IOException {
    try {
      String groupBasePath =
          ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, RS_GROUP_ZNODE);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufMagic.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<>(newGroupMap.size());
      for (String groupName : prevRSGroups) {
        if (!newGroupMap.containsKey(groupName)) {
          String znode = ZNodePaths.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }

      for (RSGroupInfo gi : newGroupMap.values()) {
        if (!gi.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
          String znode = ZNodePaths.joinZNode(groupBasePath, gi.getName());
          RSGroupProtos.RSGroupInfo proto = ProtobufUtil.toProtoGroupInfo(gi);
          LOG.debug("Updating znode: " + znode);
          ZKUtil.createAndFailSilent(watcher, znode);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
          zkOps.add(ZKUtil.ZKUtilOp.createAndFailSilent(znode,
              ProtobufUtil.prependPBMagic(proto.toByteArray())));
        }
      }
      LOG.debug("Writing ZK GroupInfo count: " + zkOps.size());

      ZKUtil.multiOrSequential(watcher, zkOps, false);
    } catch (KeeperException e) {
      LOG.error("Failed to write to rsGroupZNode", e);
      masterServices.abort("Failed to write to rsGroupZNode", e);
      throw new IOException("Failed to write to rsGroupZNode", e);
    }
  }

  /**
   * Make changes visible. Caller must be synchronized on 'this'.
   */
  private void resetRSGroupMap(Map<String, RSGroupInfo> newRSGroupMap) {
    this.holder = new RSGroupInfoHolder(newRSGroupMap);
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
    List<ServerName> servers = new ArrayList<>();
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
    // Build a list of servers in other groups than default group, from rsGroupMap
    Set<Address> serversInOtherGroup = new HashSet<>();
    for (RSGroupInfo group : listRSGroups() /* get from rsGroupMap */) {
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
  private synchronized void updateDefaultServers(SortedSet<Address> servers) {
    Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
    RSGroupInfo info = rsGroupMap.get(RSGroupInfo.DEFAULT_GROUP);
    RSGroupInfo newInfo = new RSGroupInfo(info.getName(), servers);
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(newInfo.getName(), newInfo);
    resetRSGroupMap(newGroupMap);
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
          FutureUtils.get(conn.getTable(RSGROUP_TABLE_NAME).get(new Get(ROW_KEY)));
          LOG.info("RSGroup table={} is online, refreshing cached information", RSGROUP_TABLE_NAME);
          RSGroupInfoManagerImpl.this.refresh(true);
          online = true;
          // flush any inconsistencies between ZK and HTable
          RSGroupInfoManagerImpl.this.flushConfig();
          // migrate after we are online.
          migrate();
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
          throw new IOException("Failed to create group table. " +
              MasterProcedureUtil.unwrapRemoteIOException(result));
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
    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        builder
            .addMutationRequest(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        builder.addMutationRequest(
          ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException(
            "multiMutate doesn't support " + mutation.getClass().getName());
      }
    }
    MutateRowsRequest request = builder.build();
    AsyncTable<?> table = conn.getTable(RSGROUP_TABLE_NAME);
    FutureUtils.get(table.<MultiRowMutationService, MutateRowsResponse> coprocessorService(
      MultiRowMutationService::newStub,
      (stub, controller, done) -> stub.mutateRows(controller, request, done), ROW_KEY));
  }

  private void checkGroupName(String groupName) throws ConstraintException {
    if (!groupName.matches("[a-zA-Z0-9_]+")) {
      throw new ConstraintException("RSGroup name should only contain alphanumeric characters");
    }
  }


  @Override
  public RSGroupInfo getRSGroupForTable(TableName tableName) throws IOException {
    return holder.tableName2Group.get(tableName);
  }
}
