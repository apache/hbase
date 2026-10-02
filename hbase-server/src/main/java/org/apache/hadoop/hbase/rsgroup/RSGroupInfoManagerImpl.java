/*
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.util.Shell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;

/**
 * This is an implementation of {@link RSGroupInfoManager} which makes use of an HBase table as the
 * persistence store for the group information. It also makes use of zookeeper to store group
 * information needed for bootstrapping during offline mode.
 * <h2>Concurrency</h2> RSGroup state is kept locally in Maps. There is a rsgroup name to cached
 * RSGroupInfo Map at {@link RSGroupInfoHolder#groupName2Group}. These Maps are persisted to the
 * hbase:rsgroup table (and cached in zk) on each modification.
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
  static final TableName RSGROUP_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");

  static final String KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE =
    "should keep at least " + "one server in 'default' RSGroup.";

  /** Define the config key of retries threshold when movements failed */
  static final String FAILED_MOVE_MAX_RETRY = "hbase.rsgroup.move.max.retry";

  /** Define the default number of retries */
  static final int DEFAULT_MAX_RETRY_VALUE = 50;

  /** Config key prefix; {@code hbase.rsgroup.regex.<groupname>=<regex>} */
  static final String RS_GROUP_REGEX_PREFIX = "hbase.rsgroup.regex.";
  private static final Pattern GROUP_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  private static final String RS_GROUP_ZNODE = "rsgroup";

  static final byte[] META_FAMILY_BYTES = Bytes.toBytes("m");

  static final byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");

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
  private final ServerEventsListenerThread serverEventsListenerThread =
    new ServerEventsListenerThread();
  // contains list of groups that were last flushed to persistent store
  private Set<String> prevRSGroups = new HashSet<>();

  // Package visibility for testing
  static class RSGroupMappingScript {
    static final String RS_GROUP_MAPPING_SCRIPT = "hbase.rsgroup.table.mapping.script";
    static final String RS_GROUP_MAPPING_SCRIPT_TIMEOUT =
      "hbase.rsgroup.table.mapping.script.timeout";

    private final String script;
    private final long scriptTimeout;

    RSGroupMappingScript(Configuration conf) {
      script = conf.get(RS_GROUP_MAPPING_SCRIPT);
      scriptTimeout = conf.getLong(RS_GROUP_MAPPING_SCRIPT_TIMEOUT, 5000); // 5 seconds
    }

    String getRSGroup(String namespace, String tablename) {
      if (script == null || script.isEmpty()) {
        return null;
      }
      Shell.ShellCommandExecutor rsgroupMappingScript =
        new Shell.ShellCommandExecutor(new String[] { script, "", "" }, null, null, scriptTimeout);

      String[] exec = rsgroupMappingScript.getExecString();
      exec[1] = namespace;
      exec[2] = tablename;
      try {
        rsgroupMappingScript.execute();
      } catch (IOException e) {
        // This exception may happen, like process doesn't have permission to run this script.
        LOG.error("{}, placing {} back to default rsgroup", e.getMessage(),
          TableName.valueOf(namespace, tablename));
        return RSGroupInfo.DEFAULT_GROUP;
      }
      return rsgroupMappingScript.getOutput().trim();
    }
  }

  private RSGroupMappingScript script;

  private RSGroupInfoManagerImpl(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.watcher = masterServices.getZooKeeper();
    this.conn = masterServices.getAsyncClusterConnection();
    this.rsGroupStartupWorker = new RSGroupStartupWorker();
    this.script = new RSGroupMappingScript(masterServices.getConfiguration());
  }

  private synchronized void updateAutoManagedRSGroupServers() throws IOException {
    LOG.info("Updating auto-managed RSGroup servers.");
    Map<String, RSGroupInfo> currentGroups = holder.groupName2Group;
    Map<String, SortedSet<Address>> newRSGroupToServers =
      computeAutoManagedRSGroupServers(currentGroups.values());
    Map<String, Set<Address>> currentRSGroupToServers = new HashMap<>();
    for (String groupName : newRSGroupToServers.keySet()) {
      currentRSGroupToServers.put(groupName, currentGroups.get(groupName).getServers());
    }
    if (newRSGroupToServers.equals(currentRSGroupToServers)) {
      LOG.info("No changes in auto-managed RSGroup server membership.");
      return;
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(currentGroups);
    applyAutoManagedRSGroupServers(newGroupMap, newRSGroupToServers);
    flushConfig(newGroupMap, true);
    LOG.info("Updated auto-managed RSGroup servers, {} servers",
      newRSGroupToServers.values().stream().mapToInt(SortedSet::size).sum());
  }

  /**
   * Called synchronously from {@code ServerListener#serverAdded}/{@code #serverRemoved}. If the
   * recompute only affects 'default' membership, applies it immediately in-memory instead of waking
   * the background thread. Only when a regex-governed group's membership would also change -- which
   * needs to be persisted to {@code hbase:rsgroup}/ZK -- does it hand off to the background thread,
   * so this callback is never blocked on that I/O.
   */
  private synchronized void handleServerEvent() {
    Map<String, RSGroupInfo> currentGroups = holder.groupName2Group;
    Map<String, SortedSet<Address>> newRSGroupToServers =
      computeAutoManagedRSGroupServers(currentGroups.values());
    for (Map.Entry<String, SortedSet<Address>> entry : newRSGroupToServers.entrySet()) {
      if (RSGroupInfo.DEFAULT_GROUP.equals(entry.getKey())) {
        continue;
      }
      if (!entry.getValue().equals(currentGroups.get(entry.getKey()).getServers())) {
        serverEventsListenerThread.serverChanged();
        return;
      }
    }
    SortedSet<Address> newDefaultServers = newRSGroupToServers.get(RSGroupInfo.DEFAULT_GROUP);
    if (!newDefaultServers.equals(currentGroups.get(RSGroupInfo.DEFAULT_GROUP).getServers())) {
      updateDefaultServersOnly(newDefaultServers);
    }
  }

  private synchronized void updateDefaultServersOnly(SortedSet<Address> newDefaultServers) {
    LOG.info("Updating default servers.");
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(holder.groupName2Group);
    applyAutoManagedRSGroupServers(newGroupMap,
      Collections.singletonMap(RSGroupInfo.DEFAULT_GROUP, newDefaultServers));
    // do not need to persist, as we do not persist default group.
    resetRSGroupMap(newGroupMap);
    LOG.info("Updated default servers, {} servers", newDefaultServers.size());
  }

  private synchronized void init() throws IOException {
    refresh(false);
    serverEventsListenerThread.start();
    masterServices.getServerManager().registerListener(serverEventsListenerThread);
  }

  static RSGroupInfoManager getInstance(MasterServices masterServices) throws IOException {
    RSGroupInfoManagerImpl instance = new RSGroupInfoManagerImpl(masterServices);
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
    if (
      rsGroupMap.get(rsGroupInfo.getName()) != null
        || rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)
    ) {
      throw new ConstraintException("Group already exists: " + rsGroupInfo.getName());
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.put(rsGroupInfo.getName(), rsGroupInfo);
    flushConfig(newGroupMap);
    LOG.info("Add group {} done.", rsGroupInfo.getName());
  }

  private RSGroupInfo getRSGroupInfo(final String groupName) throws ConstraintException {
    RSGroupInfo rsGroupInfo = holder.groupName2Group.get(groupName);
    if (rsGroupInfo == null) {
      throw new ConstraintException("RSGroup " + groupName + " does not exist");
    }
    return rsGroupInfo;
  }

  /** Returns Set of online Servers named for their hostname and port (not ServerName). */
  private Set<Address> getOnlineServers() {
    return masterServices.getServerManager().getOnlineServers().keySet().stream()
      .map(ServerName::getAddress).collect(Collectors.toSet());
  }

  /**
   * Returns a copy of {@code src}, safe to mutate without affecting the live {@code
   * holder.groupName2Group} entry -- callers must not mutate the object returned by
   * {@link #getRSGroupInfo} or {@link #getRSGroupOfServer} in place, since a subsequent
   * {@link #flushConfig(Map)} can still reject the change and the live state must stay untouched
   * until the change is actually persisted.
   */
  private static RSGroupInfo copyOf(RSGroupInfo src) {
    RSGroupInfo copy = new RSGroupInfo(src);
    // The copy constructor does not copy configuration, so do it by hand.
    src.getConfiguration().forEach(copy::setConfiguration);
    return copy;
  }

  public synchronized Set<Address> moveServers(Set<Address> servers, String srcGroup,
    String dstGroup) throws IOException {
    RSGroupInfo src = copyOf(getRSGroupInfo(srcGroup));
    RSGroupInfo dst = copyOf(getRSGroupInfo(dstGroup));
    Set<Address> movedServers = new HashSet<>();
    // If destination is 'default' rsgroup, only add servers that are online. If not online, drop
    // it. If not 'default' group, add server to 'dst' rsgroup EVEN IF IT IS NOT online (could be a
    // rsgroup of dead servers that are to come back later).
    Set<Address> onlineServers =
      dst.getName().equals(RSGroupInfo.DEFAULT_GROUP) ? getOnlineServers() : null;
    for (Address el : servers) {
      src.removeServer(el);
      if (onlineServers != null) {
        if (!onlineServers.contains(el)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Dropping " + el + " during move-to-default RSGroup because not online");
          }
          continue;
        }
      }
      dst.addServer(el);
      movedServers.add(el);
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(holder.groupName2Group);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);
    flushConfig(newGroupMap);
    return movedServers;
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) {
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
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    int serverCount = rsGroupInfo.getServers().size();
    if (serverCount > 0) {
      throw new ConstraintException("RSGroup " + groupName + " has " + serverCount
        + " servers; you must remove these servers from the RSGroup before"
        + " the RSGroup can be removed.");
    }
    for (TableDescriptor td : masterServices.getTableDescriptors().getAll().values()) {
      if (td.getRegionServerGroup().map(groupName::equals).orElse(false)) {
        throw new ConstraintException("RSGroup " + groupName + " is already referenced by "
          + td.getTableName() + "; you must remove all the tables from the RSGroup before "
          + "the RSGroup can be removed.");
      }
    }
    for (NamespaceDescriptor ns : masterServices.getClusterSchema().getNamespaces()) {
      String nsGroup = ns.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
      if (nsGroup != null && nsGroup.equals(groupName)) {
        throw new ConstraintException(
          "RSGroup " + groupName + " is referenced by namespace: " + ns.getName());
      }
    }
    Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
    if (!rsGroupMap.containsKey(groupName) || groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new ConstraintException(
        "Group " + groupName + " does not exist or is a reserved " + "group");
    }
    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(groupName);
    flushConfig(newGroupMap);
    LOG.info("Remove group {} done", groupName);
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
    if (servers == null || servers.isEmpty()) {
      throw new ConstraintException("The set of servers to remove cannot be null or empty.");
    }

    // check the set of servers
    checkForDeadOrOnlineServers(servers);

    Map<String, RSGroupInfo> rsGroupInfos = new HashMap<String, RSGroupInfo>();
    for (Address el : servers) {
      RSGroupInfo rsGroupInfo = getRSGroupOfServer(el);
      if (rsGroupInfo != null) {
        RSGroupInfo newRsGroupInfo = rsGroupInfos.get(rsGroupInfo.getName());
        if (newRsGroupInfo == null) {
          newRsGroupInfo = copyOf(rsGroupInfo);
        }
        newRsGroupInfo.removeServer(el);
        rsGroupInfos.put(newRsGroupInfo.getName(), newRsGroupInfo);
      } else {
        LOG.warn("Server " + el + " does not belong to any rsgroup.");
      }
    }

    if (rsGroupInfos.size() > 0) {
      Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(holder.groupName2Group);
      newGroupMap.putAll(rsGroupInfos);
      flushConfig(newGroupMap);
    }
    LOG.info("Remove decommissioned servers {} from RSGroup done", servers);
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
          if (data != null && data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis =
              new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
            RSGroupInfoList.add(ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
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
    ProcedureExecutor<MasterProcedureEnv> procExec = masterServices.getMasterProcedureExecutor();
    for (RSGroupInfo groupInfo : groupList) {
      if (groupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        continue;
      }
      SortedSet<TableName> failedTables = new TreeSet<>();
      List<MigrateRSGroupProcedure> procs = new ArrayList<>();
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
        // This is a bit tricky. Since we know that the region server group config in
        // TableDescriptor will only be used at master side, it is fine to just update the table
        // descriptor on file system and also the cache, without reopening all the regions. This
        // will be much faster than the normal modifyTable. And when upgrading, we will update
        // master first and then region server, so after all the region servers has been reopened,
        // the new TableDescriptor will be loaded.
        MigrateRSGroupProcedure proc =
          new MigrateRSGroupProcedure(procExec.getEnvironment(), tableName);
        procExec.submitProcedure(proc);
        procs.add(proc);
      }
      for (MigrateRSGroupProcedure proc : procs) {
        try {
          ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60000);
        } catch (IOException e) {
          LOG.warn("Failed to migrate rs group {} for table {}", groupInfo.getName(),
            proc.getTableName());
          failedTables.add(proc.getTableName());
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
    LOG.info("Refreshing RSGroup info from source of truth: forceOnline={}, isOnline={}",
      forceOnline, isOnline());
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
    groupList.add(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP));

    // populate the data
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap();
    for (RSGroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
    }
    // Server membership for 'default' and for every regex-governed group is always recomputed
    // on the fly -- never trusted from storage -- exactly like 'default' membership already was.
    applyAutoManagedRSGroupServers(newGroupMap, computeAutoManagedRSGroupServers(groupList));
    resetRSGroupMap(newGroupMap);
    updateCacheOfRSGroups(newGroupMap.keySet());
    LOG.info("Refresh completed successfully");
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
    flushConfig(newGroupMap, false);
  }

  /**
   * @param isAutoRegexUpdate {@code true} when called from
   *                          {@link #updateAutoManagedRSGroupServers()} to persist a regex-driven
   *                          recompute -- relaxes the offline-mode check (see
   *                          {@link #checkOnlyServerSetsDifferForAutoUpdate}) to allow any
   *                          regex-governed group's servers to change, not just 'default''s.
   */
  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap,
    boolean isAutoRegexUpdate) throws IOException {
    checkRegexBasedRSGroupMembership(newGroupMap);

    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for servers in default group
    if (!isOnline()) {
      if (newGroupMap == holder.groupName2Group) {
        // When newGroupMap is this.rsGroupMap itself,
        // do not need to check default group and other groups as followed
        return;
      }

      LOG.debug("Offline mode, cannot persist to {}", RSGROUP_TABLE_NAME);

      if (isAutoRegexUpdate) {
        checkOnlyServerSetsDifferForAutoUpdate(newGroupMap);
      } else {
        Map<String, RSGroupInfo> oldGroupMap = Maps.newHashMap(holder.groupName2Group);
        RSGroupInfo oldDefaultGroup = oldGroupMap.remove(RSGroupInfo.DEFAULT_GROUP);
        RSGroupInfo newDefaultGroup = newGroupMap.remove(RSGroupInfo.DEFAULT_GROUP);
        if (
          !oldGroupMap.equals(newGroupMap)
            /* compare both tables and servers in other groups */ || !oldDefaultGroup.getTables()
              .equals(newDefaultGroup.getTables())
          /* compare tables in default group */
        ) {
          throw new IOException("Only servers in default group can be updated during offline mode");
        }

        // Restore newGroupMap by putting its default group back
        newGroupMap.put(RSGroupInfo.DEFAULT_GROUP, newDefaultGroup);
      }

      // Refresh rsGroupMap
      // according to the inputted newGroupMap (an updated copy of rsGroupMap)
      this.holder = new RSGroupInfoHolder(newGroupMap);

      LOG.debug("New RSGroup map: {}", newGroupMap);

      // Do not need to update tableMap
      // because only server-set updates are allowed above,
      // or an IOException will be thrown
      return;
    }

    /* For online mode, persist to hbase:rsgroup and Zookeeper */
    LOG.debug("Online mode, persisting to {} and ZK", RSGROUP_TABLE_NAME);
    flushConfigTable(newGroupMap);

    // Make changes visible after having been persisted to the source of truth
    resetRSGroupMap(newGroupMap);
    saveRSGroupMapToZK(newGroupMap);
    updateCacheOfRSGroups(newGroupMap.keySet());
    LOG.info("Flush config done, new RSGroup map: {}", newGroupMap);
  }

  /**
   * Guards the offline-mode path of {@link #flushConfig(Map, boolean)} for a regex-driven
   * auto-update: such an update must never add/remove RSGroups nor change any group's tables or
   * configuration -- only server sets may differ from what is currently held in-memory.
   */
  private void checkOnlyServerSetsDifferForAutoUpdate(Map<String, RSGroupInfo> newGroupMap)
    throws IOException {
    if (!holder.groupName2Group.keySet().equals(newGroupMap.keySet())) {
      throw new IOException("Automatic regex-based RSGroup update must not add/remove RSGroups");
    }
    for (String groupName : newGroupMap.keySet()) {
      RSGroupInfo oldInfo = holder.groupName2Group.get(groupName);
      RSGroupInfo newInfo = newGroupMap.get(groupName);
      if (
        !oldInfo.getTables().equals(newInfo.getTables())
          || !oldInfo.getConfiguration().equals(newInfo.getConfiguration())
      ) {
        throw new IOException("Automatic regex-based RSGroup update must not change tables or "
          + "configuration (RSGroup '" + groupName + "')");
      }
    }
  }

  private void saveRSGroupMapToZK(Map<String, RSGroupInfo> newGroupMap) throws IOException {
    LOG.debug("Saving RSGroup info to ZK");
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

  /**
   * Parses {@code hbase.rsgroup.regex.<groupname>=<regex>} entries out of {@code conf}. Entries
   * with an invalid group name, targeting the reserved {@link RSGroupInfo#DEFAULT_GROUP}, or with
   * an unparsable regex are logged and skipped.
   */
  static Map<String, Pattern> getRegexGroupMap(Configuration conf) {
    Map<String, String> rsGroupNameToRegexMap = conf.getPropsWithPrefix(RS_GROUP_REGEX_PREFIX);
    Map<String, Pattern> rsGroupNameToPatternMap = new HashMap<>();
    for (Map.Entry<String, String> e : rsGroupNameToRegexMap.entrySet()) {
      if (!GROUP_NAME_PATTERN.matcher(e.getKey()).matches()) {
        LOG.warn("Ignoring {}{} -- '{}' is not a valid RSGroup name (only alphanumeric characters "
          + "and underscore allowed)", RS_GROUP_REGEX_PREFIX, e.getKey(), e.getKey());
        continue;
      }
      if (RSGroupInfo.DEFAULT_GROUP.equals(e.getKey())) {
        LOG.warn("Ignoring {}{} -- regex-based membership cannot target the reserved '{}' group",
          RS_GROUP_REGEX_PREFIX, e.getKey(), RSGroupInfo.DEFAULT_GROUP);
        continue;
      }
      try {
        rsGroupNameToPatternMap.put(e.getKey(), Pattern.compile(e.getValue()));
      } catch (PatternSyntaxException ex) {
        LOG.warn("Invalid regex '{}' for RSGroup '{}' ({}{}); ignoring this entry", e.getValue(),
          e.getKey(), RS_GROUP_REGEX_PREFIX, e.getKey());
      }
    }
    LOG.info("Resolved regex-based RSGroup membership config: {}", rsGroupNameToPatternMap);
    return rsGroupNameToPatternMap;
  }

  private static List<String> getMatchingRSGroupNames(String hostname,
    Map<String, Pattern> rsGroupNameToPatternMap) {
    List<String> rsGroupNames = new ArrayList<>();
    for (Map.Entry<String, Pattern> e : rsGroupNameToPatternMap.entrySet()) {
      if (e.getValue().matcher(hostname).matches()) {
        rsGroupNames.add(e.getKey());
      }
    }
    LOG.info("Hostname '{}' matches RSGroup name(s) {}", hostname, rsGroupNames);
    return rsGroupNames;
  }

  /**
   * Resolves every server in {@code onlineServers} whose hostname unambiguously matches exactly one
   * entry of {@code rsGroupNameToPatternMap} naming a group in {@code existingGroupNames}. A server
   * matching multiple regexes, or matching a regex for a group that does not (yet) exist, is
   * omitted (warn-only) rather than resolved.
   */
  static Map<Address, String> resolveServerAddrToRSGroupName(Set<Address> onlineServers,
    Map<String, Pattern> rsGroupNameToPatternMap, Set<String> existingGroupNames) {
    Map<Address, String> serverAddrToRSGroupName = new HashMap<>();
    if (rsGroupNameToPatternMap.isEmpty()) {
      return serverAddrToRSGroupName;
    }
    Set<String> nonExistingRSGroupNames = new HashSet<>();
    for (Address server : onlineServers) {
      List<String> matchingRSGroupNames =
        getMatchingRSGroupNames(server.getHostName(), rsGroupNameToPatternMap);
      if (matchingRSGroupNames.isEmpty()) {
        LOG.info("Server {} hostname does not match any regex-based RSGroup names", server);
        continue;
      }
      if (matchingRSGroupNames.size() > 1) {
        LOG.warn("Server {} hostname matches regexes for multiple RSGroups {}; treating it as "
          + "unmatched by regex (falls back to default) until the overlapping "
          + "hbase.rsgroup.regex.* entries are fixed", server, matchingRSGroupNames);
        continue;
      }
      String rsGroupName = matchingRSGroupNames.get(0);
      if (existingGroupNames.contains(rsGroupName)) {
        serverAddrToRSGroupName.put(server, rsGroupName);
      } else if (nonExistingRSGroupNames.add(rsGroupName)) {
        LOG.warn(
          "Config {}{} matches server {} but RSGroup '{}' does not exist -- create it with "
            + "addRSGroup first; treating this server as unmatched by regex until then",
          RS_GROUP_REGEX_PREFIX, rsGroupName, server, rsGroupName);
      }
    }
    LOG.info("Resolved server address to RSGroup name map: {}", serverAddrToRSGroupName);
    return serverAddrToRSGroupName;
  }

  /** Returns {@code true} if every online server is either admin-managed or regex-matched. */
  static boolean wouldEmptyDefaultGroup(Set<Address> onlineServers,
    Set<Address> adminManagedServers, Set<Address> regexMatchedServers) {
    for (Address server : onlineServers) {
      if (!adminManagedServers.contains(server) && !regexMatchedServers.contains(server)) {
        return false;
      }
    }
    return !onlineServers.isEmpty();
  }

  static final class RegexBasedRSGroupMembershipResolution {
    final Set<Address> adminManagedServers;
    final Map<Address, String> regexMatchedServers;
    final boolean wouldEmptyDefaultGroup;

    RegexBasedRSGroupMembershipResolution(Set<Address> adminManagedServers,
      Map<Address, String> regexMatchedServers, boolean wouldEmptyDefaultGroup) {
      this.adminManagedServers = adminManagedServers;
      this.regexMatchedServers = regexMatchedServers;
      this.wouldEmptyDefaultGroup = wouldEmptyDefaultGroup;
    }
  }

  static RegexBasedRSGroupMembershipResolution resolveRegexBasedRSGroupMembership(
    Map<String, Pattern> rsGroupNameToPatternMap, Set<Address> onlineServers,
    Collection<RSGroupInfo> existingGroups) {
    Set<String> existingGroupNames =
      existingGroups.stream().map(RSGroupInfo::getName).collect(Collectors.toSet());
    Set<Address> adminManagedServers = new HashSet<>();
    for (RSGroupInfo existingGroup : existingGroups) {
      if (
        !RSGroupInfo.DEFAULT_GROUP.equals(existingGroup.getName())
          && !rsGroupNameToPatternMap.containsKey(existingGroup.getName())
      ) {
        adminManagedServers.addAll(existingGroup.getServers());
      }
    }
    Map<Address, String> regexMatchedServers =
      resolveServerAddrToRSGroupName(onlineServers, rsGroupNameToPatternMap, existingGroupNames);
    boolean wouldEmptyDefault =
      wouldEmptyDefaultGroup(onlineServers, adminManagedServers, regexMatchedServers.keySet());
    if (wouldEmptyDefault) {
      LOG.warn(
        "Regex-based RSGroup membership would leave RSGroup '{}' with no online servers -- every "
          + "online server is either admin-managed or matches a {}* entry; skipping automatic "
          + "regex-based enforcement until this no longer covers every online server",
        RSGroupInfo.DEFAULT_GROUP, RS_GROUP_REGEX_PREFIX);
    }
    Map<Address, String> finalRegexMatchedServers =
      wouldEmptyDefault ? Collections.emptyMap() : regexMatchedServers;
    LOG.info(
      "Regex-based RSGroup membership resolution: onlineServers={}, adminManagedServers={}, "
        + "regexMatchedServers={}, wouldEmptyDefaultGroup={}",
      onlineServers, adminManagedServers, finalRegexMatchedServers, wouldEmptyDefault);
    return new RegexBasedRSGroupMembershipResolution(adminManagedServers, finalRegexMatchedServers,
      wouldEmptyDefault);
  }

  private Map<String, SortedSet<Address>>
    computeAutoManagedRSGroupServers(Collection<RSGroupInfo> existingGroups) {
    LOG.info("Computing auto-managed RSGroup server membership.");
    Set<Address> onlineServers = getOnlineServers();
    Map<String, Pattern> rsGroupNameToPatternMap =
      getRegexGroupMap(masterServices.getConfiguration());
    RegexBasedRSGroupMembershipResolution resolution =
      resolveRegexBasedRSGroupMembership(rsGroupNameToPatternMap, onlineServers, existingGroups);

    Set<String> existingGroupNames =
      existingGroups.stream().map(RSGroupInfo::getName).collect(Collectors.toSet());
    Map<String, SortedSet<Address>> result = new HashMap<>();
    for (String rsGroupName : rsGroupNameToPatternMap.keySet()) {
      if (existingGroupNames.contains(rsGroupName)) {
        result.put(rsGroupName, new TreeSet<>());
      }
    }
    result.put(RSGroupInfo.DEFAULT_GROUP, new TreeSet<>());

    for (Address server : onlineServers) {
      String regexGroupName = resolution.regexMatchedServers.get(server);
      if (regexGroupName != null) {
        result.get(regexGroupName).add(server);
        continue;
      }
      if (resolution.adminManagedServers.contains(server)) {
        continue;
      }
      result.get(RSGroupInfo.DEFAULT_GROUP).add(server);
    }
    LOG.info("Computed auto-managed RSGroup server membership: {}", result);
    return result;
  }

  private static void applyAutoManagedRSGroupServers(Map<String, RSGroupInfo> groupMap,
    Map<String, SortedSet<Address>> newRSGroupToServers) {
    for (Map.Entry<String, SortedSet<Address>> entry : newRSGroupToServers.entrySet()) {
      RSGroupInfo oldInfo = groupMap.get(entry.getKey());
      if (oldInfo == null) {
        continue;
      }
      RSGroupInfo newInfo = new RSGroupInfo(entry.getKey(), entry.getValue());
      newInfo.addAllTables(oldInfo.getTables());
      oldInfo.getConfiguration().forEach(newInfo::setConfiguration);
      groupMap.put(entry.getKey(), newInfo);
    }
  }

  private void checkRegexBasedRSGroupMembership(Map<String, RSGroupInfo> newGroupMap)
    throws IOException {
    LOG.info("Validating regex-based RSGroup membership");
    Map<String, Pattern> rsGroupNameToPatternMap =
      getRegexGroupMap(masterServices.getConfiguration());
    if (rsGroupNameToPatternMap.isEmpty()) {
      LOG.info("No regex-based RSGroup membership config found");
      return;
    }
    Set<Address> onlineServers = getOnlineServers();
    RegexBasedRSGroupMembershipResolution resolution = resolveRegexBasedRSGroupMembership(
      rsGroupNameToPatternMap, onlineServers, newGroupMap.values());
    if (resolution.wouldEmptyDefaultGroup) {
      return;
    }

    for (RSGroupInfo group : newGroupMap.values()) {
      Pattern ownPattern = rsGroupNameToPatternMap.get(group.getName());
      for (Address server : group.getServers()) {
        if (!onlineServers.contains(server)) {
          continue;
        }
        if (ownPattern != null) {
          // Regex-governed group: every online server in it must match its own pattern.
          if (!ownPattern.matcher(server.getHostName()).matches()) {
            throw new DoNotRetryIOException("Server " + server + " is in regex-governed RSGroup '"
              + group.getName() + "' but its hostname does not match " + RS_GROUP_REGEX_PREFIX
              + group.getName() + "; a regex-governed RSGroup may only contain servers matching "
              + "its own regex. Fix/remove the " + RS_GROUP_REGEX_PREFIX + group.getName()
              + " config.");
          }
        } else {
          // 'default' or an admin-managed group: must not contain a server that unambiguously
          // matches an existing regex-governed group -- it should be placed there instead.
          String expectedGroupName = resolution.regexMatchedServers.get(server);
          if (
            expectedGroupName != null && !expectedGroupName.equals(group.getName())
              && holder.groupName2Group.containsKey(expectedGroupName)
          ) {
            throw new DoNotRetryIOException(
              "Server " + server + " hostname matches configured " + "regex "
                + RS_GROUP_REGEX_PREFIX + expectedGroupName + " and must belong to RSGroup '"
                + expectedGroupName + "' but would be placed in " + "RSGroup '" + group.getName()
                + "'. Fix/remove the " + RS_GROUP_REGEX_PREFIX + expectedGroupName + " config.");
          }
        }
      }
    }
    LOG.info("Regex-based RSGroup membership validation completed successfully");
  }

  private class ServerEventsListenerThread extends Thread implements ServerListener {
    private final Logger LOG = LoggerFactory.getLogger(ServerEventsListenerThread.class);
    private volatile int eventCount = 0;

    ServerEventsListenerThread() {
      setDaemon(true);
    }

    @Override
    public void serverAdded(ServerName serverName) {
      LOG.info("Server added: {}", serverName);
      RSGroupInfoManagerImpl.this.handleServerEvent();
    }

    @Override
    public void serverRemoved(ServerName serverName) {
      LOG.info("Server removed: {}", serverName);
      RSGroupInfoManagerImpl.this.handleServerEvent();
    }

    private synchronized void serverChanged() {
      eventCount++;
      this.notify();
    }

    @Override
    public void run() {
      setName(ServerEventsListenerThread.class.getName() + "-" + masterServices.getServerName());
      while (isMasterRunning(masterServices)) {
        try {
          try {
            synchronized (this) {
              while (eventCount <= 0) {
                wait();
              }
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
            continue;
          }
          updateAutoManagedRSGroupServers();
          synchronized (this) {
            if (eventCount > 0) {
              eventCount--;
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to update auto-managed RSGroup servers", e);
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
        LOG.debug("Creating group table {}", RSGROUP_TABLE_NAME);
        procId = masterServices.createSystemTable(RSGROUP_TABLE_DESC);
      }
      // wait for region to be online
      int tries = 600;
      while (
        !(masterServices.getMasterProcedureExecutor().isFinished(procId))
          && masterServices.getMasterProcedureExecutor().isRunning() && tries > 0
      ) {
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
    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        builder
          .addMutationRequest(ProtobufUtil.toMutation(MutationProto.MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        builder
          .addMutationRequest(ProtobufUtil.toMutation(MutationProto.MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException(
          "multiMutate doesn't support " + mutation.getClass().getName());
      }
    }
    MutateRowsRequest request = builder.build();
    AsyncTable<?> table = conn.getTable(RSGROUP_TABLE_NAME);
    LOG.debug("Multimutating {} with {} mutations", RSGROUP_TABLE_NAME, mutations.size());
    FutureUtils.get(table.<MultiRowMutationService, MutateRowsResponse> coprocessorService(
      MultiRowMutationService::newStub,
      (stub, controller, done) -> stub.mutateRows(controller, request, done), ROW_KEY));
    LOG.info("Multimutating {} with {} mutations done", RSGROUP_TABLE_NAME, mutations.size());
  }

  private void checkGroupName(String groupName) throws ConstraintException {
    if (!GROUP_NAME_PATTERN.matcher(groupName).matches()) {
      throw new ConstraintException("RSGroup name should only contain alphanumeric characters");
    }
  }

  @Override
  public RSGroupInfo getRSGroupForTable(TableName tableName) throws IOException {
    return holder.tableName2Group.get(tableName);
  }

  /**
   * Check if the set of servers are belong to dead servers list or online servers list.
   * @param servers servers to remove
   */
  private void checkForDeadOrOnlineServers(Set<Address> servers) throws IOException {
    // This ugliness is because we only have Address, not ServerName.
    Set<Address> onlineServers = new HashSet<>();
    List<ServerName> drainingServers = masterServices.getServerManager().getDrainingServersList();
    for (ServerName server : masterServices.getServerManager().getOnlineServers().keySet()) {
      // Only online but not decommissioned servers are really online
      if (!drainingServers.contains(server)) {
        onlineServers.add(server.getAddress());
      }
    }

    Set<Address> deadServers = new HashSet<>();
    for (ServerName server : masterServices.getServerManager().getDeadServers().copyServerNames()) {
      deadServers.add(server.getAddress());
    }

    for (Address address : servers) {
      if (onlineServers.contains(address)) {
        throw new DoNotRetryIOException(
          "Server " + address + " is an online server, not allowed to remove.");
      }
      if (deadServers.contains(address)) {
        throw new DoNotRetryIOException("Server " + address + " is on the dead servers list,"
          + " Maybe it will come back again, not allowed to remove.");
      }
    }
  }

  private void checkOnlineServersOnly(Set<Address> servers) throws IOException {
    // This uglyness is because we only have Address, not ServerName.
    // Online servers are keyed by ServerName.
    Set<Address> onlineServers = new HashSet<>();
    for (ServerName server : masterServices.getServerManager().getOnlineServers().keySet()) {
      onlineServers.add(server.getAddress());
    }
    for (Address address : servers) {
      if (!onlineServers.contains(address)) {
        throw new DoNotRetryIOException(
          "Server " + address + " is not an online server in 'default' RSGroup.");
      }
    }
  }

  /** Returns List of Regions associated with this <code>server</code>. */
  private List<RegionInfo> getRegions(final Address server) {
    LinkedList<RegionInfo> regions = new LinkedList<>();
    for (Map.Entry<RegionInfo, ServerName> el : masterServices.getAssignmentManager()
      .getRegionStates().getRegionAssignments().entrySet()) {
      if (el.getValue() == null) {
        continue;
      }

      if (el.getValue().getAddress().equals(server)) {
        addRegion(regions, el.getKey());
      }
    }
    for (RegionStateNode state : masterServices.getAssignmentManager().getRegionsInTransition()) {
      if (
        state.getRegionLocation() != null && state.getRegionLocation().getAddress().equals(server)
      ) {
        addRegion(regions, state.getRegionInfo());
      }
    }
    return regions;
  }

  private void addRegion(final LinkedList<RegionInfo> regions, RegionInfo hri) {
    // If meta, move it last otherwise other unassigns fail because meta is not
    // online for them to update state in. This is dodgy. Needs to be made more
    // robust. See TODO below.
    if (hri.isMetaRegion()) {
      regions.addLast(hri);
    } else {
      regions.addFirst(hri);
    }
  }

  /**
   * Move every region from servers which are currently located on these servers, but should not be
   * located there.
   * @param movedServers    the servers that are moved to new group
   * @param srcGrpServers   all servers in the source group, excluding the movedServers
   * @param targetGroupName the target group
   * @param sourceGroupName the source group
   * @throws IOException if moving the server and tables fail
   */
  private void moveServerRegionsFromGroup(Set<Address> movedServers, Set<Address> srcGrpServers,
    String targetGroupName, String sourceGroupName) throws IOException {
    moveRegionsBetweenGroups(movedServers, srcGrpServers, targetGroupName, sourceGroupName,
      rs -> getRegions(rs), info -> {
        try {
          String groupName = RSGroupUtil.getRSGroupInfo(masterServices, this, info.getTable())
            .map(RSGroupInfo::getName).orElse(RSGroupInfo.DEFAULT_GROUP);
          return groupName.equals(targetGroupName);
        } catch (IOException e) {
          LOG.warn("Failed to test group for region {} and target group {}", info, targetGroupName);
          return false;
        }
      });
  }

  private <T> void moveRegionsBetweenGroups(Set<T> regionsOwners, Set<Address> newRegionsOwners,
    String targetGroupName, String sourceGroupName, Function<T, List<RegionInfo>> getRegionsInfo,
    Function<RegionInfo, Boolean> validation) throws IOException {
    // Get server names corresponding to given Addresses
    List<ServerName> movedServerNames = new ArrayList<>(regionsOwners.size());
    List<ServerName> srcGrpServerNames = new ArrayList<>(newRegionsOwners.size());
    for (ServerName serverName : masterServices.getServerManager().getOnlineServers().keySet()) {
      // In case region move failed in previous attempt, regionsOwners and newRegionsOwners
      // can have the same servers. So for all servers below both conditions to be checked
      if (newRegionsOwners.contains(serverName.getAddress())) {
        srcGrpServerNames.add(serverName);
      }
      if (regionsOwners.contains(serverName.getAddress())) {
        movedServerNames.add(serverName);
      }
    }
    List<Pair<RegionInfo, Future<byte[]>>> assignmentFutures = new ArrayList<>();
    int retry = 0;
    Set<String> failedRegions = new HashSet<>();
    IOException toThrow = null;
    do {
      assignmentFutures.clear();
      failedRegions.clear();
      for (ServerName owner : movedServerNames) {
        // Get regions that are associated with this server and filter regions by group tables.
        for (RegionInfo region : getRegionsInfo.apply((T) owner.getAddress())) {
          if (!validation.apply(region)) {
            LOG.info("Moving region {}, which does not belong to RSGroup {}",
              region.getShortNameToLog(), targetGroupName);
            // Move region back to source RSGroup servers
            ServerName dest =
              masterServices.getLoadBalancer().randomAssignment(region, srcGrpServerNames);
            if (dest == null) {
              failedRegions.add(region.getRegionNameAsString());
              continue;
            }
            RegionPlan rp = new RegionPlan(region, owner, dest);
            try {
              Future<byte[]> future = masterServices.getAssignmentManager().moveAsync(rp);
              assignmentFutures.add(Pair.newPair(region, future));
            } catch (IOException ioe) {
              failedRegions.add(region.getRegionNameAsString());
              LOG.debug("Move region {} failed, will retry, current retry time is {}",
                region.getShortNameToLog(), retry, ioe);
              toThrow = ioe;
            }
          }
        }
      }
      waitForRegionMovement(assignmentFutures, failedRegions, sourceGroupName, retry);
      if (failedRegions.isEmpty()) {
        LOG.info("All regions from {} are moved back to {}", movedServerNames, sourceGroupName);
        return;
      } else {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
          Thread.currentThread().interrupt();
        }
        retry++;
      }
    } while (
      !failedRegions.isEmpty() && retry <= masterServices.getConfiguration()
        .getInt(FAILED_MOVE_MAX_RETRY, DEFAULT_MAX_RETRY_VALUE)
    );

    // has up to max retry time or there are no more regions to move
    if (!failedRegions.isEmpty()) {
      // print failed moved regions, for later process conveniently
      String msg = String.format("move regions for group %s failed, failed regions: %s",
        sourceGroupName, failedRegions);
      LOG.error(msg);
      throw new DoNotRetryIOException(
        msg + ", just record the last failed region's cause, more details in server log", toThrow);
    }
  }

  /**
   * Wait for all the region move to complete. Keep waiting for other region movement completion
   * even if some region movement fails.
   */
  private void waitForRegionMovement(List<Pair<RegionInfo, Future<byte[]>>> regionMoveFutures,
    Set<String> failedRegions, String sourceGroupName, int retryCount) {
    LOG.info("Moving {} region(s) to group {}, current retry={}", regionMoveFutures.size(),
      sourceGroupName, retryCount);
    for (Pair<RegionInfo, Future<byte[]>> pair : regionMoveFutures) {
      try {
        pair.getSecond().get();
        if (
          masterServices.getAssignmentManager().getRegionStates().getRegionState(pair.getFirst())
            .isFailedOpen()
        ) {
          failedRegions.add(pair.getFirst().getRegionNameAsString());
        }
      } catch (InterruptedException e) {
        // Dont return form there lets wait for other regions to complete movement.
        failedRegions.add(pair.getFirst().getRegionNameAsString());
        LOG.warn("Sleep interrupted", e);
      } catch (Exception e) {
        failedRegions.add(pair.getFirst().getRegionNameAsString());
        LOG.error("Move region {} to group {} failed, will retry on next attempt",
          pair.getFirst().getShortNameToLog(), sourceGroupName, e);
      }
    }
  }

  private boolean isTableInGroup(TableName tableName, String groupName,
    Set<TableName> tablesInGroupCache) throws IOException {
    if (tablesInGroupCache.contains(tableName)) {
      return true;
    }
    if (
      RSGroupUtil.getRSGroupInfo(masterServices, this, tableName).map(RSGroupInfo::getName)
        .orElse(RSGroupInfo.DEFAULT_GROUP).equals(groupName)
    ) {
      tablesInGroupCache.add(tableName);
      return true;
    }
    return false;
  }

  private Map<String, RegionState> rsGroupGetRegionsInTransition(String groupName)
    throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    Set<TableName> tablesInGroupCache = new HashSet<>();
    for (RegionStateNode regionNode : masterServices.getAssignmentManager()
      .getRegionsInTransition()) {
      TableName tn = regionNode.getTable();
      if (isTableInGroup(tn, groupName, tablesInGroupCache)) {
        rit.put(regionNode.getRegionInfo().getEncodedName(), regionNode.toRegionState());
      }
    }
    return rit;
  }

  /**
   * This is an EXPENSIVE clone. Cloning though is the safest thing to do. Can't let out original
   * since it can change and at least the load balancer wants to iterate this exported list. Load
   * balancer should iterate over this list because cloned list will ignore disabled table and split
   * parent region cases. This method is invoked by {@link #balanceRSGroup}
   * @return A clone of current assignments for this group.
   */
  Map<TableName, Map<ServerName, List<RegionInfo>>> getRSGroupAssignmentsByTable(
    TableStateManager tableStateManager, String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<RegionInfo>>> result = Maps.newHashMap();
    Set<TableName> tablesInGroupCache = new HashSet<>();
    for (Map.Entry<RegionInfo, ServerName> entry : masterServices.getAssignmentManager()
      .getRegionStates().getRegionAssignments().entrySet()) {
      RegionInfo region = entry.getKey();
      TableName tn = region.getTable();
      ServerName server = entry.getValue();
      if (isTableInGroup(tn, groupName, tablesInGroupCache)) {
        if (
          tableStateManager.isTableState(tn, TableState.State.DISABLED, TableState.State.DISABLING)
        ) {
          continue;
        }
        if (region.isSplitParent()) {
          continue;
        }
        result.computeIfAbsent(tn, k -> new HashMap<>())
          .computeIfAbsent(server, k -> new ArrayList<>()).add(region);
      }
    }
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    for (ServerName serverName : masterServices.getServerManager().getOnlineServers().keySet()) {
      if (rsGroupInfo.containsServer(serverName.getAddress())) {
        for (Map<ServerName, List<RegionInfo>> map : result.values()) {
          map.computeIfAbsent(serverName, k -> Collections.emptyList());
        }
      }
    }
    return result;
  }

  @Override
  public BalanceResponse balanceRSGroup(String groupName, BalanceRequest request)
    throws IOException {
    ServerManager serverManager = masterServices.getServerManager();
    LoadBalancer balancer = masterServices.getLoadBalancer();
    getRSGroupInfo(groupName);

    BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder();

    synchronized (balancer) {
      // If balance not true, don't run balancer.
      if (!masterServices.isBalancerOn() && !request.isDryRun()) {
        return responseBuilder.build();
      }

      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = rsGroupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0 && !request.isIgnoreRegionsInTransition()) {
        LOG.debug("Not running balancer because {} region(s) in transition: {}", groupRIT.size(),
          StringUtils.abbreviate(
            masterServices.getAssignmentManager().getRegionsInTransition().toString(), 256));
        return responseBuilder.build();
      }

      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): {}",
          serverManager.getDeadServers());
        return responseBuilder.build();
      }

      // We balance per group instead of per table
      Map<TableName, Map<ServerName, List<RegionInfo>>> assignmentsByTable =
        getRSGroupAssignmentsByTable(masterServices.getTableStateManager(), groupName);
      List<RegionPlan> plans = balancer.balanceCluster(assignmentsByTable);
      boolean balancerRan = !plans.isEmpty();

      responseBuilder.setBalancerRan(balancerRan).setMovesCalculated(plans.size());

      if (balancerRan && !request.isDryRun()) {
        LOG.info("RSGroup balance {} starting with plan count: {}", groupName, plans.size());
        List<RegionPlan> executed = masterServices.executeRegionPlansWithThrottling(plans);
        responseBuilder.setMovesExecuted(executed.size());
        LOG.info("RSGroup balance " + groupName + " completed");
      }

      return responseBuilder.build();
    }
  }

  private void moveTablesAndWait(Set<TableName> tables, String targetGroup) throws IOException {
    LOG.debug("Moving {} tables to target group {}", tables.size(), targetGroup);
    List<Long> procIds = new ArrayList<Long>();
    for (TableName tableName : tables) {
      TableDescriptor oldTd = masterServices.getTableDescriptors().get(tableName);
      if (oldTd == null) {
        continue;
      }
      TableDescriptor newTd =
        TableDescriptorBuilder.newBuilder(oldTd).setRegionServerGroup(targetGroup).build();
      procIds.add(
        masterServices.modifyTable(tableName, newTd, HConstants.NO_NONCE, HConstants.NO_NONCE));
    }
    for (long procId : procIds) {
      Procedure<?> proc = masterServices.getMasterProcedureExecutor().getProcedure(procId);
      if (proc == null) {
        continue;
      }
      ProcedureSyncWait.waitForProcedureToCompleteIOE(masterServices.getMasterProcedureExecutor(),
        proc, Long.MAX_VALUE);
    }
    LOG.info("Move tables done: moved {} tables to {}", tables.size(), targetGroup);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Tables moved to {}: {}", targetGroup, tables);
    }
  }

  @Override
  public void setRSGroup(Set<TableName> tables, String groupName) throws IOException {
    getRSGroupInfo(groupName);
    moveTablesAndWait(tables, groupName);
  }

  public void moveServers(Set<Address> servers, String targetGroupName) throws IOException {
    if (servers == null) {
      throw new ConstraintException("The list of servers to move cannot be null.");
    }
    if (servers.isEmpty()) {
      // For some reason this difference between null servers and isEmpty is important distinction.
      // TODO. Why? Stuff breaks if I equate them.
      return;
    }
    if (StringUtils.isEmpty(targetGroupName)) {
      throw new ConstraintException("RSGroup cannot be null.");
    }

    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (this) {
      // Presume first server's source group. Later ensure all servers are from this group.
      Address firstServer = servers.iterator().next();
      RSGroupInfo srcGrp = getRSGroupOfServer(firstServer);
      if (srcGrp == null) {
        // Be careful. This exception message is tested for in TestRSGroupAdmin2...
        throw new ConstraintException(
          "Server " + firstServer + " is either offline or it does not exist.");
      }

      // Only move online servers (when moving from 'default') or servers from other
      // groups. This prevents bogus servers from entering groups
      if (RSGroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        if (srcGrp.getServers().size() <= servers.size()) {
          throw new ConstraintException(KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE);
        }
        checkOnlineServersOnly(servers);
      }
      // Ensure all servers are of same rsgroup.
      for (Address server : servers) {
        String tmpGroup = getRSGroupOfServer(server).getName();
        if (!tmpGroup.equals(srcGrp.getName())) {
          throw new ConstraintException("Move server request should only come from one source "
            + "RSGroup. Expecting only " + srcGrp.getName() + " but contains " + tmpGroup);
        }
      }
      if (srcGrp.getServers().size() <= servers.size()) {
        // check if there are still tables reference this group
        for (TableDescriptor td : masterServices.getTableDescriptors().getAll().values()) {
          Optional<String> optGroupName = td.getRegionServerGroup();
          if (optGroupName.isPresent() && optGroupName.get().equals(srcGrp.getName())) {
            throw new ConstraintException(
              "Cannot leave a RSGroup " + srcGrp.getName() + " that contains tables('"
                + td.getTableName() + "' at least) without servers to host them.");
          }
        }
      }

      // MovedServers may be < passed in 'servers'.
      Set<Address> movedServers = moveServers(servers, srcGrp.getName(), targetGroupName);
      // moveServers (above) no longer mutates srcGrp in place -- re-fetch to see the post-move
      // remaining server set.
      RSGroupInfo refreshedSrcGrp = getRSGroupInfo(srcGrp.getName());
      moveServerRegionsFromGroup(movedServers, refreshedSrcGrp.getServers(), targetGroupName,
        srcGrp.getName());
      LOG.info("Move servers done: moved {} servers from {} to {}", movedServers.size(),
        srcGrp.getName(), targetGroupName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Servers moved from {} to {}: {}", srcGrp.getName(), targetGroupName,
          movedServers);
      }
    }
  }

  @Override
  public String determineRSGroupInfoForTable(TableName tableName) {
    return script.getRSGroup(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
  }

  @Override
  public synchronized void renameRSGroup(String oldName, String newName) throws IOException {
    if (oldName.equals(RSGroupInfo.DEFAULT_GROUP)) {
      throw new ConstraintException(RSGroupInfo.DEFAULT_GROUP + " can't be rename");
    }
    checkGroupName(newName);
    // getRSGroupInfo validates old RSGroup existence.
    RSGroupInfo oldRSG = getRSGroupInfo(oldName);
    Map<String, RSGroupInfo> rsGroupMap = holder.groupName2Group;
    if (rsGroupMap.containsKey(newName)) {
      throw new ConstraintException("Group already exists: " + newName);
    }

    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(oldRSG.getName());
    RSGroupInfo newRSG = new RSGroupInfo(newName, oldRSG.getServers());
    newGroupMap.put(newName, newRSG);
    flushConfig(newGroupMap);
    Set<TableName> updateTables = masterServices.getTableDescriptors().getAll().values().stream()
      .filter(t -> oldName.equals(t.getRegionServerGroup().orElse(null)))
      .map(TableDescriptor::getTableName).collect(Collectors.toSet());
    setRSGroup(updateTables, newName);
    LOG.info("Rename RSGroup done: {} => {}", oldName, newName);
  }

  @Override
  public synchronized void updateRSGroupConfig(String groupName, Map<String, String> configuration)
    throws IOException {
    if (RSGroupInfo.DEFAULT_GROUP.equals(groupName)) {
      // We do not persist anything of default group, therefore, it is not supported to update
      // default group's configuration which lost once master down.
      throw new ConstraintException(
        "configuration of " + RSGroupInfo.DEFAULT_GROUP + " can't be stored persistently");
    }
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    rsGroupInfo.getConfiguration().forEach((k, v) -> rsGroupInfo.removeConfiguration(k));
    configuration.forEach((k, v) -> rsGroupInfo.setConfiguration(k, v));
    flushConfig();
  }
}
