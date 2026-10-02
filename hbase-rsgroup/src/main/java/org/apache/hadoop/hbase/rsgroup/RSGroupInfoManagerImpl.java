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

import com.google.protobuf.ServiceException;
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
import java.util.NavigableSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

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
        LOG.error("Failed to get RSGroup from script for table {}:{}", namespace, tablename, e);
        return null;
      }
      return rsgroupMappingScript.getOutput().trim();
    }
  }

  static final String RS_GROUP_REGEX_PREFIX = "hbase.rsgroup.regex.";
  private static final Pattern GROUP_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

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
    if (
      rsGroupMap.get(rsGroupInfo.getName()) != null
        || rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)
    ) {
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
    // Mutate copies, not the live rsGroupMap entries -- flushConfig() below can still reject
    // this change (e.g. the regex-membership invariant), and the live state must stay untouched
    // until the change is actually persisted.
    RSGroupInfo src = new RSGroupInfo(getRSGroupInfo(srcGroup));
    RSGroupInfo dst = new RSGroupInfo(getRSGroupInfo(dstGroup));
    Set<Address> movedServers = new HashSet<>();
    // If destination is 'default' rsgroup, only add servers that are online. If not online, drop
    // it. If not 'default' group, add server to 'dst' rsgroup EVEN IF IT IS NOT online (could be a
    // rsgroup of dead servers that are to come back later).
    Set<Address> onlineServers = dst.getName().equals(RSGroupInfo.DEFAULT_GROUP)
      ? getOnlineServers(this.masterServices)
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
    // get server's group; mutate copies, not the live rsGroupMap entries -- flushConfig() below
    // can still reject this change, and the live state must stay untouched until persisted.
    RSGroupInfo srcGroupInfo = new RSGroupInfo(getRSGroupInfo(srcGroup));
    RSGroupInfo dstGroupInfo = new RSGroupInfo(getRSGroupInfo(dstGroup));

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
          // Mutate a copy, not the live rsGroupMap entry -- flushConfig() below can still
          // reject this change, and the live state must stay untouched until persisted.
          newRsGroupInfo = new RSGroupInfo(rsGroupInfo);
          newRsGroupInfo.removeServer(el);
          rsGroupInfos.put(newRsGroupInfo.getName(), newRsGroupInfo);
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

    Map<String, RSGroupInfo> newGroupMap = Maps.newHashMap(rsGroupMap);
    newGroupMap.remove(oldName);
    RSGroupInfo newGroup =
      new RSGroupInfo(newName, (SortedSet<Address>) oldGroup.getServers(), oldGroup.getTables());
    newGroupMap.put(newName, newGroup);
    flushConfig(newGroupMap);
  }

  /**
   * Will try to get the rsgroup from {@code tableMap} first then try to get the rsgroup from
   * {@code script} try to get the rsgroup from the {@link NamespaceDescriptor} lastly. If still not
   * present, return default group.
   */
  @Override
  public RSGroupInfo determineRSGroupInfoForTable(TableName tableName) throws IOException {
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
        LOG.info("Can not get the namespace rs group config for meta table, since the"
          + " meta table is not online yet, will use default group to assign meta first");
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
      throw new ConstraintException(
        "configuration of " + RSGroupInfo.DEFAULT_GROUP + " can't be stored persistently");
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
    LOG.info("Refreshing RSGroup info from source of truth: forceOnline={}, isOnline={}",
      forceOnline, isOnline());
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

    // Replace the 'default' rsgroup loaded from the group table or zk with a freshly built
    // one -- its server membership is always recomputed, never trusted from storage.
    groupList.removeIf(group -> group.getName().equals(RSGroupInfo.DEFAULT_GROUP));
    groupList.add(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, new TreeSet<Address>(), orphanTables));

    // populate the data
    HashMap<String, RSGroupInfo> newGroupMap = Maps.newHashMap();
    for (RSGroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
    }
    // Server membership for 'default' and for every regex-governed group is always recomputed
    // on the fly -- never trusted from storage -- exactly like 'default' membership already was.
    applyAutoManagedRSGroupServers(newGroupMap, computeAutoManagedRSGroupServers(groupList));

    HashMap<TableName, String> newTableMap = Maps.newHashMap();
    for (RSGroupInfo group : newGroupMap.values()) {
      for (TableName table : group.getTables()) {
        newTableMap.put(table, group.getName());
      }
    }
    resetRSGroupAndTableMaps(newGroupMap, newTableMap);
    updateCacheOfRSGroups(rsGroupMap.keySet());
    LOG.info("Refresh completed successfully");
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
    flushConfig(this.rsGroupMap, false);
  }

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap) throws IOException {
    flushConfig(newGroupMap, false);
  }

  private synchronized void flushConfig(Map<String, RSGroupInfo> newGroupMap,
    boolean isAutoRegexUpdate) throws IOException {
    checkRegexBasedRSGroupMembership(newGroupMap);

    Map<TableName, String> newTableMap;

    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for servers in default group
    if (!isOnline()) {
      if (newGroupMap == this.rsGroupMap) {
        // When newGroupMap is this.rsGroupMap itself,
        // do not need to check default group and other groups as followed
        return;
      }

      if (isAutoRegexUpdate) {
        checkOnlyServerSetsDifferForAutoUpdate(newGroupMap);
      } else {
        Map<String, RSGroupInfo> oldGroupMap = Maps.newHashMap(rsGroupMap);
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
      rsGroupMap = newGroupMap;

      // Do not need to update tableMap
      // because only server-set updates are allowed above,
      // or an IOException will be thrown
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

  private void checkRegexBasedRSGroupMembership(Map<String, RSGroupInfo> newGroupMap)
    throws IOException {
    LOG.info("Validating regex-based RSGroup membership");
    Map<String, Pattern> rsGroupNameToPatternMap =
      getRegexGroupMap(masterServices.getConfiguration());
    if (rsGroupNameToPatternMap.isEmpty()) {
      LOG.info("No regex-based RSGroup membership config found");
      return;
    }
    Set<Address> onlineServers = getOnlineServers(masterServices);
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
              && rsGroupMap.containsKey(expectedGroupName)
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

  private void checkOnlyServerSetsDifferForAutoUpdate(Map<String, RSGroupInfo> newGroupMap)
    throws IOException {
    if (!this.rsGroupMap.keySet().equals(newGroupMap.keySet())) {
      throw new IOException("Automatic regex-based RSGroup update must not add/remove RSGroups");
    }
    for (Map.Entry<String, RSGroupInfo> entry : newGroupMap.entrySet()) {
      String groupName = entry.getKey();
      RSGroupInfo oldInfo = this.rsGroupMap.get(groupName);
      RSGroupInfo newInfo = entry.getValue();
      if (
        !oldInfo.getTables().equals(newInfo.getTables())
          || !oldInfo.getConfiguration().equals(newInfo.getConfiguration())
      ) {
        throw new IOException("Automatic regex-based RSGroup update must not change tables or "
          + "configuration (RSGroup '" + groupName + "')");
      }
    }
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

  private Map<String, SortedSet<Address>>
    computeAutoManagedRSGroupServers(Collection<RSGroupInfo> existingGroups) {
    LOG.info("Computing auto-managed RSGroup server membership.");
    Set<Address> onlineServers = getOnlineServers(masterServices);
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

  private synchronized void updateAutoManagedRSGroupServers() throws IOException {
    LOG.info("Updating auto-managed RSGroup servers.");
    Map<String, RSGroupInfo> currentGroups = rsGroupMap;
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
   * Calls {@link RSGroupInfoManagerImpl#updateAutoManagedRSGroupServers()} to update list of known
   * servers. Notifications about server changes are received by registering {@link ServerListener}.
   * As a listener, we need to return immediately, so the real work of updating the servers is done
   * asynchronously in this thread.
   */
  private class ServerEventsListenerThread extends Thread implements ServerListener {
    private final Logger LOG = LoggerFactory.getLogger(ServerEventsListenerThread.class);
    private int eventCount = 0;

    ServerEventsListenerThread() {
      setDaemon(true);
    }

    @Override
    public void serverAdded(ServerName serverName) {
      LOG.info("Server added: {}", serverName);
      serverChanged();
    }

    @Override
    public void serverRemoved(ServerName serverName) {
      LOG.info("Server removed: {}", serverName);
      serverChanged();
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
          RSGroupInfoManagerImpl.this.updateAutoManagedRSGroupServers();
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
    if (!GROUP_NAME_PATTERN.matcher(groupName).matches()) {
      throw new ConstraintException("RSGroup name should only contain alphanumeric characters");
    }
  }
}
