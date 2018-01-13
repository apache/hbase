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
package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * Helper for TableCFs Operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class ReplicationPeerConfigUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPeerConfigUtil.class);

  private ReplicationPeerConfigUtil() {}

  public static String convertToString(Set<String> namespaces) {
    if (namespaces == null) {
      return null;
    }
    return StringUtils.join(namespaces, ';');
  }

  /** convert map to TableCFs Object */
  public static ReplicationProtos.TableCF[] convert(
      Map<TableName, ? extends Collection<String>> tableCfs) {
    if (tableCfs == null) {
      return null;
    }
    List<ReplicationProtos.TableCF> tableCFList = new ArrayList<>(tableCfs.entrySet().size());
    ReplicationProtos.TableCF.Builder tableCFBuilder =  ReplicationProtos.TableCF.newBuilder();
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      tableCFBuilder.clear();
      tableCFBuilder.setTableName(ProtobufUtil.toProtoTableName(entry.getKey()));
      Collection<String> v = entry.getValue();
      if (v != null && !v.isEmpty()) {
        for (String value : entry.getValue()) {
          tableCFBuilder.addFamilies(ByteString.copyFromUtf8(value));
        }
      }
      tableCFList.add(tableCFBuilder.build());
    }
    return tableCFList.toArray(new ReplicationProtos.TableCF[tableCFList.size()]);
  }

  public static String convertToString(Map<TableName, ? extends Collection<String>> tableCfs) {
    if (tableCfs == null) {
      return null;
    }
    return convert(convert(tableCfs));
  }

  /**
   *  Convert string to TableCFs Object.
   *  This is only for read TableCFs information from TableCF node.
   *  Input String Format: ns1.table1:cf1,cf2;ns2.table2:cfA,cfB;ns3.table3.
   * */
  public static ReplicationProtos.TableCF[] convert(String tableCFsConfig) {
    if (tableCFsConfig == null || tableCFsConfig.trim().length() == 0) {
      return null;
    }

    ReplicationProtos.TableCF.Builder tableCFBuilder = ReplicationProtos.TableCF.newBuilder();
    String[] tables = tableCFsConfig.split(";");
    List<ReplicationProtos.TableCF> tableCFList = new ArrayList<>(tables.length);

    for (String tab : tables) {
      // 1 ignore empty table config
      tab = tab.trim();
      if (tab.length() == 0) {
        continue;
      }
      // 2 split to "table" and "cf1,cf2"
      //   for each table: "table#cf1,cf2" or "table"
      String[] pair = tab.split(":");
      String tabName = pair[0].trim();
      if (pair.length > 2 || tabName.length() == 0) {
        LOG.info("incorrect format:" + tableCFsConfig);
        continue;
      }

      tableCFBuilder.clear();
      // split namespace from tableName
      String ns = "default";
      String tName = tabName;
      String[] dbs = tabName.split("\\.");
      if (dbs != null && dbs.length == 2) {
        ns = dbs[0];
        tName = dbs[1];
      }
      tableCFBuilder.setTableName(
        ProtobufUtil.toProtoTableName(TableName.valueOf(ns, tName)));

      // 3 parse "cf1,cf2" part to List<cf>
      if (pair.length == 2) {
        String[] cfsList = pair[1].split(",");
        for (String cf : cfsList) {
          String cfName = cf.trim();
          if (cfName.length() > 0) {
            tableCFBuilder.addFamilies(ByteString.copyFromUtf8(cfName));
          }
        }
      }
      tableCFList.add(tableCFBuilder.build());
    }
    return tableCFList.toArray(new ReplicationProtos.TableCF[tableCFList.size()]);
  }

  /**
   *  Convert TableCFs Object to String.
   *  Output String Format: ns1.table1:cf1,cf2;ns2.table2:cfA,cfB;table3
   * */
  public static String convert(ReplicationProtos.TableCF[] tableCFs) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ReplicationProtos.TableCF tableCF = tableCFs[i];
      String namespace = tableCF.getTableName().getNamespace().toStringUtf8();
      if (StringUtils.isNotEmpty(namespace)) {
        sb.append(namespace).append(".").
            append(tableCF.getTableName().getQualifier().toStringUtf8())
            .append(":");
      } else {
        sb.append(tableCF.getTableName().toString()).append(":");
      }
      for (int j = 0; j < tableCF.getFamiliesCount(); j++) {
        sb.append(tableCF.getFamilies(j).toStringUtf8()).append(",");
      }
      sb.deleteCharAt(sb.length() - 1).append(";");
    }
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   *  Get TableCF in TableCFs, if not exist, return null.
   * */
  public static ReplicationProtos.TableCF getTableCF(ReplicationProtos.TableCF[] tableCFs,
                                           String table) {
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ReplicationProtos.TableCF tableCF = tableCFs[i];
      if (tableCF.getTableName().getQualifier().toStringUtf8().equals(table)) {
        return tableCF;
      }
    }
    return null;
  }

  /**
   *  Parse bytes into TableCFs.
   *  It is used for backward compatibility.
   *  Old format bytes have no PB_MAGIC Header
   * */
  public static ReplicationProtos.TableCF[] parseTableCFs(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    return ReplicationPeerConfigUtil.convert(Bytes.toString(bytes));
  }

  /**
   *  Convert tableCFs string into Map.
   * */
  public static Map<TableName, List<String>> parseTableCFsFromConfig(String tableCFsConfig) {
    ReplicationProtos.TableCF[] tableCFs = convert(tableCFsConfig);
    return convert2Map(tableCFs);
  }

  /**
   *  Convert tableCFs Object to Map.
   * */
  public static Map<TableName, List<String>> convert2Map(ReplicationProtos.TableCF[] tableCFs) {
    if (tableCFs == null || tableCFs.length == 0) {
      return null;
    }
    Map<TableName, List<String>> tableCFsMap = new HashMap<>();
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ReplicationProtos.TableCF tableCF = tableCFs[i];
      List<String> families = new ArrayList<>();
      for (int j = 0, m = tableCF.getFamiliesCount(); j < m; j++) {
        families.add(tableCF.getFamilies(j).toStringUtf8());
      }
      if (families.size() > 0) {
        tableCFsMap.put(ProtobufUtil.toTableName(tableCF.getTableName()), families);
      } else {
        tableCFsMap.put(ProtobufUtil.toTableName(tableCF.getTableName()), null);
      }
    }

    return tableCFsMap;
  }

  /**
   * @param bytes Content of a peer znode.
   * @return ClusterKey parsed from the passed bytes.
   * @throws DeserializationException
   */
  public static ReplicationPeerConfig parsePeerFrom(final byte[] bytes)
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pbLen = ProtobufUtil.lengthOfPBMagic();
      ReplicationProtos.ReplicationPeer.Builder builder =
          ReplicationProtos.ReplicationPeer.newBuilder();
      ReplicationProtos.ReplicationPeer peer;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pbLen, bytes.length - pbLen);
        peer = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return convert(peer);
    } else {
      if (bytes == null || bytes.length <= 0) {
        throw new DeserializationException("Bytes to deserialize should not be empty.");
      }
      return ReplicationPeerConfig.newBuilder().setClusterKey(Bytes.toString(bytes)).build();
    }
  }

  public static ReplicationPeerConfig convert(ReplicationProtos.ReplicationPeer peer) {
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder();
    if (peer.hasClusterkey()) {
      builder.setClusterKey(peer.getClusterkey());
    }
    if (peer.hasReplicationEndpointImpl()) {
      builder.setReplicationEndpointImpl(peer.getReplicationEndpointImpl());
    }

    for (HBaseProtos.BytesBytesPair pair : peer.getDataList()) {
      builder.putPeerData(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
    }

    for (HBaseProtos.NameStringPair pair : peer.getConfigurationList()) {
      builder.putConfiguration(pair.getName(), pair.getValue());
    }

    Map<TableName, List<String>> tableCFsMap = convert2Map(
      peer.getTableCfsList().toArray(new ReplicationProtos.TableCF[peer.getTableCfsCount()]));
    if (tableCFsMap != null) {
      builder.setTableCFsMap(tableCFsMap);
    }

    List<ByteString> namespacesList = peer.getNamespacesList();
    if (namespacesList != null && namespacesList.size() != 0) {
      builder.setNamespaces(
        namespacesList.stream().map(ByteString::toStringUtf8).collect(Collectors.toSet()));
    }

    if (peer.hasBandwidth()) {
      builder.setBandwidth(peer.getBandwidth());
    }

    if (peer.hasReplicateAll()) {
      builder.setReplicateAllUserTables(peer.getReplicateAll());
    }

    if (peer.hasSerial()) {
      builder.setSerial(peer.getSerial());
    }

    Map<TableName, List<String>> excludeTableCFsMap = convert2Map(peer.getExcludeTableCfsList()
        .toArray(new ReplicationProtos.TableCF[peer.getExcludeTableCfsCount()]));
    if (excludeTableCFsMap != null) {
      builder.setExcludeTableCFsMap(excludeTableCFsMap);
    }

    List<ByteString> excludeNamespacesList = peer.getExcludeNamespacesList();
    if (excludeNamespacesList != null && excludeNamespacesList.size() != 0) {
      builder.setExcludeNamespaces(
        excludeNamespacesList.stream().map(ByteString::toStringUtf8).collect(Collectors.toSet()));
    }

    if (peer.hasRemoteWALDir()) {
      builder.setRemoteWALDir(peer.getRemoteWALDir());
    }
    return builder.build();
  }

  public static ReplicationProtos.ReplicationPeer convert(ReplicationPeerConfig peerConfig) {
    ReplicationProtos.ReplicationPeer.Builder builder =
        ReplicationProtos.ReplicationPeer.newBuilder();
    if (peerConfig.getClusterKey() != null) {
      builder.setClusterkey(peerConfig.getClusterKey());
    }
    if (peerConfig.getReplicationEndpointImpl() != null) {
      builder.setReplicationEndpointImpl(peerConfig.getReplicationEndpointImpl());
    }

    for (Map.Entry<byte[], byte[]> entry : peerConfig.getPeerData().entrySet()) {
      builder.addData(HBaseProtos.BytesBytesPair.newBuilder()
          .setFirst(UnsafeByteOperations.unsafeWrap(entry.getKey()))
          .setSecond(UnsafeByteOperations.unsafeWrap(entry.getValue()))
          .build());
    }

    for (Map.Entry<String, String> entry : peerConfig.getConfiguration().entrySet()) {
      builder.addConfiguration(HBaseProtos.NameStringPair.newBuilder()
          .setName(entry.getKey())
          .setValue(entry.getValue())
          .build());
    }

    ReplicationProtos.TableCF[] tableCFs = convert(peerConfig.getTableCFsMap());
    if (tableCFs != null) {
      for (int i = 0; i < tableCFs.length; i++) {
        builder.addTableCfs(tableCFs[i]);
      }
    }
    Set<String> namespaces = peerConfig.getNamespaces();
    if (namespaces != null) {
      for (String namespace : namespaces) {
        builder.addNamespaces(ByteString.copyFromUtf8(namespace));
      }
    }

    builder.setBandwidth(peerConfig.getBandwidth());
    builder.setReplicateAll(peerConfig.replicateAllUserTables());
    builder.setSerial(peerConfig.isSerial());

    ReplicationProtos.TableCF[] excludeTableCFs = convert(peerConfig.getExcludeTableCFsMap());
    if (excludeTableCFs != null) {
      for (int i = 0; i < excludeTableCFs.length; i++) {
        builder.addExcludeTableCfs(excludeTableCFs[i]);
      }
    }
    Set<String> excludeNamespaces = peerConfig.getExcludeNamespaces();
    if (excludeNamespaces != null) {
      for (String namespace : excludeNamespaces) {
        builder.addExcludeNamespaces(ByteString.copyFromUtf8(namespace));
      }
    }

    if (peerConfig.getRemoteWALDir() != null) {
      builder.setRemoteWALDir(peerConfig.getRemoteWALDir());
    }
    return builder.build();
  }

  /**
   * @param peerConfig
   * @return Serialized protobuf of <code>peerConfig</code> with pb magic prefix prepended suitable
   *         for use as content of a this.peersZNode; i.e. the content of PEER_ID znode under
   *         /hbase/replication/peers/PEER_ID
   */
  public static byte[] toByteArray(final ReplicationPeerConfig peerConfig) {
    byte[] bytes = convert(peerConfig).toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  public static ReplicationPeerDescription toReplicationPeerDescription(
      ReplicationProtos.ReplicationPeerDescription desc) {
    boolean enabled = ReplicationProtos.ReplicationState.State.ENABLED == desc.getState()
        .getState();
    ReplicationPeerConfig config = convert(desc.getConfig());
    return new ReplicationPeerDescription(desc.getId(), enabled, config);
  }

  public static ReplicationProtos.ReplicationPeerDescription toProtoReplicationPeerDescription(
      ReplicationPeerDescription desc) {
    ReplicationProtos.ReplicationPeerDescription.Builder builder =
        ReplicationProtos.ReplicationPeerDescription.newBuilder();
    builder.setId(desc.getPeerId());
    ReplicationProtos.ReplicationState.Builder stateBuilder = ReplicationProtos.ReplicationState
        .newBuilder();
    stateBuilder.setState(desc.isEnabled() ? ReplicationProtos.ReplicationState.State.ENABLED
        : ReplicationProtos.ReplicationState.State.DISABLED);
    builder.setState(stateBuilder.build());
    builder.setConfig(convert(desc.getPeerConfig()));
    return builder.build();
  }

  public static ReplicationPeerConfig appendTableCFsToReplicationPeerConfig(
      Map<TableName, List<String>> tableCfs, ReplicationPeerConfig peerConfig) {
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder(peerConfig);
    Map<TableName, List<String>> preTableCfs = peerConfig.getTableCFsMap();
    if (preTableCfs == null) {
      builder.setTableCFsMap(tableCfs);
    } else {
      Map<TableName, List<String>> newTableCfs = copyTableCFsMap(preTableCfs);
      for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
        TableName table = entry.getKey();
        Collection<String> appendCfs = entry.getValue();
        if (newTableCfs.containsKey(table)) {
          List<String> cfs = newTableCfs.get(table);
          if (cfs == null || appendCfs == null || appendCfs.isEmpty()) {
            newTableCfs.put(table, null);
          } else {
            Set<String> cfSet = new HashSet<String>(cfs);
            cfSet.addAll(appendCfs);
            newTableCfs.put(table, Lists.newArrayList(cfSet));
          }
        } else {
          if (appendCfs == null || appendCfs.isEmpty()) {
            newTableCfs.put(table, null);
          } else {
            newTableCfs.put(table, Lists.newArrayList(appendCfs));
          }
        }
      }
      builder.setTableCFsMap(newTableCfs);
    }
    return builder.build();
  }

  private static Map<TableName, List<String>>
      copyTableCFsMap(Map<TableName, List<String>> preTableCfs) {
    Map<TableName, List<String>> newTableCfs = new HashMap<>();
    preTableCfs.forEach(
      (table, cfs) -> newTableCfs.put(table, cfs != null ? Lists.newArrayList(cfs) : null));
    return newTableCfs;
  }

  public static ReplicationPeerConfig removeTableCFsFromReplicationPeerConfig(
      Map<TableName, List<String>> tableCfs, ReplicationPeerConfig peerConfig,
      String id) throws ReplicationException {
    Map<TableName, List<String>> preTableCfs = peerConfig.getTableCFsMap();
    if (preTableCfs == null) {
      throw new ReplicationException("Table-Cfs for peer: " + id + " is null");
    }
    Map<TableName, List<String>> newTableCfs = copyTableCFsMap(preTableCfs);
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      TableName table = entry.getKey();
      Collection<String> removeCfs = entry.getValue();
      if (newTableCfs.containsKey(table)) {
        List<String> cfs = newTableCfs.get(table);
        if (cfs == null && (removeCfs == null || removeCfs.isEmpty())) {
          newTableCfs.remove(table);
        } else if (cfs != null && (removeCfs != null && !removeCfs.isEmpty())) {
          Set<String> cfSet = new HashSet<String>(cfs);
          cfSet.removeAll(removeCfs);
          if (cfSet.isEmpty()) {
            newTableCfs.remove(table);
          } else {
            newTableCfs.put(table, Lists.newArrayList(cfSet));
          }
        } else if (cfs == null && (removeCfs != null && !removeCfs.isEmpty())) {
          throw new ReplicationException("Cannot remove cf of table: " + table
              + " which doesn't specify cfs from table-cfs config in peer: " + id);
        } else if (cfs != null && (removeCfs == null || removeCfs.isEmpty())) {
          throw new ReplicationException("Cannot remove table: " + table
              + " which has specified cfs from table-cfs config in peer: " + id);
        }
      } else {
        throw new ReplicationException(
            "No table: " + table + " in table-cfs config of peer: " + id);
      }
    }
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder(peerConfig);
    builder.setTableCFsMap(newTableCfs);
    return builder.build();
  }

  /**
   * Returns the configuration needed to talk to the remote slave cluster.
   * @param conf the base configuration
   * @param peer the description of replication peer
   * @return the configuration for the peer cluster, null if it was unable to get the configuration
   * @throws IOException when create peer cluster configuration failed
   */
  public static Configuration getPeerClusterConfiguration(Configuration conf,
      ReplicationPeerDescription peer) throws IOException {
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    Configuration otherConf;
    try {
      otherConf = HBaseConfiguration.createClusterConf(conf, peerConfig.getClusterKey());
    } catch (IOException e) {
      throw new IOException("Can't get peer configuration for peerId=" + peer.getPeerId(), e);
    }

    if (!peerConfig.getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(otherConf);
      compound.addStringMap(peerConfig.getConfiguration());
      return compound;
    }

    return otherConf;
  }
}
