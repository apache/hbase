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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;

/**
 * Helper for TableCFs Operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class ReplicationSerDeHelper {

  private static final Log LOG = LogFactory.getLog(ReplicationSerDeHelper.class);

  private ReplicationSerDeHelper() {}

  public static String convertToString(Set<String> namespaces) {
    if (namespaces == null) {
      return null;
    }
    return StringUtils.join(namespaces, ';');
  }

  /** convert map to TableCFs Object */
  public static ZooKeeperProtos.TableCF[] convert(
      Map<TableName, ? extends Collection<String>> tableCfs) {
    if (tableCfs == null) {
      return null;
    }
    List<ZooKeeperProtos.TableCF> tableCFList = new ArrayList<>();
    ZooKeeperProtos.TableCF.Builder tableCFBuilder =  ZooKeeperProtos.TableCF.newBuilder();
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
    return tableCFList.toArray(new ZooKeeperProtos.TableCF[tableCFList.size()]);
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
  public static ZooKeeperProtos.TableCF[] convert(String tableCFsConfig) {
    if (tableCFsConfig == null || tableCFsConfig.trim().length() == 0) {
      return null;
    }
    List<ZooKeeperProtos.TableCF> tableCFList = new ArrayList<>();
    ZooKeeperProtos.TableCF.Builder tableCFBuilder = ZooKeeperProtos.TableCF.newBuilder();

    String[] tables = tableCFsConfig.split(";");
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
    return tableCFList.toArray(new ZooKeeperProtos.TableCF[tableCFList.size()]);
  }

  /**
   *  Convert TableCFs Object to String.
   *  Output String Format: ns1.table1:cf1,cf2;ns2.table2:cfA,cfB;table3
   * */
  public static String convert(ZooKeeperProtos.TableCF[] tableCFs) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ZooKeeperProtos.TableCF tableCF = tableCFs[i];
      String namespace = tableCF.getTableName().getNamespace().toStringUtf8();
      if (!Strings.isEmpty(namespace)) {
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
  public static ZooKeeperProtos.TableCF getTableCF(ZooKeeperProtos.TableCF[] tableCFs,
                                           String table) {
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ZooKeeperProtos.TableCF tableCF = tableCFs[i];
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
  public static ZooKeeperProtos.TableCF[] parseTableCFs(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    return ReplicationSerDeHelper.convert(Bytes.toString(bytes));
  }

  /**
   *  Convert tableCFs string into Map.
   * */
  public static Map<TableName, List<String>> parseTableCFsFromConfig(String tableCFsConfig) {
    ZooKeeperProtos.TableCF[] tableCFs = convert(tableCFsConfig);
    return convert2Map(tableCFs);
  }

  /**
   *  Convert tableCFs Object to Map.
   * */
  public static Map<TableName, List<String>> convert2Map(ZooKeeperProtos.TableCF[] tableCFs) {
    if (tableCFs == null || tableCFs.length == 0) {
      return null;
    }
    Map<TableName, List<String>> tableCFsMap = new HashMap<TableName, List<String>>();
    for (int i = 0, n = tableCFs.length; i < n; i++) {
      ZooKeeperProtos.TableCF tableCF = tableCFs[i];
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
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationPeer.Builder builder =
          ZooKeeperProtos.ReplicationPeer.newBuilder();
      ZooKeeperProtos.ReplicationPeer peer;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        peer = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return convert(peer);
    } else {
      if (bytes.length > 0) {
        return new ReplicationPeerConfig().setClusterKey(Bytes.toString(bytes));
      }
      return new ReplicationPeerConfig().setClusterKey("");
    }
  }

  public static ReplicationPeerConfig convert(ZooKeeperProtos.ReplicationPeer peer) {
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    if (peer.hasClusterkey()) {
      peerConfig.setClusterKey(peer.getClusterkey());
    }
    if (peer.hasReplicationEndpointImpl()) {
      peerConfig.setReplicationEndpointImpl(peer.getReplicationEndpointImpl());
    }

    for (HBaseProtos.BytesBytesPair pair : peer.getDataList()) {
      peerConfig.getPeerData().put(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
    }

    for (HBaseProtos.NameStringPair pair : peer.getConfigurationList()) {
      peerConfig.getConfiguration().put(pair.getName(), pair.getValue());
    }

    Map<TableName, ? extends Collection<String>> tableCFsMap = convert2Map(
      peer.getTableCfsList().toArray(new ZooKeeperProtos.TableCF[peer.getTableCfsCount()]));
    if (tableCFsMap != null) {
      peerConfig.setTableCFsMap(tableCFsMap);
    }
    List<ByteString> namespacesList = peer.getNamespacesList();
    if (namespacesList != null && namespacesList.size() != 0) {
      Set<String> namespaces = new HashSet<String>();
      for (ByteString namespace : namespacesList) {
        namespaces.add(namespace.toStringUtf8());
      }
      peerConfig.setNamespaces(namespaces);
    }
    return peerConfig;
  }

  public static ZooKeeperProtos.ReplicationPeer convert(ReplicationPeerConfig  peerConfig) {
    ZooKeeperProtos.ReplicationPeer.Builder builder = ZooKeeperProtos.ReplicationPeer.newBuilder();
    if (peerConfig.getClusterKey() != null) {
      builder.setClusterkey(peerConfig.getClusterKey());
    }
    if (peerConfig.getReplicationEndpointImpl() != null) {
      builder.setReplicationEndpointImpl(peerConfig.getReplicationEndpointImpl());
    }

    for (Map.Entry<byte[], byte[]> entry : peerConfig.getPeerData().entrySet()) {
      builder.addData(HBaseProtos.BytesBytesPair.newBuilder()
          .setFirst(ByteString.copyFrom(entry.getKey()))
          .setSecond(ByteString.copyFrom(entry.getValue()))
          .build());
    }

    for (Map.Entry<String, String> entry : peerConfig.getConfiguration().entrySet()) {
      builder.addConfiguration(HBaseProtos.NameStringPair.newBuilder()
          .setName(entry.getKey())
          .setValue(entry.getValue())
          .build());
    }

    ZooKeeperProtos.TableCF[] tableCFs = convert(peerConfig.getTableCFsMap());
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
}
