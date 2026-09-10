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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.TableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@InterfaceAudience.Private
public class OfflineTableReplicationQueueStorage implements ReplicationQueueStorage {

  private final Map<ReplicationQueueId, Map<String, ReplicationGroupOffset>> offsets =
    new HashMap<>();

  private final Map<String, Map<String, Long>> lastSequenceIds = new HashMap<>();

  private final Map<String, Set<String>> hfileRefs = new HashMap<>();

  private void loadRegionInfo(FileSystem fs, Path regionDir,
    NavigableMap<byte[], RegionInfo> startKey2RegionInfo) throws IOException {
    RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    // TODO: we consider that the there will not be too many regions for hbase:replication table, so
    // here we just iterate over all the regions to find out the overlapped ones. Can be optimized
    // later.
    Iterator<Map.Entry<byte[], RegionInfo>> iter = startKey2RegionInfo.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<byte[], RegionInfo> entry = iter.next();
      if (hri.isOverlap(entry.getValue())) {
        if (hri.getRegionId() > entry.getValue().getRegionId()) {
          // we are newer, remove the old hri, we can not break here as if hri is a merged region,
          // we need to remove all its parent regions.
          iter.remove();
        } else {
          // we are older, just return, skip the below add
          return;
        }
      }

    }
    startKey2RegionInfo.put(hri.getStartKey(), hri);
  }

  private void loadOffsets(Result result) {
    NavigableMap<byte[], byte[]> map =
      result.getFamilyMap(TableReplicationQueueStorage.QUEUE_FAMILY);
    if (map == null || map.isEmpty()) {
      return;
    }
    Map<String, ReplicationGroupOffset> offsetMap = new HashMap<>();
    map.forEach((k, v) -> {
      String walGroup = Bytes.toString(k);
      ReplicationGroupOffset offset = ReplicationGroupOffset.parse(Bytes.toString(v));
      offsetMap.put(walGroup, offset);
    });
    ReplicationQueueId queueId = ReplicationQueueId.parse(Bytes.toString(result.getRow()));
    offsets.put(queueId, offsetMap);
  }

  private void loadLastSequenceIds(Result result) {
    NavigableMap<byte[], byte[]> map =
      result.getFamilyMap(TableReplicationQueueStorage.LAST_SEQUENCE_ID_FAMILY);
    if (map == null || map.isEmpty()) {
      return;
    }
    Map<String, Long> lastSeqIdMap = new HashMap<>();
    map.forEach((k, v) -> {
      String encodedRegionName = Bytes.toString(k);
      long lastSeqId = Bytes.toLong(v);
      lastSeqIdMap.put(encodedRegionName, lastSeqId);
    });
    String peerId = Bytes.toString(result.getRow());
    lastSequenceIds.put(peerId, lastSeqIdMap);
  }

  private void loadHFileRefs(Result result) {
    NavigableMap<byte[], byte[]> map =
      result.getFamilyMap(TableReplicationQueueStorage.HFILE_REF_FAMILY);
    if (map == null || map.isEmpty()) {
      return;
    }
    Set<String> refs = new HashSet<>();
    map.keySet().forEach(ref -> refs.add(Bytes.toString(ref)));
    String peerId = Bytes.toString(result.getRow());
    hfileRefs.put(peerId, refs);
  }

  private void loadReplicationQueueData(Configuration conf, TableName tableName)
    throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    FileSystem fs = tableDir.getFileSystem(conf);
    FileStatus[] regionDirs =
      CommonFSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) {
      return;
    }
    NavigableMap<byte[], RegionInfo> startKey2RegionInfo = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (FileStatus regionDir : regionDirs) {
      loadRegionInfo(fs, regionDir.getPath(), startKey2RegionInfo);
    }
    TableDescriptor td = ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName);
    for (RegionInfo hri : startKey2RegionInfo.values()) {
      try (ClientSideRegionScanner scanner =
        new ClientSideRegionScanner(conf, fs, rootDir, td, hri, new Scan(), null)) {
        for (;;) {
          Result result = scanner.next();
          if (result == null) {
            break;
          }
          loadOffsets(result);
          loadLastSequenceIds(result);
          loadHFileRefs(result);
        }
      }
    }
  }

  public OfflineTableReplicationQueueStorage(Configuration conf, TableName tableName)
    throws IOException {
    loadReplicationQueueData(conf, tableName);
  }

  @Override
  public synchronized void setOffset(ReplicationQueueId queueId, String walGroup,
    ReplicationGroupOffset offset, Map<String, Long> lastSeqIds) throws ReplicationException {
    Map<String, ReplicationGroupOffset> offsetMap = offsets.get(queueId);
    if (offsetMap == null) {
      offsetMap = new HashMap<>();
      offsets.put(queueId, offsetMap);
    }
    offsetMap.put(walGroup, offset);
    Map<String, Long> lastSeqIdsMap = lastSequenceIds.get(queueId.getPeerId());
    if (lastSeqIdsMap == null) {
      lastSeqIdsMap = new HashMap<>();
      lastSequenceIds.put(queueId.getPeerId(), lastSeqIdsMap);
    }
    for (Map.Entry<String, Long> entry : lastSeqIds.entrySet()) {
      Long oldSeqId = lastSeqIdsMap.get(entry.getKey());
      if (oldSeqId == null || oldSeqId < entry.getValue()) {
        lastSeqIdsMap.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public synchronized Map<String, ReplicationGroupOffset> getOffsets(ReplicationQueueId queueId)
    throws ReplicationException {
    Map<String, ReplicationGroupOffset> offsetMap = offsets.get(queueId);
    if (offsetMap == null) {
      return Collections.emptyMap();
    }
    return ImmutableMap.copyOf(offsetMap);
  }

  @Override
  public List<String> listAllPeerIds() throws ReplicationException {
    return offsets.keySet().stream().map(ReplicationQueueId::getPeerId).distinct()
      .collect(Collectors.toList());
  }

  @Override
  public synchronized List<ReplicationQueueId> listAllQueueIds(String peerId)
    throws ReplicationException {
    return offsets.keySet().stream().filter(rqi -> rqi.getPeerId().equals(peerId))
      .collect(Collectors.toList());
  }

  @Override
  public synchronized List<ReplicationQueueId> listAllQueueIds(ServerName serverName)
    throws ReplicationException {
    return offsets.keySet().stream().filter(rqi -> rqi.getServerName().equals(serverName))
      .collect(Collectors.toList());
  }

  @Override
  public synchronized List<ReplicationQueueId> listAllQueueIds(String peerId, ServerName serverName)
    throws ReplicationException {
    return offsets.keySet().stream()
      .filter(rqi -> rqi.getPeerId().equals(peerId) && rqi.getServerName().equals(serverName))
      .collect(Collectors.toList());
  }

  @Override
  public synchronized List<ReplicationQueueData> listAllQueues() throws ReplicationException {
    return offsets.entrySet().stream()
      .map(e -> new ReplicationQueueData(e.getKey(), ImmutableMap.copyOf(e.getValue())))
      .collect(Collectors.toList());
  }

  @Override
  public synchronized List<ServerName> listAllReplicators() throws ReplicationException {
    return offsets.keySet().stream().map(ReplicationQueueId::getServerName).distinct()
      .collect(Collectors.toList());
  }

  @Override
  public synchronized Map<String, ReplicationGroupOffset> claimQueue(ReplicationQueueId queueId,
    ServerName targetServerName) throws ReplicationException {
    Map<String, ReplicationGroupOffset> offsetMap = offsets.remove(queueId);
    if (offsetMap == null) {
      return Collections.emptyMap();
    }
    offsets.put(queueId.claim(targetServerName), offsetMap);
    return ImmutableMap.copyOf(offsetMap);
  }

  @Override
  public synchronized void removeQueue(ReplicationQueueId queueId) throws ReplicationException {
    offsets.remove(queueId);
  }

  @Override
  public synchronized void removeAllQueues(String peerId) throws ReplicationException {
    Iterator<ReplicationQueueId> iter = offsets.keySet().iterator();
    while (iter.hasNext()) {
      if (iter.next().getPeerId().equals(peerId)) {
        iter.remove();
      }
    }
  }

  @Override
  public synchronized long getLastSequenceId(String encodedRegionName, String peerId)
    throws ReplicationException {
    Map<String, Long> lastSeqIdMap = lastSequenceIds.get(peerId);
    if (lastSeqIdMap == null) {
      return HConstants.NO_SEQNUM;
    }
    Long lastSeqId = lastSeqIdMap.get(encodedRegionName);
    return lastSeqId != null ? lastSeqId.longValue() : HConstants.NO_SEQNUM;
  }

  @Override
  public synchronized void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds)
    throws ReplicationException {
    Map<String, Long> lastSeqIdMap = lastSequenceIds.get(peerId);
    if (lastSeqIdMap == null) {
      lastSeqIdMap = new HashMap<>();
      lastSequenceIds.put(peerId, lastSeqIdMap);
    }
    lastSeqIdMap.putAll(lastSeqIds);
  }

  @Override
  public synchronized void removeLastSequenceIds(String peerId) throws ReplicationException {
    lastSequenceIds.remove(peerId);
  }

  @Override
  public synchronized void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
    throws ReplicationException {
    Map<String, Long> lastSeqIdMap = lastSequenceIds.get(peerId);
    if (lastSeqIdMap == null) {
      return;
    }
    for (String encodedRegionName : encodedRegionNames) {
      lastSeqIdMap.remove(encodedRegionName);
    }
  }

  @Override
  public synchronized void removePeerFromHFileRefs(String peerId) throws ReplicationException {
    hfileRefs.remove(peerId);
  }

  @Override
  public synchronized void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
    throws ReplicationException {
    Set<String> refs = hfileRefs.get(peerId);
    if (refs == null) {
      refs = new HashSet<>();
      hfileRefs.put(peerId, refs);
    }
    for (Pair<Path, Path> pair : pairs) {
      refs.add(pair.getSecond().getName());
    }
  }

  @Override
  public synchronized void removeHFileRefs(String peerId, List<String> files)
    throws ReplicationException {
    Set<String> refs = hfileRefs.get(peerId);
    if (refs == null) {
      return;
    }
    refs.removeAll(files);
  }

  @Override
  public synchronized List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    return ImmutableList.copyOf(hfileRefs.keySet());
  }

  @Override
  public synchronized List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    Set<String> refs = hfileRefs.get(peerId);
    if (refs == null) {
      return Collections.emptyList();
    }
    return ImmutableList.copyOf(refs);
  }

  @Override
  public synchronized Set<String> getAllHFileRefs() throws ReplicationException {
    return hfileRefs.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
  }

  @Override
  public boolean hasData() throws ReplicationException {
    return true;
  }

  @Override
  public void batchUpdateQueues(ServerName serverName, List<ReplicationQueueData> datas)
    throws ReplicationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void batchUpdateLastSequenceIds(
    List<ZKReplicationQueueStorageForMigration.ZkLastPushedSeqId> lastPushedSeqIds)
    throws ReplicationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void batchUpdateHFileRefs(String peerId, List<String> hfileRefs)
    throws ReplicationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeLastSequenceIdsAndHFileRefsBefore(long ts) throws ReplicationException {
    throw new UnsupportedOperationException();
  }
}
