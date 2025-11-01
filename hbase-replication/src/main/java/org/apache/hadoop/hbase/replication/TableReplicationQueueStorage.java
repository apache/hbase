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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration.ZkLastPushedSeqId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos;

/**
 * HBase table based replication queue storage.
 */
@InterfaceAudience.Private
public class TableReplicationQueueStorage implements ReplicationQueueStorage {

  public static final byte[] QUEUE_FAMILY = Bytes.toBytes("queue");

  public static final byte[] LAST_SEQUENCE_ID_FAMILY = Bytes.toBytes("sid");

  public static final byte[] HFILE_REF_FAMILY = Bytes.toBytes("hfileref");

  private final Connection conn;

  private final TableName tableName;

  public TableReplicationQueueStorage(Connection conn, TableName tableName) {
    this.conn = conn;
    this.tableName = tableName;
  }

  private void addLastSeqIdsPut(MultiRowMutationProtos.MutateRowsRequest.Builder builder,
    String peerId, Map<String, Long> lastSeqIds, AsyncTable<?> table) throws IOException {
    // get the previous sequence ids first
    byte[] row = Bytes.toBytes(peerId);
    Get get = new Get(row);
    lastSeqIds.keySet().forEach(encodedRegionName -> get.addColumn(LAST_SEQUENCE_ID_FAMILY,
      Bytes.toBytes(encodedRegionName)));
    Result result = FutureUtils.get(table.get(get));
    Put put = new Put(row);
    for (Map.Entry<String, Long> entry : lastSeqIds.entrySet()) {
      String encodedRegionName = entry.getKey();
      long lastSeqId = entry.getValue();
      byte[] encodedRegionNameAsBytes = Bytes.toBytes(encodedRegionName);
      byte[] previousLastSeqIdAsBytes =
        result.getValue(LAST_SEQUENCE_ID_FAMILY, encodedRegionNameAsBytes);
      if (previousLastSeqIdAsBytes != null) {
        long previousLastSeqId = Bytes.toLong(previousLastSeqIdAsBytes);
        if (lastSeqId > previousLastSeqId) {
          // update last seq id when it is greater, and use CAS to make sure we do not overwrite
          // other's value.
          put.addColumn(LAST_SEQUENCE_ID_FAMILY, encodedRegionNameAsBytes,
            Bytes.toBytes(lastSeqId));
          builder.addCondition(ProtobufUtil.toCondition(row, LAST_SEQUENCE_ID_FAMILY,
            encodedRegionNameAsBytes, CompareOperator.EQUAL, previousLastSeqIdAsBytes, null));
        }
      } else {
        // also update last seq id when there is no value yet, and use CAS to make sure we do not
        // overwrite
        // other's value.
        put.addColumn(LAST_SEQUENCE_ID_FAMILY, encodedRegionNameAsBytes, Bytes.toBytes(lastSeqId));
        builder.addCondition(ProtobufUtil.toCondition(row, LAST_SEQUENCE_ID_FAMILY,
          encodedRegionNameAsBytes, CompareOperator.EQUAL, null, null));
      }
    }
    if (!put.isEmpty()) {
      builder.addMutationRequest(ProtobufUtil.toMutation(MutationType.PUT, put));
    }
  }

  @Override
  public void setOffset(ReplicationQueueId queueId, String walGroup, ReplicationGroupOffset offset,
    Map<String, Long> lastSeqIds) throws ReplicationException {
    Put put = new Put(Bytes.toBytes(queueId.toString())).addColumn(QUEUE_FAMILY,
      Bytes.toBytes(walGroup), Bytes.toBytes(offset.toString()));
    AsyncTable<?> asyncTable = conn.toAsyncConnection().getTable(tableName);
    try {
      if (lastSeqIds.isEmpty()) {
        FutureUtils.get(asyncTable.put(put));
      } else {
        for (;;) {
          MultiRowMutationProtos.MutateRowsRequest.Builder builder =
            MultiRowMutationProtos.MutateRowsRequest.newBuilder();
          addLastSeqIdsPut(builder, queueId.getPeerId(), lastSeqIds, asyncTable);
          if (builder.getMutationRequestCount() > 0) {
            // use MultiRowMutationService to atomically update offset and last sequence ids
            MultiRowMutationProtos.MutateRowsRequest request =
              builder.addMutationRequest(ProtobufUtil.toMutation(MutationType.PUT, put)).build();
            MultiRowMutationProtos.MutateRowsResponse responose =
              FutureUtils.get(asyncTable.<MultiRowMutationProtos.MultiRowMutationService.Interface,
                MultiRowMutationProtos.MutateRowsResponse> coprocessorService(
                  MultiRowMutationProtos.MultiRowMutationService::newStub,
                  (stub, controller, done) -> stub.mutateRows(controller, request, done),
                  put.getRow()));
            if (responose.getProcessed()) {
              break;
            }
          } else {
            // we do not need to update last seq id, fallback to single put
            FutureUtils.get(asyncTable.put(put));
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to setOffset, queueId=" + queueId + ", walGroup="
        + walGroup + ", offset=" + offset + ", lastSeqIds=" + lastSeqIds, e);
    }
  }

  private ImmutableMap<String, ReplicationGroupOffset> parseOffsets(Result result) {
    ImmutableMap.Builder<String, ReplicationGroupOffset> builder =
      ImmutableMap.builderWithExpectedSize(result.size());
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(QUEUE_FAMILY);
    if (map != null) {
      map.forEach((k, v) -> {
        String walGroup = Bytes.toString(k);
        ReplicationGroupOffset offset = ReplicationGroupOffset.parse(Bytes.toString(v));
        builder.put(walGroup, offset);
      });
    }
    return builder.build();
  }

  private Map<String, ReplicationGroupOffset> getOffsets0(Table table, ReplicationQueueId queueId)
    throws IOException {
    Result result = table.get(new Get(Bytes.toBytes(queueId.toString())).addFamily(QUEUE_FAMILY));
    return parseOffsets(result);
  }

  @Override
  public Map<String, ReplicationGroupOffset> getOffsets(ReplicationQueueId queueId)
    throws ReplicationException {
    try (Table table = conn.getTable(tableName)) {
      return getOffsets0(table, queueId);
    } catch (IOException e) {
      throw new ReplicationException("failed to getOffsets, queueId=" + queueId, e);
    }
  }

  private void listAllQueueIds(Table table, Scan scan, List<ReplicationQueueId> queueIds)
    throws IOException {
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        ReplicationQueueId queueId = ReplicationQueueId.parse(Bytes.toString(result.getRow()));
        queueIds.add(queueId);
      }
    }
  }

  private void listAllQueueIds(Table table, String peerId, ServerName serverName,
    List<ReplicationQueueId> queueIds) throws IOException {
    listAllQueueIds(table,
      new Scan().setStartStopRowForPrefixScan(ReplicationQueueId.getScanPrefix(serverName, peerId))
        .addFamily(QUEUE_FAMILY).setFilter(new KeyOnlyFilter()),
      queueIds);
  }

  @Override
  public List<String> listAllPeerIds() throws ReplicationException {
    List<String> peerIds = new ArrayList<>();
    try (Table table = conn.getTable(tableName)) {
      KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
      String previousPeerId = null;
      for (;;) {
        // first, get the next peerId
        Scan peerScan =
          new Scan().addFamily(QUEUE_FAMILY).setOneRowLimit().setFilter(keyOnlyFilter);
        if (previousPeerId != null) {
          peerScan.withStartRow(ReplicationQueueId.getScanStartRowForNextPeerId(previousPeerId));
        }
        String peerId;
        try (ResultScanner scanner = table.getScanner(peerScan)) {
          Result result = scanner.next();
          if (result == null) {
            // no more peers, break
            break;
          }
          peerId = ReplicationQueueId.getPeerId(Bytes.toString(result.getRow()));
        }
        peerIds.add(peerId);
        previousPeerId = peerId;
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to listAllPeerIds", e);
    }
    return peerIds;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(String peerId) throws ReplicationException {
    Scan scan = new Scan().setStartStopRowForPrefixScan(ReplicationQueueId.getScanPrefix(peerId))
      .addFamily(QUEUE_FAMILY).setFilter(new KeyOnlyFilter());
    List<ReplicationQueueId> queueIds = new ArrayList<>();
    try (Table table = conn.getTable(tableName)) {
      listAllQueueIds(table, scan, queueIds);
    } catch (IOException e) {
      throw new ReplicationException("failed to listAllQueueIds, peerId=" + peerId, e);
    }
    return queueIds;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(ServerName serverName)
    throws ReplicationException {
    List<ReplicationQueueId> queueIds = new ArrayList<>();
    try (Table table = conn.getTable(tableName)) {
      KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
      String previousPeerId = null;
      for (;;) {
        // first, get the next peerId
        Scan peerScan =
          new Scan().addFamily(QUEUE_FAMILY).setOneRowLimit().setFilter(keyOnlyFilter);
        if (previousPeerId != null) {
          peerScan.withStartRow(ReplicationQueueId.getScanStartRowForNextPeerId(previousPeerId));
        }
        String peerId;
        try (ResultScanner scanner = table.getScanner(peerScan)) {
          Result result = scanner.next();
          if (result == null) {
            // no more peers, break
            break;
          }
          peerId = ReplicationQueueId.getPeerId(Bytes.toString(result.getRow()));
        }
        listAllQueueIds(table, peerId, serverName, queueIds);
        previousPeerId = peerId;
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to listAllQueueIds, serverName=" + serverName, e);
    }
    return queueIds;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(String peerId, ServerName serverName)
    throws ReplicationException {
    List<ReplicationQueueId> queueIds = new ArrayList<>();
    try (Table table = conn.getTable(tableName)) {
      listAllQueueIds(table, peerId, serverName, queueIds);
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to listAllQueueIds, peerId=" + peerId + ", serverName=" + serverName, e);
    }
    return queueIds;
  }

  @Override
  public List<ReplicationQueueData> listAllQueues() throws ReplicationException {
    List<ReplicationQueueData> queues = new ArrayList<>();
    Scan scan = new Scan().addFamily(QUEUE_FAMILY).setReadType(ReadType.STREAM);
    try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        ReplicationQueueId queueId = ReplicationQueueId.parse(Bytes.toString(result.getRow()));
        ReplicationQueueData queueData = new ReplicationQueueData(queueId, parseOffsets(result));
        queues.add(queueData);
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to listAllQueues", e);
    }
    return queues;
  }

  @Override
  public List<ServerName> listAllReplicators() throws ReplicationException {
    Set<ServerName> replicators = new HashSet<>();
    Scan scan = new Scan().addFamily(QUEUE_FAMILY).setFilter(new KeyOnlyFilter())
      .setReadType(ReadType.STREAM);
    try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        ReplicationQueueId queueId = ReplicationQueueId.parse(Bytes.toString(result.getRow()));
        replicators.add(queueId.getServerName());
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to listAllReplicators", e);
    }
    return new ArrayList<>(replicators);
  }

  @Override
  public Map<String, ReplicationGroupOffset> claimQueue(ReplicationQueueId queueId,
    ServerName targetServerName) throws ReplicationException {
    ReplicationQueueId newQueueId = queueId.claim(targetServerName);
    byte[] coprocessorRow = ReplicationQueueId.getScanPrefix(queueId.getPeerId());
    AsyncTable<?> asyncTable = conn.toAsyncConnection().getTable(tableName);
    try (Table table = conn.getTable(tableName)) {
      for (;;) {
        Map<String, ReplicationGroupOffset> offsets = getOffsets0(table, queueId);
        if (offsets.isEmpty()) {
          return Collections.emptyMap();
        }
        Map.Entry<String, ReplicationGroupOffset> entry = offsets.entrySet().iterator().next();
        ClientProtos.Condition condition = ProtobufUtil.toCondition(
          Bytes.toBytes(queueId.toString()), QUEUE_FAMILY, Bytes.toBytes(entry.getKey()),
          CompareOperator.EQUAL, Bytes.toBytes(entry.getValue().toString()), null);
        Delete delete = new Delete(Bytes.toBytes(queueId.toString())).addFamily(QUEUE_FAMILY);
        Put put = new Put(Bytes.toBytes(newQueueId.toString()));
        offsets.forEach((walGroup, offset) -> put.addColumn(QUEUE_FAMILY, Bytes.toBytes(walGroup),
          Bytes.toBytes(offset.toString())));
        MultiRowMutationProtos.MutateRowsRequest request =
          MultiRowMutationProtos.MutateRowsRequest.newBuilder().addCondition(condition)
            .addMutationRequest(ProtobufUtil.toMutation(MutationType.DELETE, delete))
            .addMutationRequest(ProtobufUtil.toMutation(MutationType.PUT, put)).build();
        MultiRowMutationProtos.MutateRowsResponse resp =
          FutureUtils.get(asyncTable.<MultiRowMutationProtos.MultiRowMutationService.Interface,
            MultiRowMutationProtos.MutateRowsResponse> coprocessorService(
              MultiRowMutationProtos.MultiRowMutationService::newStub,
              (stub, controller, done) -> stub.mutateRows(controller, request, done),
              coprocessorRow));
        if (resp.getProcessed()) {
          return offsets;
        }
        // if the multi is not processed, which usually the queue has already been claimed by
        // others, for safety, let's try claiming again, usually the next get operation above will
        // return an empty map and we will quit the loop.
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to claimQueue, queueId=" + queueId + ", targetServerName=" + targetServerName, e);
    }
  }

  @Override
  public void removeQueue(ReplicationQueueId queueId) throws ReplicationException {
    try (Table table = conn.getTable(tableName)) {
      table.delete(new Delete(Bytes.toBytes(queueId.toString())).addFamily(QUEUE_FAMILY));
    } catch (IOException e) {
      throw new ReplicationException("failed to removeQueue, queueId=" + queueId, e);
    }
  }

  @Override
  public void removeAllQueues(String peerId) throws ReplicationException {
    Scan scan = new Scan().setStartStopRowForPrefixScan(ReplicationQueueId.getScanPrefix(peerId))
      .addFamily(QUEUE_FAMILY).setFilter(new KeyOnlyFilter());
    try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        table.delete(new Delete(result.getRow()));
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to removeAllQueues, peerId=" + peerId, e);
    }
  }

  @Override
  public long getLastSequenceId(String encodedRegionName, String peerId)
    throws ReplicationException {
    byte[] qual = Bytes.toBytes(encodedRegionName);
    try (Table table = conn.getTable(tableName)) {
      Result result =
        table.get(new Get(Bytes.toBytes(peerId)).addColumn(LAST_SEQUENCE_ID_FAMILY, qual));
      byte[] lastSeqId = result.getValue(LAST_SEQUENCE_ID_FAMILY, qual);
      return lastSeqId != null ? Bytes.toLong(lastSeqId) : HConstants.NO_SEQNUM;
    } catch (IOException e) {
      throw new ReplicationException("failed to getLastSequenceId, encodedRegionName="
        + encodedRegionName + ", peerId=" + peerId, e);
    }
  }

  @Override
  public void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds)
    throws ReplicationException {
    // No need CAS and retry here, because it'll call setLastSequenceIds() for disabled peers
    // only, so no conflict happen.
    Put put = new Put(Bytes.toBytes(peerId));
    lastSeqIds.forEach((encodedRegionName, lastSeqId) -> put.addColumn(LAST_SEQUENCE_ID_FAMILY,
      Bytes.toBytes(encodedRegionName), Bytes.toBytes(lastSeqId)));
    try (Table table = conn.getTable(tableName)) {
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to setLastSequenceIds, peerId=" + peerId + ", lastSeqIds=" + lastSeqIds, e);
    }
  }

  @Override
  public void removeLastSequenceIds(String peerId) throws ReplicationException {
    Delete delete = new Delete(Bytes.toBytes(peerId)).addFamily(LAST_SEQUENCE_ID_FAMILY);
    try (Table table = conn.getTable(tableName)) {
      table.delete(delete);
    } catch (IOException e) {
      throw new ReplicationException("failed to removeLastSequenceIds, peerId=" + peerId, e);
    }
  }

  @Override
  public void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
    throws ReplicationException {
    Delete delete = new Delete(Bytes.toBytes(peerId));
    encodedRegionNames.forEach(n -> delete.addColumns(LAST_SEQUENCE_ID_FAMILY, Bytes.toBytes(n)));
    try (Table table = conn.getTable(tableName)) {
      table.delete(delete);
    } catch (IOException e) {
      throw new ReplicationException("failed to removeLastSequenceIds, peerId=" + peerId
        + ", encodedRegionNames=" + encodedRegionNames, e);
    }
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) throws ReplicationException {
    try (Table table = conn.getTable(tableName)) {
      table.delete(new Delete(Bytes.toBytes(peerId)).addFamily(HFILE_REF_FAMILY));
    } catch (IOException e) {
      throw new ReplicationException("failed to removePeerFromHFileRefs, peerId=" + peerId, e);
    }
  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
    throws ReplicationException {
    Put put = new Put(Bytes.toBytes(peerId));
    pairs.forEach(p -> put.addColumn(HFILE_REF_FAMILY, Bytes.toBytes(p.getSecond().getName()),
      HConstants.EMPTY_BYTE_ARRAY));
    try (Table table = conn.getTable(tableName)) {
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to addHFileRefs, peerId=" + peerId + ", pairs=" + pairs, e);
    }
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) throws ReplicationException {
    Delete delete = new Delete(Bytes.toBytes(peerId));
    files.forEach(f -> delete.addColumns(HFILE_REF_FAMILY, Bytes.toBytes(f)));
    try (Table table = conn.getTable(tableName)) {
      table.delete(delete);
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to removeHFileRefs, peerId=" + peerId + ", files=" + files, e);
    }
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    List<String> peerIds = new ArrayList<>();
    Scan scan = new Scan().addFamily(HFILE_REF_FAMILY).setReadType(ReadType.STREAM)
      .setFilter(new KeyOnlyFilter());
    try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        peerIds.add(Bytes.toString(result.getRow()));
      }
    } catch (IOException e) {
      throw new ReplicationException("failed to getAllPeersFromHFileRefsQueue", e);
    }
    return peerIds;
  }

  private <T extends Collection<String>> T scanHFiles(Scan scan, Supplier<T> creator)
    throws IOException {
    T files = creator.get();
    try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
          Cell cell = cellScanner.current();
          files.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
        }
      }
    }
    return files;
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    // use scan to avoid getting a too large row one time, which may cause a very huge memory usage.
    Scan scan = new Scan().addFamily(HFILE_REF_FAMILY)
      .setStartStopRowForPrefixScan(Bytes.toBytes(peerId)).setAllowPartialResults(true);
    try {
      return scanHFiles(scan, ArrayList::new);
    } catch (IOException e) {
      throw new ReplicationException("failed to getReplicableHFiles, peerId=" + peerId, e);
    }
  }

  @Override
  public Set<String> getAllHFileRefs() throws ReplicationException {
    Scan scan = new Scan().addFamily(HFILE_REF_FAMILY).setReadType(ReadType.STREAM)
      .setAllowPartialResults(true);
    try {
      return scanHFiles(scan, HashSet::new);
    } catch (IOException e) {
      throw new ReplicationException("failed to getAllHFileRefs", e);
    }
  }

  @Override
  public boolean hasData() throws ReplicationException {
    try {
      return conn.getAdmin().tableExists(tableName);
    } catch (IOException e) {
      throw new ReplicationException("failed to get replication queue table", e);
    }
  }

  @Override
  public void batchUpdateQueues(ServerName serverName, List<ReplicationQueueData> datas)
    throws ReplicationException {
    List<Put> puts = new ArrayList<>();
    for (ReplicationQueueData data : datas) {
      if (data.getOffsets().isEmpty()) {
        continue;
      }
      Put put = new Put(Bytes.toBytes(data.getId().toString()));
      data.getOffsets().forEach((walGroup, offset) -> {
        put.addColumn(QUEUE_FAMILY, Bytes.toBytes(walGroup), Bytes.toBytes(offset.toString()));
      });
      puts.add(put);
    }
    try (Table table = conn.getTable(tableName)) {
      table.put(puts);
    } catch (IOException e) {
      throw new ReplicationException("failed to batch update queues", e);
    }
  }

  @Override
  public void batchUpdateLastSequenceIds(List<ZkLastPushedSeqId> lastPushedSeqIds)
    throws ReplicationException {
    Map<String, Put> peerId2Put = new HashMap<>();
    for (ZkLastPushedSeqId lastPushedSeqId : lastPushedSeqIds) {
      peerId2Put
        .computeIfAbsent(lastPushedSeqId.getPeerId(), peerId -> new Put(Bytes.toBytes(peerId)))
        .addColumn(LAST_SEQUENCE_ID_FAMILY, Bytes.toBytes(lastPushedSeqId.getEncodedRegionName()),
          Bytes.toBytes(lastPushedSeqId.getLastPushedSeqId()));
    }
    try (Table table = conn.getTable(tableName)) {
      table
        .put(peerId2Put.values().stream().filter(p -> !p.isEmpty()).collect(Collectors.toList()));
    } catch (IOException e) {
      throw new ReplicationException("failed to batch update last pushed sequence ids", e);
    }
  }

  @Override
  public void batchUpdateHFileRefs(String peerId, List<String> hfileRefs)
    throws ReplicationException {
    if (hfileRefs.isEmpty()) {
      return;
    }
    Put put = new Put(Bytes.toBytes(peerId));
    for (String ref : hfileRefs) {
      put.addColumn(HFILE_REF_FAMILY, Bytes.toBytes(ref), HConstants.EMPTY_BYTE_ARRAY);
    }
    try (Table table = conn.getTable(tableName)) {
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException("failed to batch update hfile references", e);
    }
  }

  @Override
  public void removeLastSequenceIdsAndHFileRefsBefore(long ts) throws ReplicationException {
    try (Table table = conn.getTable(tableName);
      ResultScanner scanner = table.getScanner(new Scan().addFamily(LAST_SEQUENCE_ID_FAMILY)
        .addFamily(HFILE_REF_FAMILY).setFilter(new KeyOnlyFilter()))) {
      for (;;) {
        Result r = scanner.next();
        if (r == null) {
          break;
        }
        Delete delete = new Delete(r.getRow()).addFamily(LAST_SEQUENCE_ID_FAMILY, ts)
          .addFamily(HFILE_REF_FAMILY, ts);
        table.delete(delete);
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to remove last sequence ids and hfile references before timestamp " + ts, e);
    }
  }
}
