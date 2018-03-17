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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table based replication queue storage.
 */
@InterfaceAudience.Private
public class TableReplicationQueueStorage extends TableReplicationStorageBase
    implements ReplicationQueueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(TableReplicationQueueStorage.class);

  public TableReplicationQueueStorage(ZKWatcher zookeeper, Configuration conf) throws IOException {
    super(zookeeper, conf);
  }

  /**
   * Serialize the {fileName, position} pair into a byte array.
   */
  private static byte[] makeByteArray(String fileName, long position) {
    byte[] data = new byte[Bytes.SIZEOF_INT + fileName.length() + Bytes.SIZEOF_LONG];
    int pos = 0;
    pos = Bytes.putInt(data, pos, fileName.length());
    pos = Bytes.putBytes(data, pos, Bytes.toBytes(fileName), 0, fileName.length());
    pos = Bytes.putLong(data, pos, position);
    assert pos == data.length;
    return data;
  }

  /**
   * Deserialize the byte array into a {filename, position} pair.
   */
  private static Pair<String, Long> parseFileNameAndPosition(byte[] data, int offset)
      throws IOException {
    if (data == null) {
      throw new IOException("The byte array shouldn't be null");
    }
    int pos = offset;
    int len = Bytes.toInt(data, pos, Bytes.SIZEOF_INT);
    pos += Bytes.SIZEOF_INT;
    if (pos + len > data.length) {
      throw new IllegalArgumentException("offset (" + pos + ") + length (" + len + ") exceed the"
          + " capacity of the array: " + data.length);
    }
    String fileName = Bytes.toString(Bytes.copy(data, pos, len));
    pos += len;
    long position = Bytes.toLong(data, pos, Bytes.SIZEOF_LONG);
    return new Pair<>(fileName, position);
  }

  @Override
  public void removeQueue(ServerName serverName, String queueId) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Delete delete = new Delete(getServerNameRowKey(serverName));
      delete.addColumn(FAMILY_QUEUE, Bytes.toBytes(queueId));
      // Delete all <fileName, position> pairs.
      delete.addColumns(FAMILY_WAL, Bytes.toBytes(queueId), HConstants.LATEST_TIMESTAMP);
      table.delete(delete);
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to remove wal from queue, serverName=" + serverName + ", queueId=" + queueId, e);
    }
  }

  @Override
  public void addWAL(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Put put = new Put(getServerNameRowKey(serverName));
      put.addColumn(FAMILY_RS_STATE, QUALIFIER_STATE_ENABLED, HConstants.EMPTY_BYTE_ARRAY);
      put.addColumn(FAMILY_QUEUE, Bytes.toBytes(queueId), HConstants.EMPTY_BYTE_ARRAY);
      put.addColumn(FAMILY_WAL, Bytes.toBytes(queueId), makeByteArray(fileName, 0L));
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException("Failed to add wal to queue, serverName=" + serverName
          + ", queueId=" + queueId + ", fileName=" + fileName, e);
    }
  }

  @Override
  public void removeWAL(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Optional<WALCell> walCell = getWALsInQueue0(table, serverName, queueId).stream()
          .filter(w -> w.fileNameMatch(fileName)).findFirst();
      if (walCell.isPresent()) {
        Delete delete = new Delete(getServerNameRowKey(walCell.get().serverName))
            .addColumn(FAMILY_WAL, Bytes.toBytes(queueId), walCell.get().cellTimestamp);
        table.delete(delete);
      } else {
        LOG.warn(fileName + " has already been deleted when removing log");
      }
    } catch (IOException e) {
      throw new ReplicationException("Failed to remove wal from queue, serverName=" + serverName
          + ", queueId=" + queueId + ", fileName=" + fileName, e);
    }
  }

  @Override
  public void setWALPosition(ServerName serverName, String queueId, String fileName, long position,
      Map<String, Long> lastSeqIds) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Optional<WALCell> walCell = getWALsInQueue0(table, serverName, queueId).stream()
          .filter(w -> w.fileNameMatch(fileName)).findFirst();
      if (walCell.isPresent()) {
        List<Put> puts = new ArrayList<>();
        Put put = new Put(getServerNameRowKey(serverName)).addColumn(FAMILY_WAL,
              Bytes.toBytes(walCell.get().queueId), walCell.get().cellTimestamp,
              makeByteArray(fileName, position));
        puts.add(put);
        // Update the last pushed sequence id for each region in a batch.
        String peerId = ReplicationUtils.parsePeerIdFromQueueId(queueId);
        if (lastSeqIds != null && lastSeqIds.size() > 0) {
          for (Map.Entry<String, Long> e : lastSeqIds.entrySet()) {
            Put regionPut = new Put(Bytes.toBytes(peerId)).addColumn(FAMILY_REGIONS,
              getRegionQualifier(e.getKey()), Bytes.toBytes(e.getValue()));
            puts.add(regionPut);
          }
        }
        table.put(puts);
      } else {
        throw new ReplicationException("WAL file " + fileName + " does not found under queue "
            + queueId + " for server " + serverName);
      }
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to set wal position and last sequence ids, serverName=" + serverName
              + ", queueId=" + queueId + ", fileName=" + fileName + ", position=" + position,
          e);
    }
  }

  @Override
  public long getLastSequenceId(String encodedRegionName, String peerId)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Get get = new Get(Bytes.toBytes(peerId));
      get.addColumn(FAMILY_REGIONS, getRegionQualifier(encodedRegionName));
      Result r = table.get(get);
      if (r == null || r.listCells() == null) {
        return HConstants.NO_SEQNUM;
      }
      return Bytes.toLong(r.getValue(FAMILY_REGIONS, getRegionQualifier(encodedRegionName)));
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to get last sequence id, region=" + encodedRegionName + ", peerId=" + peerId, e);
    }
  }

  @Override
  public long getWALPosition(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Optional<WALCell> walCell = getWALsInQueue0(table, serverName, queueId).stream()
          .filter(w -> w.fileNameMatch(fileName)).findFirst();
      if (walCell.isPresent()) {
        return walCell.get().position;
      } else {
        LOG.warn("WAL " + fileName + " does not found under queue " + queueId + " for server "
            + serverName);
        return 0;
      }
    } catch (IOException e) {
      throw new ReplicationException("Failed to get wal position. serverName=" + serverName
          + ", queueId=" + queueId + ", fileName=" + fileName, e);
    }
  }

  /**
   * Each cell in column wal:{queueId} will be parsed to a WALCell. The WALCell will be more
   * friendly to upper layer.
   */
  private static final class WALCell {
    ServerName serverName;
    String queueId;
    String wal;
    long position;
    long cellTimestamp;// Timestamp of the cell

    private WALCell(ServerName serverName, String queueId, String wal, long position,
        long cellTimestamp) {
      this.serverName = serverName;
      this.queueId = queueId;
      this.wal = wal;
      this.position = position;
      this.cellTimestamp = cellTimestamp;
    }

    public static WALCell create(Cell cell) throws IOException {
      ServerName serverName = ServerName.parseServerName(
        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
      String queueId = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
      Pair<String, Long> fileAndPos =
          parseFileNameAndPosition(cell.getValueArray(), cell.getValueOffset());
      return new WALCell(serverName, queueId, fileAndPos.getFirst(), fileAndPos.getSecond(),
          cell.getTimestamp());
    }

    public boolean fileNameMatch(String fileName) {
      return StringUtils.equals(wal, fileName);
    }
  }

  /**
   * Parse the WALCell list from a HBase result.
   */
  private List<WALCell> result2WALCells(Result r) throws IOException {
    List<WALCell> wals = new ArrayList<>();
    if (r != null && r.listCells() != null && r.listCells().size() > 0) {
      for (Cell cell : r.listCells()) {
        wals.add(WALCell.create(cell));
      }
    }
    return wals;
  }

  /**
   * List all WALs for the specific region server and queueId.
   */
  private List<WALCell> getWALsInQueue0(Table table, ServerName serverName, String queueId)
      throws IOException {
    Get get = new Get(getServerNameRowKey(serverName)).addColumn(FAMILY_WAL, Bytes.toBytes(queueId))
        .readAllVersions();
    return result2WALCells(table.get(get));
  }

  @Override
  public List<String> getWALsInQueue(ServerName serverName, String queueId)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      return getWALsInQueue0(table, serverName, queueId).stream().map(p -> p.wal)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to get wals in queue. serverName=" + serverName + ", queueId=" + queueId, e);
    }
  }

  @Override
  public List<String> getAllQueues(ServerName serverName) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      List<String> queues = new ArrayList<>();
      Get get = new Get(getServerNameRowKey(serverName)).addFamily(FAMILY_QUEUE);
      Result r = table.get(get);
      if (r != null && r.listCells() != null && r.listCells().size() > 0) {
        for (Cell c : r.listCells()) {
          String queue =
              Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
          queues.add(queue);
        }
      }
      return queues;
    } catch (IOException e) {
      throw new ReplicationException("Failed to get all queues. serverName=" + serverName, e);
    }
  }

  @Override
  public Pair<String, SortedSet<String>> claimQueue(ServerName sourceServerName, String queueId,
      ServerName destServerName) throws ReplicationException {
    LOG.info(
      "Atomically moving " + sourceServerName + "/" + queueId + "'s WALs to " + destServerName);
    try (Table table = getReplicationMetaTable()) {
      // Create an enabled region server for destination.
      byte[] destServerNameRowKey = getServerNameRowKey(destServerName);
      byte[] srcServerNameRowKey = getServerNameRowKey(sourceServerName);
      Put put = new Put(destServerNameRowKey).addColumn(FAMILY_RS_STATE, QUALIFIER_STATE_ENABLED,
        HConstants.EMPTY_BYTE_ARRAY);
      table.put(put);
      List<WALCell> wals = getWALsInQueue0(table, sourceServerName, queueId);
      String newQueueId = queueId + "-" + sourceServerName;
      // Remove the queue in source region server if wal set of the queue is empty.
      if (CollectionUtils.isEmpty(wals)) {
        Delete delete =
            new Delete(srcServerNameRowKey).addColumn(FAMILY_QUEUE, Bytes.toBytes(queueId))
                .addColumns(FAMILY_WAL, Bytes.toBytes(queueId), HConstants.LATEST_TIMESTAMP);
        table.delete(delete);
        LOG.info("Removed " + sourceServerName + "/" + queueId + " since it's empty");
        return new Pair<>(newQueueId, Collections.emptySortedSet());
      }
      // Transfer all wals from source region server to destination region server in a batch.
      List<Mutation> mutations = new ArrayList<>();
      // a. Create queue for destination server.
      mutations.add(new Put(destServerNameRowKey).addColumn(FAMILY_QUEUE, Bytes.toBytes(newQueueId),
        HConstants.EMPTY_BYTE_ARRAY));
      SortedSet<String> logQueue = new TreeSet<>();
      for (WALCell wal : wals) {
        byte[] data = makeByteArray(wal.wal, wal.cellTimestamp);
        // b. Add wal to destination server.
        mutations.add(
          new Put(destServerNameRowKey).addColumn(FAMILY_WAL, Bytes.toBytes(newQueueId), data));
        // c. Remove wal from source server.
        mutations.add(new Delete(srcServerNameRowKey).addColumn(FAMILY_WAL, Bytes.toBytes(queueId),
          wal.cellTimestamp));
        logQueue.add(wal.wal);
      }
      // d. Remove the queue of source server.
      mutations
          .add(new Delete(srcServerNameRowKey).addColumn(FAMILY_QUEUE, Bytes.toBytes(queueId)));
      Object[] results = new Object[mutations.size()];
      table.batch(mutations, results);
      boolean allSuccess = Stream.of(results).allMatch(r -> r != null);
      if (!allSuccess) {
        throw new ReplicationException("Claim queue queueId=" + queueId + " from "
            + sourceServerName + " to " + destServerName + " failed, not all mutations success.");
      }
      LOG.info(
        "Atomically moved " + sourceServerName + "/" + queueId + "'s WALs to " + destServerName);
      return new Pair<>(newQueueId, logQueue);
    } catch (IOException | InterruptedException e) {
      throw new ReplicationException("Claim queue queueId=" + queueId + " from " + sourceServerName
          + " to " + destServerName + " failed", e);
    }
  }

  @Override
  public void removeReplicatorIfQueueIsEmpty(ServerName serverName) throws ReplicationException {
    // TODO Make this to be a checkAndDelete, and provide a UT for it.
    try (Table table = getReplicationMetaTable()) {
      Get get = new Get(getServerNameRowKey(serverName)).addFamily(FAMILY_WAL).readAllVersions();
      Result r = table.get(get);
      if (r == null || r.listCells() == null || r.listCells().size() == 0) {
        Delete delete = new Delete(getServerNameRowKey(serverName));
        table.delete(delete);
      }
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to remove replicator when queue is empty, serverName=" + serverName, e);
    }
  }

  @Override
  public List<ServerName> getListOfReplicators() throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Scan scan = new Scan().addColumn(FAMILY_RS_STATE, QUALIFIER_STATE_ENABLED).readVersions(1);
      Set<ServerName> serverNames = new HashSet<>();
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          if (r.listCells().size() > 0) {
            Cell firstCell = r.listCells().get(0);
            String serverName = Bytes.toString(firstCell.getRowArray(), firstCell.getRowOffset(),
              firstCell.getRowLength());
            serverNames.add(ServerName.parseServerName(serverName));
          }
        }
      }
      return new ArrayList<>(serverNames);
    } catch (IOException e) {
      throw new ReplicationException("Failed to get list of replicators", e);
    }
  }

  @Override
  public Set<String> getAllWALs() throws ReplicationException {
    Set<String> walSet = new HashSet<>();
    try (Table table = getReplicationMetaTable()) {
      Scan scan = new Scan().addFamily(FAMILY_WAL).readAllVersions();
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          result2WALCells(r).forEach(w -> walSet.add(w.wal));
        }
      }
      return walSet;
    } catch (IOException e) {
      throw new ReplicationException("Failed to get all wals", e);
    }
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    // Need to do nothing.
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) throws ReplicationException {
    // Need to do nothing.
  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      List<Put> puts = new ArrayList<>();
      for (Pair<Path, Path> p : pairs) {
        Put put = new Put(Bytes.toBytes(peerId));
        put.addColumn(FAMILY_HFILE_REFS, Bytes.toBytes(p.getSecond().getName()),
          HConstants.EMPTY_BYTE_ARRAY);
        puts.add(put);
      }
      table.put(puts);
    } catch (IOException e) {
      throw new ReplicationException("Failed to add hfile refs, peerId=" + peerId, e);
    }
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      List<Delete> deletes = new ArrayList<>();
      for (String file : files) {
        Delete delete = new Delete(Bytes.toBytes(peerId));
        delete.addColumns(FAMILY_HFILE_REFS, Bytes.toBytes(file));
        deletes.add(delete);
      }
      table.delete(deletes);
    } catch (IOException e) {
      throw new ReplicationException("Failed to remove hfile refs, peerId=" + peerId, e);
    }
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Set<String> peers = new HashSet<>();
      Scan scan = new Scan().addFamily(FAMILY_HFILE_REFS);
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          if (r.listCells().size() > 0) {
            Cell firstCell = r.listCells().get(0);
            String peerId = Bytes.toString(firstCell.getRowArray(), firstCell.getRowOffset(),
              firstCell.getRowLength());
            peers.add(peerId);
          }
        }
      }
      return new ArrayList<>(peers);
    } catch (IOException e) {
      throw new ReplicationException("Faield to get all peers by reading hbase:replication meta",
          e);
    }
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Get get = new Get(Bytes.toBytes(peerId)).addFamily(FAMILY_HFILE_REFS);
      Result r = table.get(get);
      List<String> hfiles = new ArrayList<>();
      if (r != null && r.listCells() != null) {
        for (Cell c : r.listCells()) {
          hfiles.add(
            Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength()));
        }
      }
      return hfiles;
    } catch (IOException e) {
      throw new ReplicationException("Failed to get replicable hfiles, peerId=" + peerId, e);
    }
  }

  @Override
  public Set<String> getAllHFileRefs() throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Scan scan = new Scan().addFamily(FAMILY_HFILE_REFS);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Set<String> hfileSet = new HashSet<>();
        for (Result r : scanner) {
          for (Cell c : r.listCells()) {
            String hfile = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(),
              c.getQualifierLength());
            hfileSet.add(hfile);
          }
        }
        return hfileSet;
      }
    } catch (IOException e) {
      throw new ReplicationException("Failed to get all hfile refs", e);
    }
  }
}
