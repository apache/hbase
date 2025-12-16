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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.ClientMetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for storing replication barriers in family 'rep_barrier' of meta table.
 * <p/>
 * See SerialReplicationChecker on how to make use of the barriers.
 */
@InterfaceAudience.Private
public final class ReplicationBarrierFamilyFormat {

  public static final byte[] REPLICATION_PARENT_QUALIFIER = Bytes.toBytes("parent");

  private static final byte ESCAPE_BYTE = (byte) 0xFF;

  private static final byte SEPARATED_BYTE = 0x00;

  private ReplicationBarrierFamilyFormat() {
  }

  public static void addReplicationBarrier(Put put, long openSeqNum) throws IOException {
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.REPLICATION_BARRIER_FAMILY).setQualifier(HConstants.SEQNUM_QUALIFIER)
      .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(Bytes.toBytes(openSeqNum))
      .build());
  }

  private static void writeRegionName(ByteArrayOutputStream out, byte[] regionName) {
    for (byte b : regionName) {
      if (b == ESCAPE_BYTE) {
        out.write(ESCAPE_BYTE);
      }
      out.write(b);
    }
  }

  public static byte[] getParentsBytes(List<RegionInfo> parents) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Iterator<RegionInfo> iter = parents.iterator();
    writeRegionName(bos, iter.next().getRegionName());
    while (iter.hasNext()) {
      bos.write(ESCAPE_BYTE);
      bos.write(SEPARATED_BYTE);
      writeRegionName(bos, iter.next().getRegionName());
    }
    return bos.toByteArray();
  }

  private static List<byte[]> parseParentsBytes(byte[] bytes) {
    List<byte[]> parents = new ArrayList<>();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] == ESCAPE_BYTE) {
        i++;
        if (bytes[i] == SEPARATED_BYTE) {
          parents.add(bos.toByteArray());
          bos.reset();
          continue;
        }
        // fall through to append the byte
      }
      bos.write(bytes[i]);
    }
    if (bos.size() > 0) {
      parents.add(bos.toByteArray());
    }
    return parents;
  }

  public static void addReplicationParent(Put put, List<RegionInfo> parents) throws IOException {
    byte[] value = getParentsBytes(parents);
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.REPLICATION_BARRIER_FAMILY).setQualifier(REPLICATION_PARENT_QUALIFIER)
      .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(value).build());
  }

  public static Put makePutForReplicationBarrier(RegionInfo regionInfo, long openSeqNum, long ts)
    throws IOException {
    Put put = new Put(regionInfo.getRegionName(), ts);
    addReplicationBarrier(put, openSeqNum);
    return put;
  }

  public static final class ReplicationBarrierResult {
    private final long[] barriers;
    private final RegionState.State state;
    private final List<byte[]> parentRegionNames;

    ReplicationBarrierResult(long[] barriers, State state, List<byte[]> parentRegionNames) {
      this.barriers = barriers;
      this.state = state;
      this.parentRegionNames = parentRegionNames;
    }

    public long[] getBarriers() {
      return barriers;
    }

    public RegionState.State getState() {
      return state;
    }

    public List<byte[]> getParentRegionNames() {
      return parentRegionNames;
    }

    @Override
    public String toString() {
      return "ReplicationBarrierResult [barriers=" + Arrays.toString(barriers) + ", state=" + state
        + ", parentRegionNames="
        + parentRegionNames.stream().map(Bytes::toStringBinary).collect(Collectors.joining(", "))
        + "]";
    }
  }

  private static long getReplicationBarrier(Cell c) {
    return Bytes.toLong(c.getValueArray(), c.getValueOffset(), c.getValueLength());
  }

  public static long[] getReplicationBarriers(Result result) {
    return result.getColumnCells(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER)
      .stream().mapToLong(ReplicationBarrierFamilyFormat::getReplicationBarrier).sorted().distinct()
      .toArray();
  }

  private static ReplicationBarrierResult getReplicationBarrierResult(Result result) {
    long[] barriers = getReplicationBarriers(result);
    byte[] stateBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
    RegionState.State state =
      stateBytes != null ? RegionState.State.valueOf(Bytes.toString(stateBytes)) : null;
    byte[] parentRegionsBytes =
      result.getValue(HConstants.REPLICATION_BARRIER_FAMILY, REPLICATION_PARENT_QUALIFIER);
    List<byte[]> parentRegionNames =
      parentRegionsBytes != null ? parseParentsBytes(parentRegionsBytes) : Collections.emptyList();
    return new ReplicationBarrierResult(barriers, state, parentRegionNames);
  }

  public static ReplicationBarrierResult getReplicationBarrierResult(Connection conn,
    TableName tableName, byte[] row, byte[] encodedRegionName) throws IOException {
    byte[] metaStartKey = RegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    byte[] metaStopKey =
      RegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW, "", false);
    Scan scan = new Scan().withStartRow(metaStartKey).withStopRow(metaStopKey)
      .addColumn(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER)
      .addFamily(HConstants.REPLICATION_BARRIER_FAMILY).readAllVersions().setReversed(true)
      .setCaching(10);
    try (Table table = conn.getTable(MetaTableName.getInstance());
      ResultScanner scanner = table.getScanner(scan)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          return new ReplicationBarrierResult(new long[0], null, Collections.emptyList());
        }
        byte[] regionName = result.getRow();
        // TODO: we may look up a region which has already been split or merged so we need to check
        // whether the encoded name matches. Need to find a way to quit earlier when there is no
        // record for the given region, for now it will scan to the end of the table.
        if (
          !Bytes.equals(encodedRegionName, Bytes.toBytes(RegionInfo.encodeRegionName(regionName)))
        ) {
          continue;
        }
        return getReplicationBarrierResult(result);
      }
    }
  }

  public static long[] getReplicationBarriers(Connection conn, byte[] regionName)
    throws IOException {
    try (Table table = conn.getTable(MetaTableName.getInstance())) {
      Result result = table.get(new Get(regionName)
        .addColumn(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER)
        .readAllVersions());
      return getReplicationBarriers(result);
    }
  }

  public static List<Pair<String, Long>> getTableEncodedRegionNameAndLastBarrier(Connection conn,
    TableName tableName) throws IOException {
    List<Pair<String, Long>> list = new ArrayList<>();
    MetaTableAccessor.scanMeta(conn,
      ClientMetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REPLICATION),
      ClientMetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REPLICATION),
      QueryType.REPLICATION, r -> {
        byte[] value =
          r.getValue(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER);
        if (value == null) {
          return true;
        }
        long lastBarrier = Bytes.toLong(value);
        String encodedRegionName = RegionInfo.encodeRegionName(r.getRow());
        list.add(Pair.newPair(encodedRegionName, lastBarrier));
        return true;
      });
    return list;
  }

  public static List<String> getTableEncodedRegionNamesForSerialReplication(Connection conn,
    TableName tableName) throws IOException {
    List<String> list = new ArrayList<>();
    MetaTableAccessor.scanMeta(conn,
      ClientMetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REPLICATION),
      ClientMetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REPLICATION),
      QueryType.REPLICATION, new FirstKeyOnlyFilter(), Integer.MAX_VALUE, r -> {
        list.add(RegionInfo.encodeRegionName(r.getRow()));
        return true;
      });
    return list;
  }
}
