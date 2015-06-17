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
package org.apache.hadoop.hbase.protobuf;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.*;
import org.apache.hadoop.hbase.quotas.QuotaScope;
import org.apache.hadoop.hbase.quotas.QuotaType;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Converter functions exposed by ProtobufUtil
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
        justification="None. Address sometime.")
@InterfaceAudience.Private
public class ProtobufConverter {

  private ProtobufConverter(){
  }

  /**
   * Many results are simple: no cell, exists true or false. To save on object creations,
   *  we reuse them across calls.
   */
  private final static Cell[] EMPTY_CELL_ARRAY = new Cell[]{};
  private final static Result EMPTY_RESULT = Result.create(EMPTY_CELL_ARRAY);
  private final static Result EMPTY_RESULT_EXISTS_TRUE = Result.create(null, true);
  private final static Result EMPTY_RESULT_EXISTS_FALSE = Result.create(null, false);
  private final static Result EMPTY_RESULT_STALE = Result.create(EMPTY_CELL_ARRAY, null, true);
  private final static Result EMPTY_RESULT_EXISTS_TRUE_STALE
          = Result.create((Cell[])null, true, true);
  private final static Result EMPTY_RESULT_EXISTS_FALSE_STALE
          = Result.create((Cell[])null, false, true);

  private final static ClientProtos.Result EMPTY_RESULT_PB;
  private final static ClientProtos.Result EMPTY_RESULT_PB_EXISTS_TRUE;
  private final static ClientProtos.Result EMPTY_RESULT_PB_EXISTS_FALSE;
  private final static ClientProtos.Result EMPTY_RESULT_PB_STALE;
  private final static ClientProtos.Result EMPTY_RESULT_PB_EXISTS_TRUE_STALE;
  private final static ClientProtos.Result EMPTY_RESULT_PB_EXISTS_FALSE_STALE;

  static {
    ClientProtos.Result.Builder builder = ClientProtos.Result.newBuilder();

    builder.setExists(true);
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB_EXISTS_TRUE =  builder.build();

    builder.setStale(true);
    EMPTY_RESULT_PB_EXISTS_TRUE_STALE = builder.build();
    builder.clear();

    builder.setExists(false);
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB_EXISTS_FALSE =  builder.build();
    builder.setStale(true);
    EMPTY_RESULT_PB_EXISTS_FALSE_STALE = builder.build();

    builder.clear();
    builder.setAssociatedCellCount(0);
    EMPTY_RESULT_PB =  builder.build();
    builder.setStale(true);
    EMPTY_RESULT_PB_STALE = builder.build();
  }

  /**
   * Dynamic class loader to load filter/comparators
   */
  private final static ClassLoader CLASS_LOADER;

  static {
    ClassLoader parent = ProtobufUtil.class.getClassLoader();
    Configuration conf = HBaseConfiguration.create();
    CLASS_LOADER = new DynamicClassLoader(conf, parent);
  }

  /**
   * Convert a ServerName to a protocol buffer ServerName
   *
   * @param serverName the ServerName to convert
   * @return the converted protocol buffer ServerName
   * @see #toServerName(org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName)
   */
  public static HBaseProtos.ServerName toServerName(final ServerName serverName) {
    if (serverName == null) return null;
    HBaseProtos.ServerName.Builder builder =
            HBaseProtos.ServerName.newBuilder();
    builder.setHostName(serverName.getHostname());
    if (serverName.getPort() >= 0) {
        builder.setPort(serverName.getPort());
    }
    if (serverName.getStartcode() >= 0) {
        builder.setStartCode(serverName.getStartcode());
    }
    return builder.build();
  }

  /**
   * Convert a protocol buffer ServerName to a ServerName
   *
   * @param proto the protocol buffer ServerName to convert
   * @return the converted ServerName
   */
  public static ServerName toServerName(final HBaseProtos.ServerName proto) {
    if (proto == null) return null;
    String hostName = proto.getHostName();
    long startCode = -1;
    int port = -1;
    if (proto.hasPort()) {
      port = proto.getPort();
    }
    if (proto.hasStartCode()) {
      startCode = proto.getStartCode();
    }
    return ServerName.valueOf(hostName, port, startCode);
  }

  /**
   * Convert a protobuf Durability into a client Durability
   */
  public static Durability toDurability(
          final ClientProtos.MutationProto.Durability proto) {
    switch(proto) {
      case USE_DEFAULT:
        return Durability.USE_DEFAULT;
      case SKIP_WAL:
        return Durability.SKIP_WAL;
      case ASYNC_WAL:
        return Durability.ASYNC_WAL;
      case SYNC_WAL:
        return Durability.SYNC_WAL;
      case FSYNC_WAL:
        return Durability.FSYNC_WAL;
      default:
        return Durability.USE_DEFAULT;
    }
  }

  /**
   * Convert a client Durability into a protobuf Durability
   */
  public static ClientProtos.MutationProto.Durability toDurability(
          final Durability d) {
    switch(d) {
      case USE_DEFAULT:
        return ClientProtos.MutationProto.Durability.USE_DEFAULT;
      case SKIP_WAL:
        return ClientProtos.MutationProto.Durability.SKIP_WAL;
      case ASYNC_WAL:
        return ClientProtos.MutationProto.Durability.ASYNC_WAL;
      case SYNC_WAL:
        return ClientProtos.MutationProto.Durability.SYNC_WAL;
      case FSYNC_WAL:
        return ClientProtos.MutationProto.Durability.FSYNC_WAL;
      default:
        return ClientProtos.MutationProto.Durability.USE_DEFAULT;
    }
  }

  /**
   * Convert a protocol buffer Get to a client Get
   *
   * @param proto the protocol buffer Get to convert
   * @return the converted client Get
   * @throws IOException
   */
  public static Get toGet(
          final ClientProtos.Get proto) throws IOException {
    if (proto == null) return null;
    byte[] row = proto.getRow().toByteArray();
    Get get = new Get(row);
    if (proto.hasCacheBlocks()) {
      get.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.hasMaxVersions()) {
      get.setMaxVersions(proto.getMaxVersions());
    }
    if (proto.hasStoreLimit()) {
      get.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.hasStoreOffset()) {
      get.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.hasTimeRange()) {
      HBaseProtos.TimeRange timeRange = proto.getTimeRange();
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.hasFrom()) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.hasTo()) {
        maxStamp = timeRange.getTo();
      }
      get.setTimeRange(minStamp, maxStamp);
    }
    if (proto.hasFilter()) {
      FilterProtos.Filter filter = proto.getFilter();
      get.setFilter(ProtobufUtil.toFilter(filter));
    }
    for (HBaseProtos.NameBytesPair attribute: proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      for (ClientProtos.Column column: proto.getColumnList()) {
        byte[] family = column.getFamily().toByteArray();
        if (column.getQualifierCount() > 0) {
          for (ByteString qualifier: column.getQualifierList()) {
            get.addColumn(family, qualifier.toByteArray());
          }
        } else {
          get.addFamily(family);
        }
      }
    }
    if (proto.hasExistenceOnly() && proto.getExistenceOnly()){
      get.setCheckExistenceOnly(true);
    }
    if (proto.hasClosestRowBefore() && proto.getClosestRowBefore()){
      get.setClosestRowBefore(true);
    }
    if (proto.hasConsistency()) {
      get.setConsistency(toConsistency(proto.getConsistency()));
    }
    return get;
  }

  public static Consistency toConsistency(ClientProtos.Consistency consistency) {
    switch (consistency) {
      case STRONG : return Consistency.STRONG;
      case TIMELINE : return Consistency.TIMELINE;
      default : return Consistency.STRONG;
    }
  }

  public static ClientProtos.Consistency toConsistency(Consistency consistency) {
    switch (consistency) {
      case STRONG : return ClientProtos.Consistency.STRONG;
      case TIMELINE : return ClientProtos.Consistency.TIMELINE;
      default : return ClientProtos.Consistency.STRONG;
    }
  }

  /**
   * Convert a protocol buffer Mutate to a Put.
   *
   * @param proto The protocol buffer MutationProto to convert
   * @param cellScanner If non-null, the Cell data that goes with this proto.
   * @return A client Put.
   * @throws IOException
   */
  public static Put toPut(final ClientProtos.MutationProto proto, final CellScanner cellScanner)
          throws IOException {
    // TODO: Server-side at least why do we convert back to the Client types?  Why not just pb it?
    ClientProtos.MutationProto.MutationType type = proto.getMutateType();
    assert type == ClientProtos.MutationProto.MutationType.PUT: type.name();
    long timestamp = proto.hasTimestamp()? proto.getTimestamp(): HConstants.LATEST_TIMESTAMP;
    Put put = null;
    int cellCount = proto.hasAssociatedCellCount()? proto.getAssociatedCellCount(): 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
                toShortString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
                  " no cell returned: " + toShortString(proto));
        }
        Cell cell = cellScanner.current();
        if (put == null) {
          put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), timestamp);
        }
        put.add(cell);
      }
    } else {
      if (proto.hasRow()) {
        put = new Put(proto.getRow().asReadOnlyByteBuffer(), timestamp);
      } else {
        throw new IllegalArgumentException("row cannot be null");
      }
      // The proto has the metadata and the data itself
      for (ClientProtos.MutationProto.ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (ClientProtos.MutationProto.ColumnValue.QualifierValue qv: column.getQualifierValueList()) {
          if (!qv.hasValue()) {
            throw new DoNotRetryIOException(
                    "Missing required field: qualifier value");
          }
          ByteBuffer qualifier =
                  qv.hasQualifier() ? qv.getQualifier().asReadOnlyByteBuffer() : null;
          ByteBuffer value =
                  qv.hasValue() ? qv.getValue().asReadOnlyByteBuffer() : null;
          long ts = timestamp;
          if (qv.hasTimestamp()) {
            ts = qv.getTimestamp();
          }
          byte[] tags;
          if (qv.hasTags()) {
            tags = qv.getTags().toByteArray();
            Object[] array = Tag.asList(tags, 0, (short)tags.length).toArray();
            Tag[] tagArray = new Tag[array.length];
            for(int i = 0; i< array.length; i++) {
              tagArray[i] = (Tag)array[i];
            }
            if(qv.hasDeleteType()) {
              byte[] qual = qv.hasQualifier() ? qv.getQualifier().toByteArray() : null;
              put.add(new KeyValue(proto.getRow().toByteArray(), family, qual, ts,
                      fromDeleteType(qv.getDeleteType()), null, tags));
            } else {
              put.addImmutable(family, qualifier, ts, value, tagArray);
            }
          } else {
            if(qv.hasDeleteType()) {
              byte[] qual = qv.hasQualifier() ? qv.getQualifier().toByteArray() : null;
              put.add(new KeyValue(proto.getRow().toByteArray(), family, qual, ts,
                      fromDeleteType(qv.getDeleteType())));
            } else{
              put.addImmutable(family, qualifier, ts, value);
            }
          }
        }
      }
    }
    put.setDurability(toDurability(proto.getDurability()));
    for (HBaseProtos.NameBytesPair attribute: proto.getAttributeList()) {
      put.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return put;
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto the protocol buffer Mutate to convert
   * @param cellScanner if non-null, the data that goes with this delete.
   * @return the converted client Delete
   * @throws IOException
   */
  public static Delete toDelete(final ClientProtos.MutationProto proto, final CellScanner cellScanner)
          throws IOException {
    ClientProtos.MutationProto.MutationType type = proto.getMutateType();
    assert type == ClientProtos.MutationProto.MutationType.DELETE : type.name();
    byte [] row = proto.hasRow()? proto.getRow().toByteArray(): null;
    long timestamp = HConstants.LATEST_TIMESTAMP;
    if (proto.hasTimestamp()) {
      timestamp = proto.getTimestamp();
    }
    Delete delete = null;
    int cellCount = proto.hasAssociatedCellCount()? proto.getAssociatedCellCount(): 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        // TextFormat should be fine for a Delete since it carries no data, just coordinates.
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
                TextFormat.shortDebugString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          // TextFormat should be fine for a Delete since it carries no data, just coordinates.
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
                  " no cell returned: " + TextFormat.shortDebugString(proto));
        }
        Cell cell = cellScanner.current();
        if (delete == null) {
          delete =
                  new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), timestamp);
        }
        delete.addDeleteMarker(cell);
      }
    } else {
      delete = new Delete(row, timestamp);
      for (ClientProtos.MutationProto.ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (ClientProtos.MutationProto.ColumnValue.QualifierValue qv: column.getQualifierValueList()) {
          ClientProtos.MutationProto.DeleteType deleteType = qv.getDeleteType();
          byte[] qualifier = null;
          if (qv.hasQualifier()) {
            qualifier = qv.getQualifier().toByteArray();
          }
          long ts = HConstants.LATEST_TIMESTAMP;
          if (qv.hasTimestamp()) {
            ts = qv.getTimestamp();
          }
          if (deleteType == ClientProtos.MutationProto.DeleteType.DELETE_ONE_VERSION) {
            delete.deleteColumn(family, qualifier, ts);
          } else if (deleteType == ClientProtos.MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS) {
            delete.deleteColumns(family, qualifier, ts);
          } else if (deleteType == ClientProtos.MutationProto.DeleteType.DELETE_FAMILY_VERSION) {
            delete.deleteFamilyVersion(family, ts);
          } else {
            delete.deleteFamily(family, ts);
          }
        }
      }
    }
    delete.setDurability(toDurability(proto.getDurability()));
    for (HBaseProtos.NameBytesPair attribute: proto.getAttributeList()) {
      delete.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return delete;
  }

  /**
   * Convert a protocol buffer Mutate to an Append
   * @param cellScanner
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Append
   * @throws IOException
   */
  public static Append toAppend(final ClientProtos.MutationProto proto, final CellScanner cellScanner)
          throws IOException {
    ClientProtos.MutationProto.MutationType type = proto.getMutateType();
    assert type == ClientProtos.MutationProto.MutationType.APPEND : type.name();
    byte [] row = proto.hasRow()? proto.getRow().toByteArray(): null;
    Append append = null;
    int cellCount = proto.hasAssociatedCellCount()? proto.getAssociatedCellCount(): 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
                toShortString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
                  " no cell returned: " + toShortString(proto));
        }
        Cell cell = cellScanner.current();
        if (append == null) {
          append = new Append(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        }
        append.add(cell);
      }
    } else {
      append = new Append(row);
      for (ClientProtos.MutationProto.ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (ClientProtos.MutationProto.ColumnValue.QualifierValue qv: column.getQualifierValueList()) {
          byte[] qualifier = qv.getQualifier().toByteArray();
          if (!qv.hasValue()) {
            throw new DoNotRetryIOException(
                    "Missing required field: qualifier value");
          }
          byte[] value = qv.getValue().toByteArray();
          byte[] tags = null;
          if (qv.hasTags()) {
            tags = qv.getTags().toByteArray();
          }
          append.add(CellUtil.createCell(row, family, qualifier, qv.getTimestamp(),
                  KeyValue.Type.Put, value, tags));
        }
      }
    }
    append.setDurability(toDurability(proto.getDurability()));
    for (HBaseProtos.NameBytesPair attribute: proto.getAttributeList()) {
      append.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return append;
  }

  /**
   * Convert a MutateRequest to Mutation
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted Mutation
   * @throws IOException
   */
  public static Mutation toMutation(final ClientProtos.MutationProto proto) throws IOException {
    ClientProtos.MutationProto.MutationType type = proto.getMutateType();
    if (type == ClientProtos.MutationProto.MutationType.APPEND) {
      return toAppend(proto, null);
    }
    if (type == ClientProtos.MutationProto.MutationType.DELETE) {
      return toDelete(proto, null);
    }
    if (type == ClientProtos.MutationProto.MutationType.PUT) {
      return toPut(proto, null);
    }
    throw new IOException("Unknown mutation type " + type);
  }

  /**
   * Convert a protocol buffer Mutate to an Increment
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Increment
   * @throws IOException
   */
  public static Increment toIncrement(final ClientProtos.MutationProto proto, final CellScanner cellScanner)
          throws IOException {
    ClientProtos.MutationProto.MutationType type = proto.getMutateType();
    assert type == ClientProtos.MutationProto.MutationType.INCREMENT : type.name();
    byte [] row = proto.hasRow()? proto.getRow().toByteArray(): null;
    Increment increment = null;
    int cellCount = proto.hasAssociatedCellCount()? proto.getAssociatedCellCount(): 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: " +
                TextFormat.shortDebugString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i +
                  " no cell returned: " + TextFormat.shortDebugString(proto));
        }
        Cell cell = cellScanner.current();
        if (increment == null) {
          increment = new Increment(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        }
        increment.add(cell);
      }
    } else {
      increment = new Increment(row);
      for (ClientProtos.MutationProto.ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (ClientProtos.MutationProto.ColumnValue.QualifierValue qv: column.getQualifierValueList()) {
          byte[] qualifier = qv.getQualifier().toByteArray();
          if (!qv.hasValue()) {
            throw new DoNotRetryIOException("Missing required field: qualifier value");
          }
          byte[] value = qv.getValue().toByteArray();
          byte[] tags = null;
          if (qv.hasTags()) {
            tags = qv.getTags().toByteArray();
          }
          increment.add(CellUtil.createCell(row, family, qualifier, qv.getTimestamp(),
                  KeyValue.Type.Put, value, tags));
        }
      }
    }
    if (proto.hasTimeRange()) {
      HBaseProtos.TimeRange timeRange = proto.getTimeRange();
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.hasFrom()) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.hasTo()) {
        maxStamp = timeRange.getTo();
      }
      increment.setTimeRange(minStamp, maxStamp);
    }
    increment.setDurability(toDurability(proto.getDurability()));
    for (HBaseProtos.NameBytesPair attribute : proto.getAttributeList()) {
      increment.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return increment;
  }

  /**
   * Convert a client Scan to a protocol buffer Scan
   *
   * @param scan the client Scan to convert
   * @return the converted protocol buffer Scan
   * @throws IOException
   */
  public static ClientProtos.Scan toScan(
          final Scan scan) throws IOException {
    ClientProtos.Scan.Builder scanBuilder =
            ClientProtos.Scan.newBuilder();
    scanBuilder.setCacheBlocks(scan.getCacheBlocks());
    if (scan.getBatch() > 0) {
      scanBuilder.setBatchSize(scan.getBatch());
    }
    if (scan.getMaxResultSize() > 0) {
      scanBuilder.setMaxResultSize(scan.getMaxResultSize());
    }
    if (scan.isSmall()) {
      scanBuilder.setSmall(scan.isSmall());
    }
    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      scanBuilder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand.booleanValue());
    }
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    TimeRange timeRange = scan.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
              HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      scanBuilder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(ByteStringer.wrap(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(ByteStringer.wrap(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(ProtobufUtil.toFilter(scan.getFilter()));
    }
    if (scan.hasFamilies()) {
      ClientProtos.Column.Builder columnBuilder = ClientProtos.Column.newBuilder();
      for (Map.Entry<byte[],NavigableSet<byte []>>
              family: scan.getFamilyMap().entrySet()) {
        columnBuilder.setFamily(ByteStringer.wrap(family.getKey()));
        NavigableSet<byte []> qualifiers = family.getValue();
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte [] qualifier: qualifiers) {
            columnBuilder.addQualifier(ByteStringer.wrap(qualifier));
          }
        }
        scanBuilder.addColumn(columnBuilder.build());
      }
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      scanBuilder.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      scanBuilder.setStoreOffset(scan.getRowOffsetPerColumnFamily());
    }
    if (scan.isReversed()) {
      scanBuilder.setReversed(scan.isReversed());
    }
    if (scan.getConsistency() == Consistency.TIMELINE) {
      scanBuilder.setConsistency(toConsistency(scan.getConsistency()));
    }
    if (scan.getCaching() > 0) {
      scanBuilder.setCaching(scan.getCaching());
    }
    return scanBuilder.build();
  }

  /**
   * Convert a protocol buffer Scan to a client Scan
   *
   * @param proto the protocol buffer Scan to convert
   * @return the converted client Scan
   * @throws IOException
   */
  public static Scan toScan(
          final ClientProtos.Scan proto) throws IOException {
    byte [] startRow = HConstants.EMPTY_START_ROW;
    byte [] stopRow  = HConstants.EMPTY_END_ROW;
    if (proto.hasStartRow()) {
      startRow = proto.getStartRow().toByteArray();
    }
    if (proto.hasStopRow()) {
      stopRow = proto.getStopRow().toByteArray();
    }
    Scan scan = new Scan(startRow, stopRow);
    if (proto.hasCacheBlocks()) {
      scan.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.hasMaxVersions()) {
      scan.setMaxVersions(proto.getMaxVersions());
    }
    if (proto.hasStoreLimit()) {
      scan.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.hasStoreOffset()) {
      scan.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.hasLoadColumnFamiliesOnDemand()) {
      scan.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
    }
    if (proto.hasTimeRange()) {
      HBaseProtos.TimeRange timeRange = proto.getTimeRange();
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.hasFrom()) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.hasTo()) {
        maxStamp = timeRange.getTo();
      }
      scan.setTimeRange(minStamp, maxStamp);
    }
    if (proto.hasFilter()) {
      FilterProtos.Filter filter = proto.getFilter();
      scan.setFilter(ProtobufUtil.toFilter(filter));
    }
    if (proto.hasBatchSize()) {
      scan.setBatch(proto.getBatchSize());
    }
    if (proto.hasMaxResultSize()) {
      scan.setMaxResultSize(proto.getMaxResultSize());
    }
    if (proto.hasSmall()) {
      scan.setSmall(proto.getSmall());
    }
    for (HBaseProtos.NameBytesPair attribute: proto.getAttributeList()) {
      scan.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      for (ClientProtos.Column column: proto.getColumnList()) {
        byte[] family = column.getFamily().toByteArray();
        if (column.getQualifierCount() > 0) {
          for (ByteString qualifier: column.getQualifierList()) {
            scan.addColumn(family, qualifier.toByteArray());
          }
        } else {
          scan.addFamily(family);
        }
      }
    }
    if (proto.hasReversed()) {
      scan.setReversed(proto.getReversed());
    }
    if (proto.hasConsistency()) {
      scan.setConsistency(toConsistency(proto.getConsistency()));
    }
    if (proto.hasCaching()) {
      scan.setCaching(proto.getCaching());
    }
    return scan;
  }

  /**
   * Create a protocol buffer Get based on a client Get.
   *
   * @param get the client Get
   * @return a protocol buffer Get
   * @throws IOException
   */
  public static ClientProtos.Get toGet(
          final Get get) throws IOException {
    ClientProtos.Get.Builder builder =
            ClientProtos.Get.newBuilder();
    builder.setRow(ByteStringer.wrap(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getFilter() != null) {
      builder.setFilter(ProtobufUtil.toFilter(get.getFilter()));
    }
    TimeRange timeRange = get.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
              HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      ClientProtos.Column.Builder columnBuilder = ClientProtos.Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family: families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(ByteStringer.wrap(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier: qualifiers) {
            columnBuilder.addQualifier(ByteStringer.wrap(qualifier));
          }
        }
        builder.addColumn(columnBuilder.build());
      }
    }
    if (get.getMaxResultsPerColumnFamily() >= 0) {
      builder.setStoreLimit(get.getMaxResultsPerColumnFamily());
    }
    if (get.getRowOffsetPerColumnFamily() > 0) {
      builder.setStoreOffset(get.getRowOffsetPerColumnFamily());
    }
    if (get.isCheckExistenceOnly()){
      builder.setExistenceOnly(true);
    }
    if (get.isClosestRowBefore()){
      builder.setClosestRowBefore(true);
    }
    if (get.getConsistency() != null && get.getConsistency() != Consistency.STRONG) {
      builder.setConsistency(toConsistency(get.getConsistency()));
    }

    return builder.build();
  }

  /**
   * Convert a client Increment to a protobuf Mutate.
   *
   * @param increment
   * @return the converted mutate
   */
  public static ClientProtos.MutationProto toMutation(
          final Increment increment, final ClientProtos.MutationProto.Builder builder, long nonce) {
    builder.setRow(ByteStringer.wrap(increment.getRow()));
    builder.setMutateType(ClientProtos.MutationProto.MutationType.INCREMENT);
    builder.setDurability(toDurability(increment.getDurability()));
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    TimeRange timeRange = increment.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
              HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    ClientProtos.MutationProto.ColumnValue.Builder columnBuilder = ClientProtos.MutationProto.ColumnValue.newBuilder();
    ClientProtos.MutationProto.ColumnValue.QualifierValue.Builder valueBuilder = ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder();
    for (Map.Entry<byte[], List<Cell>> family: increment.getFamilyCellMap().entrySet()) {
      columnBuilder.setFamily(ByteStringer.wrap(family.getKey()));
      columnBuilder.clearQualifierValue();
      List<Cell> values = family.getValue();
      if (values != null && values.size() > 0) {
        for (Cell cell: values) {
          valueBuilder.clear();
          valueBuilder.setQualifier(ByteStringer.wrap(
                  cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
          valueBuilder.setValue(ByteStringer.wrap(
                  cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
          if (cell.getTagsLength() > 0) {
            valueBuilder.setTags(ByteStringer.wrap(cell.getTagsArray(),
                    cell.getTagsOffset(), cell.getTagsLength()));
          }
          columnBuilder.addQualifierValue(valueBuilder.build());
        }
      }
      builder.addColumnValue(columnBuilder.build());
    }
    Map<String, byte[]> attributes = increment.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param type
   * @param mutation
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static ClientProtos.MutationProto toMutation(final ClientProtos.MutationProto.MutationType type, final Mutation mutation,
                                                      final long nonce) throws IOException {
    return toMutation(type, mutation, ClientProtos.MutationProto.newBuilder(), nonce);
  }

  public static ClientProtos.MutationProto toMutation(final ClientProtos.MutationProto.MutationType type, final Mutation mutation,
                                                      ClientProtos.MutationProto.Builder builder) throws IOException {
    return toMutation(type, mutation, builder, HConstants.NO_NONCE);
  }

  @SuppressWarnings("deprecation")
  public static ClientProtos.MutationProto toMutation(final ClientProtos.MutationProto.MutationType type, final Mutation mutation,
                                                      ClientProtos.MutationProto.Builder builder, long nonce)
          throws IOException {
    builder = getMutationBuilderAndSetCommonFields(type, mutation, builder);
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    ClientProtos.MutationProto.ColumnValue.Builder columnBuilder = ClientProtos.MutationProto.ColumnValue.newBuilder();
    ClientProtos.MutationProto.ColumnValue.QualifierValue.Builder valueBuilder = ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder();
    for (Map.Entry<byte[],List<Cell>> family: mutation.getFamilyCellMap().entrySet()) {
      columnBuilder.clear();
      columnBuilder.setFamily(ByteStringer.wrap(family.getKey()));
      for (Cell cell: family.getValue()) {
        valueBuilder.clear();
        valueBuilder.setQualifier(ByteStringer.wrap(
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
        valueBuilder.setValue(ByteStringer.wrap(
                cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        valueBuilder.setTimestamp(cell.getTimestamp());
        if(cell.getTagsLength() > 0) {
          valueBuilder.setTags(ByteStringer.wrap(cell.getTagsArray(), cell.getTagsOffset(),
                  cell.getTagsLength()));
        }
        if (type == ClientProtos.MutationProto.MutationType.DELETE || (type == ClientProtos.MutationProto.MutationType.PUT && CellUtil.isDelete(cell))) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(cell.getTypeByte());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      builder.addColumnValue(columnBuilder.build());
    }
    return builder.build();
  }

  public static ClientProtos.MutationProto toMutationNoData(final ClientProtos.MutationProto.MutationType type, final Mutation mutation,
                                                            final ClientProtos.MutationProto.Builder builder, long nonce) throws IOException {
    getMutationBuilderAndSetCommonFields(type, mutation, builder);
    builder.setAssociatedCellCount(mutation.size());
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    return builder.build();
  }

  /**
   * Code shared by {@link ProtobufUtil#toMutation} and
   * {@link ProtobufUtil#toMutationNoData(ClientProtos.MutationProto.MutationType,
   * Mutation)}
   * @param type
   * @param mutation
   * @return A partly-filled out protobuf'd Mutation.
   */
  private static ClientProtos.MutationProto.Builder getMutationBuilderAndSetCommonFields(final ClientProtos.MutationProto.MutationType type,
                                                                                         final Mutation mutation, ClientProtos.MutationProto.Builder builder) {
    builder.setRow(ByteStringer.wrap(mutation.getRow()));
    builder.setMutateType(type);
    builder.setDurability(toDurability(mutation.getDurability()));
    builder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    return builder;
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final Result result) {
    if (result.getExists() != null) {
      return toResult(result.getExists(), result.isStale());
    }

    Cell[] cells = result.rawCells();
    if (cells == null || cells.length == 0) {
      return result.isStale() ? EMPTY_RESULT_PB_STALE : EMPTY_RESULT_PB;
    }

    ClientProtos.Result.Builder builder = ClientProtos.Result.newBuilder();
    for (Cell c : cells) {
      builder.addCell(toCell(c));
    }

    builder.setStale(result.isStale());
    builder.setPartial(result.isPartial());

    return builder.build();
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param existence the client existence to send
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final boolean existence, boolean stale) {
    if (stale){
      return existence ? EMPTY_RESULT_PB_EXISTS_TRUE_STALE : EMPTY_RESULT_PB_EXISTS_FALSE_STALE;
    } else {
      return existence ? EMPTY_RESULT_PB_EXISTS_TRUE : EMPTY_RESULT_PB_EXISTS_FALSE;
    }
  }

  /**
   * Convert a client Result to a protocol buffer Result.
   * The pb Result does not include the Cell data.  That is for transport otherwise.
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResultNoData(final Result result) {
    if (result.getExists() != null) return toResult(result.getExists(), result.isStale());
    int size = result.size();
    if (size == 0) return result.isStale() ? EMPTY_RESULT_PB_STALE : EMPTY_RESULT_PB;
    ClientProtos.Result.Builder builder = ClientProtos.Result.newBuilder();
    builder.setAssociatedCellCount(size);
    builder.setStale(result.isStale());
    return builder.build();
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @return the converted client Result
   */
  public static Result toResult(final ClientProtos.Result proto) {
    if (proto.hasExists()) {
      if (proto.getStale()) {
        return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE_STALE :EMPTY_RESULT_EXISTS_FALSE_STALE;
      }
      return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE : EMPTY_RESULT_EXISTS_FALSE;
    }

    List<CellProtos.Cell> values = proto.getCellList();
    if (values.isEmpty()){
      return proto.getStale() ? EMPTY_RESULT_STALE : EMPTY_RESULT;
    }

    List<Cell> cells = new ArrayList<Cell>(values.size());
    for (CellProtos.Cell c : values) {
      cells.add(toCell(c));
    }
    return Result.create(cells, null, proto.getStale(), proto.getPartial());
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @param scanner Optional cell scanner.
   * @return the converted client Result
   * @throws IOException
   */
  public static Result toResult(final ClientProtos.Result proto, final CellScanner scanner)
          throws IOException {
    List<CellProtos.Cell> values = proto.getCellList();

    if (proto.hasExists()) {
      if ((values != null && !values.isEmpty()) ||
              (proto.hasAssociatedCellCount() && proto.getAssociatedCellCount() > 0)) {
        throw new IllegalArgumentException("bad proto: exists with cells is no allowed " + proto);
      }
      if (proto.getStale()) {
        return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE_STALE :EMPTY_RESULT_EXISTS_FALSE_STALE;
      }
      return proto.getExists() ? EMPTY_RESULT_EXISTS_TRUE : EMPTY_RESULT_EXISTS_FALSE;
    }

    // TODO: Unit test that has some Cells in scanner and some in the proto.
    List<Cell> cells = null;
    if (proto.hasAssociatedCellCount()) {
      int count = proto.getAssociatedCellCount();
      cells = new ArrayList<Cell>(count + values.size());
      for (int i = 0; i < count; i++) {
        if (!scanner.advance()) throw new IOException("Failed get " + i + " of " + count);
        cells.add(scanner.current());
      }
    }

    if (!values.isEmpty()){
      if (cells == null) cells = new ArrayList<Cell>(values.size());
      for (CellProtos.Cell c: values) {
        cells.add(toCell(c));
      }
    }

    return (cells == null || cells.isEmpty())
            ? (proto.getStale() ? EMPTY_RESULT_STALE : EMPTY_RESULT)
            : Result.create(cells, null, proto.getStale());
  }


  /**
   * Convert a ByteArrayComparable to a protocol buffer Comparator
   *
   * @param comparator the ByteArrayComparable to convert
   * @return the converted protocol buffer Comparator
   */
  public static ComparatorProtos.Comparator toComparator(ByteArrayComparable comparator) {
    ComparatorProtos.Comparator.Builder builder = ComparatorProtos.Comparator.newBuilder();
    builder.setName(comparator.getClass().getName());
    builder.setSerializedComparator(ByteStringer.wrap(comparator.toByteArray()));
    return builder.build();
  }

  /**
   * Convert a protocol buffer Comparator to a ByteArrayComparable
   *
   * @param proto the protocol buffer Comparator to convert
   * @return the converted ByteArrayComparable
   */
  @SuppressWarnings("unchecked")
  public static ByteArrayComparable toComparator(ComparatorProtos.Comparator proto)
          throws IOException {
    String type = proto.getName();
    String funcName = "parseFrom";
    byte [] value = proto.getSerializedComparator().toByteArray();
    try {
      Class<? extends ByteArrayComparable> c =
              (Class<? extends ByteArrayComparable>)Class.forName(type, true, CLASS_LOADER);
      Method parseFrom = c.getMethod(funcName, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + funcName + " in type: " + type);
      }
      return (ByteArrayComparable)parseFrom.invoke(null, value);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert a protocol buffer Filter to a client Filter
   *
   * @param proto the protocol buffer Filter to convert
   * @return the converted Filter
   */
  @SuppressWarnings("unchecked")
  public static Filter toFilter(FilterProtos.Filter proto) throws IOException {
    String type = proto.getName();
    final byte [] value = proto.getSerializedFilter().toByteArray();
    String funcName = "parseFrom";
    try {
      Class<? extends Filter> c =
              (Class<? extends Filter>)Class.forName(type, true, CLASS_LOADER);
      Method parseFrom = c.getMethod(funcName, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + funcName + " in type: " + type);
      }
      return (Filter)parseFrom.invoke(c, value);
    } catch (Exception e) {
      // Either we couldn't instantiate the method object, or "parseFrom" failed.
      // In either case, let's not retry.
      throw new DoNotRetryIOException(e);
    }
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static FilterProtos.Filter toFilter(Filter filter) throws IOException {
    FilterProtos.Filter.Builder builder = FilterProtos.Filter.newBuilder();
    builder.setName(filter.getClass().getName());
    builder.setSerializedFilter(ByteStringer.wrap(filter.toByteArray()));
    return builder.build();
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type
   * @return protocol buffer DeleteType
   * @throws IOException
   */
  public static ClientProtos.MutationProto.DeleteType toDeleteType(
          KeyValue.Type type) throws IOException {
    switch (type) {
      case Delete:
        return ClientProtos.MutationProto.DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return ClientProtos.MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamily:
        return ClientProtos.MutationProto.DeleteType.DELETE_FAMILY;
      case DeleteFamilyVersion:
        return ClientProtos.MutationProto.DeleteType.DELETE_FAMILY_VERSION;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }

  /**
   * Convert a protocol buffer DeleteType to delete KeyValue type.
   *
   * @param type The DeleteType
   * @return The type.
   * @throws IOException
   */
  public static KeyValue.Type fromDeleteType(
          ClientProtos.MutationProto.DeleteType type) throws IOException {
    switch (type) {
      case DELETE_ONE_VERSION:
        return KeyValue.Type.Delete;
      case DELETE_MULTIPLE_VERSIONS:
        return KeyValue.Type.DeleteColumn;
      case DELETE_FAMILY:
        return KeyValue.Type.DeleteFamily;
      case DELETE_FAMILY_VERSION:
        return KeyValue.Type.DeleteFamilyVersion;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }

  /**
   * Convert a stringified protocol buffer exception Parameter to a Java Exception
   *
   * @param parameter the protocol buffer Parameter to convert
   * @return the converted Exception
   * @throws IOException if failed to deserialize the parameter
   */
  @SuppressWarnings("unchecked")
  public static Throwable toException(final HBaseProtos.NameBytesPair parameter) throws IOException {
    if (parameter == null || !parameter.hasValue()) return null;
    String desc = parameter.getValue().toStringUtf8();
    String type = parameter.getName();
    try {
      Class<? extends Throwable> c =
              (Class<? extends Throwable>)Class.forName(type, true, CLASS_LOADER);
      Constructor<? extends Throwable> cn = null;
      try {
        cn = c.getDeclaredConstructor(String.class);
        return cn.newInstance(desc);
      } catch (NoSuchMethodException e) {
        // Could be a raw RemoteException. See HBASE-8987.
        cn = c.getDeclaredConstructor(String.class, String.class);
        return cn.newInstance(type, desc);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Converts a Permission proto to a client Permission object.
   *
   * @param proto the protobuf Permission
   * @return the converted Permission
   */
  public static Permission toPermission(AccessControlProtos.Permission proto) {
    if (proto.getType() != AccessControlProtos.Permission.Type.Global) {
      return toTablePermission(proto);
    } else {
      List<Permission.Action> actions = toPermissionActions(proto.getGlobalPermission().getActionList());
      return new Permission(actions.toArray(new Permission.Action[actions.size()]));
    }
  }

  /**
   * Converts a Permission proto to a client TablePermission object.
   *
   * @param proto the protobuf Permission
   * @return the converted TablePermission
   */
  public static TablePermission toTablePermission(AccessControlProtos.Permission proto) {
    if(proto.getType() == AccessControlProtos.Permission.Type.Global) {
      AccessControlProtos.GlobalPermission perm = proto.getGlobalPermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      return new TablePermission(null, null, null,
              actions.toArray(new Permission.Action[actions.size()]));
    }
    if(proto.getType() == AccessControlProtos.Permission.Type.Namespace) {
      AccessControlProtos.NamespacePermission perm = proto.getNamespacePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      if(!proto.hasNamespacePermission()) {
        throw new IllegalStateException("Namespace must not be empty in NamespacePermission");
      }
      String namespace = perm.getNamespaceName().toStringUtf8();
      return new TablePermission(namespace, actions.toArray(new Permission.Action[actions.size()]));
    }
    if(proto.getType() == AccessControlProtos.Permission.Type.Table) {
      AccessControlProtos.TablePermission perm = proto.getTablePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      byte[] qualifier = null;
      byte[] family = null;
      TableName table = null;

      if (!perm.hasTableName()) {
        throw new IllegalStateException("TableName cannot be empty");
      }
      table = ProtobufUtil.toTableName(perm.getTableName());

      if (perm.hasFamily()) family = perm.getFamily().toByteArray();
      if (perm.hasQualifier()) qualifier = perm.getQualifier().toByteArray();

      return new TablePermission(table, family, qualifier,
              actions.toArray(new Permission.Action[actions.size()]));
    }
    throw new IllegalStateException("Unrecognize Perm Type: "+proto.getType());
  }

  /**
   * Convert a client Permission to a Permission proto
   *
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    AccessControlProtos.Permission.Builder ret = AccessControlProtos.Permission.newBuilder();
    if (perm instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission)perm;
      if(tablePerm.hasNamespace()) {
        ret.setType(AccessControlProtos.Permission.Type.Namespace);

        AccessControlProtos.NamespacePermission.Builder builder =
                AccessControlProtos.NamespacePermission.newBuilder();
        builder.setNamespaceName(ByteString.copyFromUtf8(tablePerm.getNamespace()));
        Permission.Action actions[] = perm.getActions();
        if (actions != null) {
          for (Permission.Action a : actions) {
            builder.addAction(toPermissionAction(a));
          }
        }
        ret.setNamespacePermission(builder);
        return ret.build();
      } else if (tablePerm.hasTable()) {
        ret.setType(AccessControlProtos.Permission.Type.Table);

        AccessControlProtos.TablePermission.Builder builder =
                AccessControlProtos.TablePermission.newBuilder();
        builder.setTableName(ProtobufUtil.toProtoTableName(tablePerm.getTableName()));
        if (tablePerm.hasFamily()) {
          builder.setFamily(ByteStringer.wrap(tablePerm.getFamily()));
        }
        if (tablePerm.hasQualifier()) {
          builder.setQualifier(ByteStringer.wrap(tablePerm.getQualifier()));
        }
        Permission.Action actions[] = perm.getActions();
        if (actions != null) {
          for (Permission.Action a : actions) {
            builder.addAction(toPermissionAction(a));
          }
        }
        ret.setTablePermission(builder);
        return ret.build();
      }
    }

    ret.setType(AccessControlProtos.Permission.Type.Global);

    AccessControlProtos.GlobalPermission.Builder builder =
            AccessControlProtos.GlobalPermission.newBuilder();
    Permission.Action actions[] = perm.getActions();
    if (actions != null) {
      for (Permission.Action a: actions) {
        builder.addAction(toPermissionAction(a));
      }
    }
    ret.setGlobalPermission(builder);
    return ret.build();
  }

  /**
   * Converts a list of Permission.Action proto to a list of client Permission.Action objects.
   *
   * @param protoActions the list of protobuf Actions
   * @return the converted list of Actions
   */
  public static List<Permission.Action> toPermissionActions(
          List<AccessControlProtos.Permission.Action> protoActions) {
    List<Permission.Action> actions = new ArrayList<Permission.Action>(protoActions.size());
    for (AccessControlProtos.Permission.Action a : protoActions) {
      actions.add(toPermissionAction(a));
    }
    return actions;
  }

  /**
   * Converts a Permission.Action proto to a client Permission.Action object.
   *
   * @param action the protobuf Action
   * @return the converted Action
   */
  public static Permission.Action toPermissionAction(
          AccessControlProtos.Permission.Action action) {
    switch (action) {
      case READ:
        return Permission.Action.READ;
      case WRITE:
        return Permission.Action.WRITE;
      case EXEC:
        return Permission.Action.EXEC;
      case CREATE:
        return Permission.Action.CREATE;
      case ADMIN:
        return Permission.Action.ADMIN;
    }
    throw new IllegalArgumentException("Unknown action value "+action.name());
  }

  /**
   * Convert a client Permission.Action to a Permission.Action proto
   *
   * @param action the client Action
   * @return the protobuf Action
   */
  public static AccessControlProtos.Permission.Action toPermissionAction(
          Permission.Action action) {
    switch (action) {
      case READ:
        return AccessControlProtos.Permission.Action.READ;
      case WRITE:
        return AccessControlProtos.Permission.Action.WRITE;
      case EXEC:
        return AccessControlProtos.Permission.Action.EXEC;
      case CREATE:
        return AccessControlProtos.Permission.Action.CREATE;
      case ADMIN:
        return AccessControlProtos.Permission.Action.ADMIN;
    }
    throw new IllegalArgumentException("Unknown action value "+action.name());
  }

  /**
   * Convert a client user permission to a user permission proto
   *
   * @param perm the client UserPermission
   * @return the protobuf UserPermission
   */
  public static AccessControlProtos.UserPermission toUserPermission(UserPermission perm) {
    return AccessControlProtos.UserPermission.newBuilder()
            .setUser(ByteStringer.wrap(perm.getUser()))
            .setPermission(toPermission(perm))
            .build();
  }

  /**
   * Converts a user permission proto to a client user permission object.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(AccessControlProtos.UserPermission proto) {
    return new UserPermission(proto.getUser().toByteArray(),
            toTablePermission(proto.getPermission()));
  }

  /**
   * Convert a ListMultimap&lt;String, TablePermission&gt; where key is username
   * to a protobuf UserPermission
   *
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static AccessControlProtos.UsersAndPermissions toUserTablePermissions(
          ListMultimap<String, TablePermission> perm) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
            AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<TablePermission>> entry : perm.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
              AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (TablePermission tablePerm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(tablePerm));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  /**
   * Convert a protobuf UserTablePermissions to a
   * ListMultimap&lt;String, TablePermission&gt; where key is username.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static ListMultimap<String, TablePermission> toUserTablePermissions(
          AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;

    for (int i = 0; i < proto.getUserPermissionsCount(); i++) {
      userPerm = proto.getUserPermissions(i);
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        TablePermission tablePerm = toTablePermission(userPerm.getPermissions(j));
        perms.put(userPerm.getUser().toStringUtf8(), tablePerm);
      }
    }

    return perms;
  }

  /**
   * Converts a Token instance (with embedded identifier) to the protobuf representation.
   *
   * @param token the Token instance to copy
   * @return the protobuf Token message
   */
  public static AuthenticationProtos.Token toToken(Token<AuthenticationTokenIdentifier> token) {
    AuthenticationProtos.Token.Builder builder = AuthenticationProtos.Token.newBuilder();
    builder.setIdentifier(ByteStringer.wrap(token.getIdentifier()));
    builder.setPassword(ByteStringer.wrap(token.getPassword()));
    if (token.getService() != null) {
      builder.setService(ByteString.copyFromUtf8(token.getService().toString()));
    }
    return builder.build();
  }

  /**
   * Converts a protobuf Token message back into a Token instance.
   *
   * @param proto the protobuf Token message
   * @return the Token instance
   */
  public static Token<AuthenticationTokenIdentifier> toToken(AuthenticationProtos.Token proto) {
    return new Token<AuthenticationTokenIdentifier>(
            proto.hasIdentifier() ? proto.getIdentifier().toByteArray() : null,
            proto.hasPassword() ? proto.getPassword().toByteArray() : null,
            AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE,
            proto.hasService() ? new Text(proto.getService().toStringUtf8()) : null);
  }

  public static ScanMetrics toScanMetrics(final byte[] bytes) {
    Parser<MapReduceProtos.ScanMetrics> parser = MapReduceProtos.ScanMetrics.PARSER;
    MapReduceProtos.ScanMetrics pScanMetrics = null;
    try {
      pScanMetrics = parser.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      //Ignored there are just no key values to add.
    }
    ScanMetrics scanMetrics = new ScanMetrics();
    if (pScanMetrics != null) {
      for (HBaseProtos.NameInt64Pair pair : pScanMetrics.getMetricsList()) {
        if (pair.hasName() && pair.hasValue()) {
          scanMetrics.setCounter(pair.getName(), pair.getValue());
        }
      }
    }
    return scanMetrics;
  }

  public static MapReduceProtos.ScanMetrics toScanMetrics(ScanMetrics scanMetrics) {
    MapReduceProtos.ScanMetrics.Builder builder = MapReduceProtos.ScanMetrics.newBuilder();
    Map<String, Long> metrics = scanMetrics.getMetricsMap();
    for (Map.Entry<String, Long> e : metrics.entrySet()) {
      HBaseProtos.NameInt64Pair nameInt64Pair =
              HBaseProtos.NameInt64Pair.newBuilder()
                      .setName(e.getKey())
                      .setValue(e.getValue())
                      .build();
      builder.addMetrics(nameInt64Pair);
    }
    return builder.build();
  }

  public static CellProtos.Cell toCell(final Cell kv) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    CellProtos.Cell.Builder kvbuilder = CellProtos.Cell.newBuilder();
    kvbuilder.setRow(ByteStringer.wrap(kv.getRowArray(), kv.getRowOffset(),
            kv.getRowLength()));
    kvbuilder.setFamily(ByteStringer.wrap(kv.getFamilyArray(),
            kv.getFamilyOffset(), kv.getFamilyLength()));
    kvbuilder.setQualifier(ByteStringer.wrap(kv.getQualifierArray(),
            kv.getQualifierOffset(), kv.getQualifierLength()));
    kvbuilder.setCellType(CellProtos.CellType.valueOf(kv.getTypeByte()));
    kvbuilder.setTimestamp(kv.getTimestamp());
    kvbuilder.setValue(ByteStringer.wrap(kv.getValueArray(), kv.getValueOffset(),
            kv.getValueLength()));
    return kvbuilder.build();
  }

  public static Cell toCell(final CellProtos.Cell cell) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    return CellUtil.createCell(cell.getRow().toByteArray(),
            cell.getFamily().toByteArray(),
            cell.getQualifier().toByteArray(),
            cell.getTimestamp(),
            (byte)cell.getCellType().getNumber(),
            cell.getValue().toByteArray());
  }

  public static HBaseProtos.NamespaceDescriptor toProtoNamespaceDescriptor(NamespaceDescriptor ns) {
    HBaseProtos.NamespaceDescriptor.Builder b =
            HBaseProtos.NamespaceDescriptor.newBuilder()
                    .setName(ByteString.copyFromUtf8(ns.getName()));
    for(Map.Entry<String, String> entry: ns.getConfiguration().entrySet()) {
      b.addConfiguration(HBaseProtos.NameStringPair.newBuilder()
              .setName(entry.getKey())
              .setValue(entry.getValue()));
    }
    return b.build();
  }

  public static NamespaceDescriptor toNamespaceDescriptor(
          HBaseProtos.NamespaceDescriptor desc) throws IOException {
    NamespaceDescriptor.Builder b =
            NamespaceDescriptor.create(desc.getName().toStringUtf8());
    for(HBaseProtos.NameStringPair prop : desc.getConfigurationList()) {
      b.addConfiguration(prop.getName(), prop.getValue());
    }
    return b.build();
  }

  public static WALProtos.CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] family,
                                                                      List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    // compaction descriptor contains relative paths.
    // input / output paths are relative to the store dir
    // store dir is relative to region dir
    WALProtos.CompactionDescriptor.Builder builder = WALProtos.CompactionDescriptor.newBuilder()
            .setTableName(ByteStringer.wrap(info.getTable().toBytes()))
            .setEncodedRegionName(ByteStringer.wrap(info.getEncodedNameAsBytes()))
            .setFamilyName(ByteStringer.wrap(family))
            .setStoreHomeDir(storeDir.getName()); //make relative
    for (Path inputPath : inputPaths) {
      builder.addCompactionInput(inputPath.getName()); //relative path
    }
    for (Path outputPath : outputPaths) {
      builder.addCompactionOutput(outputPath.getName());
    }
    builder.setRegionName(ByteStringer.wrap(info.getRegionName()));
    return builder.build();
  }

  public static WALProtos.FlushDescriptor toFlushDescriptor(WALProtos.FlushDescriptor.FlushAction action, HRegionInfo hri,
                                                            long flushSeqId, Map<byte[], List<Path>> committedFiles) {
    WALProtos.FlushDescriptor.Builder desc = WALProtos.FlushDescriptor.newBuilder()
            .setAction(action)
            .setEncodedRegionName(ByteStringer.wrap(hri.getEncodedNameAsBytes()))
            .setRegionName(ByteStringer.wrap(hri.getRegionName()))
            .setFlushSequenceNumber(flushSeqId)
            .setTableName(ByteStringer.wrap(hri.getTable().getName()));

    for (Map.Entry<byte[], List<Path>> entry : committedFiles.entrySet()) {
      WALProtos.FlushDescriptor.StoreFlushDescriptor.Builder builder =
              WALProtos.FlushDescriptor.StoreFlushDescriptor.newBuilder()
                      .setFamilyName(ByteStringer.wrap(entry.getKey()))
                      .setStoreHomeDir(Bytes.toString(entry.getKey())); //relative to region
      if (entry.getValue() != null) {
        for (Path path : entry.getValue()) {
          builder.addFlushOutput(path.getName());
        }
      }
      desc.addStoreFlushes(builder);
    }
    return desc.build();
  }

  public static WALProtos.RegionEventDescriptor toRegionEventDescriptor(
          WALProtos.RegionEventDescriptor.EventType eventType, HRegionInfo hri, long seqId, ServerName server,
          Map<byte[], List<Path>> storeFiles) {
    WALProtos.RegionEventDescriptor.Builder desc = WALProtos.RegionEventDescriptor.newBuilder()
            .setEventType(eventType)
            .setTableName(ByteStringer.wrap(hri.getTable().getName()))
            .setEncodedRegionName(ByteStringer.wrap(hri.getEncodedNameAsBytes()))
            .setRegionName(ByteStringer.wrap(hri.getRegionName()))
            .setLogSequenceNumber(seqId)
            .setServer(toServerName(server));

    for (Map.Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      WALProtos.StoreDescriptor.Builder builder = WALProtos.StoreDescriptor.newBuilder()
              .setFamilyName(ByteStringer.wrap(entry.getKey()))
              .setStoreHomeDir(Bytes.toString(entry.getKey()));
      for (Path path : entry.getValue()) {
        builder.addStoreFile(path.getName());
      }

      desc.addStores(builder);
    }
    return desc.build();
  }

  public static TableName toTableName(HBaseProtos.TableName tableNamePB) {
    return TableName.valueOf(tableNamePB.getNamespace().asReadOnlyByteBuffer(),
            tableNamePB.getQualifier().asReadOnlyByteBuffer());
  }

  public static HBaseProtos.TableName toProtoTableName(TableName tableName) {
    return HBaseProtos.TableName.newBuilder()
            .setNamespace(ByteStringer.wrap(tableName.getNamespace()))
            .setQualifier(ByteStringer.wrap(tableName.getQualifier())).build();
  }

  /**
   * Convert a protocol buffer CellVisibility to a client CellVisibility
   *
   * @param proto
   * @return the converted client CellVisibility
   */
  public static CellVisibility toCellVisibility(ClientProtos.CellVisibility proto) {
    if (proto == null) return null;
    return new CellVisibility(proto.getExpression());
  }

  /**
   * Convert a protocol buffer CellVisibility bytes to a client CellVisibility
   *
   * @param protoBytes
   * @return the converted client CellVisibility
   * @throws DeserializationException
   */
  public static CellVisibility toCellVisibility(byte[] protoBytes) throws DeserializationException {
    if (protoBytes == null) return null;
    ClientProtos.CellVisibility.Builder builder = ClientProtos.CellVisibility.newBuilder();
    ClientProtos.CellVisibility proto = null;
    try {
      proto = builder.mergeFrom(protoBytes).build();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return toCellVisibility(proto);
  }

  /**
   * Create a protocol buffer CellVisibility based on a client CellVisibility.
   *
   * @param cellVisibility
   * @return a protocol buffer CellVisibility
   */
  public static ClientProtos.CellVisibility toCellVisibility(CellVisibility cellVisibility) {
    ClientProtos.CellVisibility.Builder builder = ClientProtos.CellVisibility.newBuilder();
    builder.setExpression(cellVisibility.getExpression());
    return builder.build();
  }

  /**
   * Convert a protocol buffer Authorizations to a client Authorizations
   *
   * @param proto
   * @return the converted client Authorizations
   */
  public static Authorizations toAuthorizations(ClientProtos.Authorizations proto) {
    if (proto == null) return null;
    return new Authorizations(proto.getLabelList());
  }

  /**
   * Convert a protocol buffer Authorizations bytes to a client Authorizations
   *
   * @param protoBytes
   * @return the converted client Authorizations
   * @throws DeserializationException
   */
  public static Authorizations toAuthorizations(byte[] protoBytes) throws DeserializationException {
    if (protoBytes == null) return null;
    ClientProtos.Authorizations.Builder builder = ClientProtos.Authorizations.newBuilder();
    ClientProtos.Authorizations proto = null;
    try {
      proto = builder.mergeFrom(protoBytes).build();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return toAuthorizations(proto);
  }

  /**
   * Create a protocol buffer Authorizations based on a client Authorizations.
   *
   * @param authorizations
   * @return a protocol buffer Authorizations
   */
  public static ClientProtos.Authorizations toAuthorizations(Authorizations authorizations) {
    ClientProtos.Authorizations.Builder builder = ClientProtos.Authorizations.newBuilder();
    for (String label : authorizations.getLabels()) {
      builder.addLabel(label);
    }
    return builder.build();
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(String user,
                                                                              Permission perms) {
    return AccessControlProtos.UsersAndPermissions.newBuilder()
            .addUserPermissions(AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder()
                    .setUser(ByteString.copyFromUtf8(user))
                    .addPermissions(toPermission(perms))
                    .build())
            .build();
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(
          ListMultimap<String, Permission> perms) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
            AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<Permission>> entry : perms.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
              AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (Permission perm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(perm));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  public static ListMultimap<String, Permission> toUsersAndPermissions(
          AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, Permission> result = ArrayListMultimap.create();
    for (AccessControlProtos.UsersAndPermissions.UserPermissions userPerms:
            proto.getUserPermissionsList()) {
      String user = userPerms.getUser().toStringUtf8();
      for (AccessControlProtos.Permission perm: userPerms.getPermissionsList()) {
        result.put(user, toPermission(perm));
      }
    }
    return result;
  }

  /**
   * Convert a protocol buffer TimeUnit to a client TimeUnit
   *
   * @param proto
   * @return the converted client TimeUnit
   */
  public static TimeUnit toTimeUnit(final HBaseProtos.TimeUnit proto) {
    switch (proto) {
      case NANOSECONDS:  return TimeUnit.NANOSECONDS;
      case MICROSECONDS: return TimeUnit.MICROSECONDS;
      case MILLISECONDS: return TimeUnit.MILLISECONDS;
      case SECONDS:      return TimeUnit.SECONDS;
      case MINUTES:      return TimeUnit.MINUTES;
      case HOURS:        return TimeUnit.HOURS;
      case DAYS:         return TimeUnit.DAYS;
    }
    throw new RuntimeException("Invalid TimeUnit " + proto);
  }

  /**
   * Convert a client TimeUnit to a protocol buffer TimeUnit
   *
   * @param timeUnit
   * @return the converted protocol buffer TimeUnit
   */
  public static HBaseProtos.TimeUnit toProtoTimeUnit(final TimeUnit timeUnit) {
    switch (timeUnit) {
      case NANOSECONDS:  return HBaseProtos.TimeUnit.NANOSECONDS;
      case MICROSECONDS: return HBaseProtos.TimeUnit.MICROSECONDS;
      case MILLISECONDS: return HBaseProtos.TimeUnit.MILLISECONDS;
      case SECONDS:      return HBaseProtos.TimeUnit.SECONDS;
      case MINUTES:      return HBaseProtos.TimeUnit.MINUTES;
      case HOURS:        return HBaseProtos.TimeUnit.HOURS;
      case DAYS:         return HBaseProtos.TimeUnit.DAYS;
    }
    throw new RuntimeException("Invalid TimeUnit " + timeUnit);
  }

  /**
   * Convert a protocol buffer ThrottleType to a client ThrottleType
   *
   * @param proto
   * @return the converted client ThrottleType
   */
  public static ThrottleType toThrottleType(final QuotaProtos.ThrottleType proto) {
    switch (proto) {
      case REQUEST_NUMBER: return ThrottleType.REQUEST_NUMBER;
      case REQUEST_SIZE:   return ThrottleType.REQUEST_SIZE;
      case WRITE_NUMBER:   return ThrottleType.WRITE_NUMBER;
      case WRITE_SIZE:     return ThrottleType.WRITE_SIZE;
      case READ_NUMBER:    return ThrottleType.READ_NUMBER;
      case READ_SIZE:      return ThrottleType.READ_SIZE;
    }
    throw new RuntimeException("Invalid ThrottleType " + proto);
  }

  /**
   * Convert a client ThrottleType to a protocol buffer ThrottleType
   *
   * @param type
   * @return the converted protocol buffer ThrottleType
   */
  public static QuotaProtos.ThrottleType toProtoThrottleType(final ThrottleType type) {
    switch (type) {
      case REQUEST_NUMBER: return QuotaProtos.ThrottleType.REQUEST_NUMBER;
      case REQUEST_SIZE:   return QuotaProtos.ThrottleType.REQUEST_SIZE;
      case WRITE_NUMBER:   return QuotaProtos.ThrottleType.WRITE_NUMBER;
      case WRITE_SIZE:     return QuotaProtos.ThrottleType.WRITE_SIZE;
      case READ_NUMBER:    return QuotaProtos.ThrottleType.READ_NUMBER;
      case READ_SIZE:      return QuotaProtos.ThrottleType.READ_SIZE;
    }
    throw new RuntimeException("Invalid ThrottleType " + type);
  }

  /**
   * Convert a protocol buffer QuotaScope to a client QuotaScope
   *
   * @param proto
   * @return the converted client QuotaScope
   */
  public static QuotaScope toQuotaScope(final QuotaProtos.QuotaScope proto) {
    switch (proto) {
      case CLUSTER: return QuotaScope.CLUSTER;
      case MACHINE: return QuotaScope.MACHINE;
    }
    throw new RuntimeException("Invalid QuotaScope " + proto);
  }

  /**
   * Convert a client QuotaScope to a protocol buffer QuotaScope
   *
   * @param scope
   * @return the converted protocol buffer QuotaScope
   */
  public static QuotaProtos.QuotaScope toProtoQuotaScope(final QuotaScope scope) {
    switch (scope) {
      case CLUSTER: return QuotaProtos.QuotaScope.CLUSTER;
      case MACHINE: return QuotaProtos.QuotaScope.MACHINE;
    }
    throw new RuntimeException("Invalid QuotaScope " + scope);
  }

  /**
   * Convert a protocol buffer QuotaType to a client QuotaType
   *
   * @param proto
   * @return the converted client QuotaType
   */
  public static QuotaType toQuotaScope(final QuotaProtos.QuotaType proto) {
    switch (proto) {
      case THROTTLE: return QuotaType.THROTTLE;
    }
    throw new RuntimeException("Invalid QuotaType " + proto);
  }

  /**
   * Convert a client QuotaType to a protocol buffer QuotaType
   *
   * @param type
   * @return the converted protocol buffer QuotaType
   */
  public static QuotaProtos.QuotaType toProtoQuotaScope(final QuotaType type) {
    switch (type) {
      case THROTTLE: return QuotaProtos.QuotaType.THROTTLE;
    }
    throw new RuntimeException("Invalid QuotaType " + type);
  }

  /**
   * Build a protocol buffer TimedQuota
   *
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the quota scope
   * @return the protocol buffer TimedQuota
   */
  public static QuotaProtos.TimedQuota toTimedQuota(final long limit, final TimeUnit timeUnit,
                                                    final QuotaScope scope) {
    return QuotaProtos.TimedQuota.newBuilder()
            .setSoftLimit(limit)
            .setTimeUnit(toProtoTimeUnit(timeUnit))
            .setScope(toProtoQuotaScope(scope))
            .build();
  }

  /**
   * Generates a marker for the WAL so that we propagate the notion of a bulk region load
   * throughout the WAL.
   *
   * @param tableName         The tableName into which the bulk load is being imported into.
   * @param encodedRegionName Encoded region name of the region which is being bulk loaded.
   * @param storeFiles        A set of store files of a column family are bulk loaded.
   * @param bulkloadSeqId     sequence ID (by a force flush) used to create bulk load hfile
   *                          name
   * @return The WAL log marker for bulk loads.
   */
  public static WALProtos.BulkLoadDescriptor toBulkLoadDescriptor(TableName tableName,
                                                                  ByteString encodedRegionName, Map<byte[], List<Path>> storeFiles, long bulkloadSeqId) {
    WALProtos.BulkLoadDescriptor.Builder desc = WALProtos.BulkLoadDescriptor.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setEncodedRegionName(encodedRegionName).setBulkloadSeqNum(bulkloadSeqId);

    for (Map.Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      WALProtos.StoreDescriptor.Builder builder = WALProtos.StoreDescriptor.newBuilder()
              .setFamilyName(ByteStringer.wrap(entry.getKey()))
              .setStoreHomeDir(Bytes.toString(entry.getKey())); // relative to region
      for (Path path : entry.getValue()) {
        builder.addStoreFile(path.getName());
      }
      desc.addStores(builder);
    }

    return desc.build();
  }

  /**
   * Print out some subset of a MutationProto rather than all of it and its data
   * @param proto Protobuf to print out
   * @return Short String of mutation proto
   */
  static String toShortString(final ClientProtos.MutationProto proto) {
    return "row=" + Bytes.toString(proto.getRow().toByteArray()) +
            ", type=" + proto.getMutateType().toString();
  }

}
