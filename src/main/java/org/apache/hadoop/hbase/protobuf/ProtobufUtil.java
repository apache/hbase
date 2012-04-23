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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UUID;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.FamilyScope;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * Protobufs utility.
 */
public final class ProtobufUtil {

  private ProtobufUtil() {
  }

  /**
   * Primitive type to class mapping.
   */
  private final static Map<String, Class<?>>
    PRIMITIVES = new HashMap<String, Class<?>>();

  static {
    PRIMITIVES.put(Boolean.TYPE.getName(), Boolean.TYPE);
    PRIMITIVES.put(Byte.TYPE.getName(), Byte.TYPE);
    PRIMITIVES.put(Character.TYPE.getName(), Character.TYPE);
    PRIMITIVES.put(Short.TYPE.getName(), Short.TYPE);
    PRIMITIVES.put(Integer.TYPE.getName(), Integer.TYPE);
    PRIMITIVES.put(Long.TYPE.getName(), Long.TYPE);
    PRIMITIVES.put(Float.TYPE.getName(), Float.TYPE);
    PRIMITIVES.put(Double.TYPE.getName(), Double.TYPE);
    PRIMITIVES.put(Void.TYPE.getName(), Void.TYPE);
  }

  /**
   * Magic we put ahead of a serialized protobuf message.
   * For example, all znode content is protobuf messages with the below magic
   * for preamble.
   */
  static final byte [] PB_MAGIC = new byte [] {'P', 'B', 'U', 'F'};

  /**
   * Prepend the passed bytes with four bytes of magic, {@link #PB_MAGIC}, to flag what
   * follows as a protobuf in hbase.  Prepend these bytes to all content written to znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</codes> with magic prepended (Creates a new
   * byte array that is <code>bytes.length</code> plus {@link #PB_MAGIC}.length.
   */
  public static byte [] prependPBMagic(final byte [] bytes) {
    return Bytes.add(PB_MAGIC, bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @return True if passed <code>bytes</code> has {@link #PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes) {
    if (bytes == null || bytes.length <= PB_MAGIC.length) return false;
    return Bytes.compareTo(PB_MAGIC, 0, PB_MAGIC.length, bytes, 0, PB_MAGIC.length) == 0;
  }

  /**
   * @return Length of {@link #PB_MAGIC}
   */
  public static int lengthOfPBMagic() {
    return PB_MAGIC.length;
  }

  /**
   * Return the IOException thrown by the remote server wrapped in
   * ServiceException as cause.
   *
   * @param se ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or
   *   a new IOException that wraps the unexpected ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  /**
   * Convert a protocol buffer Exec to a client Exec
   *
   * @param proto the protocol buffer Exec to convert
   * @return the converted client Exec
   */
  @SuppressWarnings("unchecked")
  public static Exec toExec(
      final ClientProtos.Exec proto) throws IOException {
    byte[] row = proto.getRow().toByteArray();
    String protocolName = proto.getProtocolName();
    String methodName = proto.getMethodName();
    List<Object> parameters = new ArrayList<Object>();
    Class<? extends CoprocessorProtocol> protocol = null;
    Method method = null;
    try {
      List<Class<?>> types = new ArrayList<Class<?>>();
      for (NameBytesPair parameter: proto.getParameterList()) {
        String type = parameter.getName();
        Class<?> declaredClass = PRIMITIVES.get(type);
        if (declaredClass == null) {
          declaredClass = Class.forName(parameter.getName());
        }
        parameters.add(toObject(parameter));
        types.add(declaredClass);
      }
      Class<?> [] parameterTypes = new Class<?> [types.size()];
      types.toArray(parameterTypes);
      protocol = (Class<? extends CoprocessorProtocol>)
        Class.forName(protocolName);
      method = protocol.getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException nsme) {
      throw new IOException(nsme);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }
    Configuration conf = HBaseConfiguration.create();
    for (NameStringPair p: proto.getPropertyList()) {
      conf.set(p.getName(), p.getValue());
    }
    Object[] parameterObjects = new Object[parameters.size()];
    parameters.toArray(parameterObjects);
    return new Exec(conf, row, protocol,
      method, parameterObjects);
  }

  /**
   * Convert a ServerName to a protocol buffer ServerName
   *
   * @param serverName the ServerName to convert
   * @return the converted protocol buffer ServerName
   */
  public static HBaseProtos.ServerName
      toServerName(final ServerName serverName) {
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
  public static ServerName toServerName(
      final HBaseProtos.ServerName proto) {
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
    return new ServerName(hostName, port, startCode);
  }

  /**
   * Convert a RegionInfo to a HRegionInfo
   *
   * @param proto the RegionInfo to convert
   * @return the converted HRegionInfo
   */
  public static HRegionInfo
      toRegionInfo(final RegionInfo proto) {
    if (proto == null) return null;
    byte[] tableName = proto.getTableName().toByteArray();
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      return HRegionInfo.ROOT_REGIONINFO;
    } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      return HRegionInfo.FIRST_META_REGIONINFO;
    }
    long regionId = proto.getRegionId();
    byte[] startKey = null;
    byte[] endKey = null;
    if (proto.hasStartKey()) {
      startKey = proto.getStartKey().toByteArray();
    }
    if (proto.hasEndKey()) {
      endKey = proto.getEndKey().toByteArray();
    }
    boolean split = false;
    if (proto.hasSplit()) {
      split = proto.getSplit();
    }
    HRegionInfo hri = new HRegionInfo(tableName,
      startKey, endKey, split, regionId);
    if (proto.hasOffline()) {
      hri.setOffline(proto.getOffline());
    }
    return hri;
  }

  /**
   * Convert a HRegionInfo to a RegionInfo
   *
   * @param info the HRegionInfo to convert
   * @return the converted RegionInfo
   */
  public static RegionInfo
      toRegionInfo(final HRegionInfo info) {
    if (info == null) return null;
    RegionInfo.Builder builder = RegionInfo.newBuilder();
    builder.setTableName(ByteString.copyFrom(info.getTableName()));
    builder.setRegionId(info.getRegionId());
    if (info.getStartKey() != null) {
        builder.setStartKey(ByteString.copyFrom(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      builder.setEndKey(ByteString.copyFrom(info.getEndKey()));
    }
    builder.setOffline(info.isOffline());
    builder.setSplit(info.isSplit());
    return builder.build();
  }

  /**
   * Convert a protocol buffer Get to a client Get
   *
   * @param get the protocol buffer Get to convert
   * @return the converted client Get
   * @throws IOException
   */
  public static Get toGet(
      final ClientProtos.Get proto) throws IOException {
    if (proto == null) return null;
    byte[] row = proto.getRow().toByteArray();
    RowLock rowLock = null;
    if (proto.hasLockId()) {
      rowLock = new RowLock(proto.getLockId());
    }
    Get get = new Get(row, rowLock);
    if (proto.hasCacheBlocks()) {
      get.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.hasMaxVersions()) {
      get.setMaxVersions(proto.getMaxVersions());
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
      NameBytesPair filter = proto.getFilter();
      get.setFilter((Filter)toObject(filter));
    }
    for (NameBytesPair attribute: proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      for (Column column: proto.getColumnList()) {
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
    return get;
  }

  /**
   * Convert a protocol buffer Mutate to a Put
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Put
   * @throws DoNotRetryIOException
   */
  public static Put toPut(
      final Mutate proto) throws DoNotRetryIOException {
    MutateType type = proto.getMutateType();
    assert type == MutateType.PUT : type.name();
    byte[] row = proto.getRow().toByteArray();
    long timestamp = HConstants.LATEST_TIMESTAMP;
    if (proto.hasTimestamp()) {
      timestamp = proto.getTimestamp();
    }
    RowLock lock = null;
    if (proto.hasLockId()) {
      lock = new RowLock(proto.getLockId());
    }
    Put put = new Put(row, timestamp, lock);
    put.setWriteToWAL(proto.getWriteToWAL());
    for (NameBytesPair attribute: proto.getAttributeList()) {
      put.setAttribute(attribute.getName(),
        attribute.getValue().toByteArray());
    }
    for (ColumnValue column: proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv: column.getQualifierValueList()) {
        byte[] qualifier = qv.getQualifier().toByteArray();
        if (!qv.hasValue()) {
          throw new DoNotRetryIOException(
            "Missing required field: qualifer value");
        }
        byte[] value = qv.getValue().toByteArray();
        long ts = timestamp;
        if (qv.hasTimestamp()) {
          ts = qv.getTimestamp();
        }
        put.add(family, qualifier, ts, value);
      }
    }
    return put;
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Delete
   */
  public static Delete toDelete(final Mutate proto) {
    MutateType type = proto.getMutateType();
    assert type == MutateType.DELETE : type.name();
    byte[] row = proto.getRow().toByteArray();
    long timestamp = HConstants.LATEST_TIMESTAMP;
    if (proto.hasTimestamp()) {
      timestamp = proto.getTimestamp();
    }
    RowLock lock = null;
    if (proto.hasLockId()) {
      lock = new RowLock(proto.getLockId());
    }
    Delete delete = new Delete(row, timestamp, lock);
    delete.setWriteToWAL(proto.getWriteToWAL());
    for (NameBytesPair attribute: proto.getAttributeList()) {
      delete.setAttribute(attribute.getName(),
        attribute.getValue().toByteArray());
    }
    for (ColumnValue column: proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv: column.getQualifierValueList()) {
        DeleteType deleteType = qv.getDeleteType();
        byte[] qualifier = null;
        if (qv.hasQualifier()) {
          qualifier = qv.getQualifier().toByteArray();
        }
        long ts = HConstants.LATEST_TIMESTAMP;
        if (qv.hasTimestamp()) {
          ts = qv.getTimestamp();
        }
        if (deleteType == DeleteType.DELETE_ONE_VERSION) {
          delete.deleteColumn(family, qualifier, ts);
        } else if (deleteType == DeleteType.DELETE_MULTIPLE_VERSIONS) {
          delete.deleteColumns(family, qualifier, ts);
        } else {
          delete.deleteFamily(family, ts);
        }
      }
    }
    return delete;
  }

  /**
   * Convert a protocol buffer Mutate to an Append
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Append
   * @throws DoNotRetryIOException
   */
  public static Append toAppend(
      final Mutate proto) throws DoNotRetryIOException {
    MutateType type = proto.getMutateType();
    assert type == MutateType.APPEND : type.name();
    byte[] row = proto.getRow().toByteArray();
    Append append = new Append(row);
    append.setWriteToWAL(proto.getWriteToWAL());
    for (NameBytesPair attribute: proto.getAttributeList()) {
      append.setAttribute(attribute.getName(),
        attribute.getValue().toByteArray());
    }
    for (ColumnValue column: proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv: column.getQualifierValueList()) {
        byte[] qualifier = qv.getQualifier().toByteArray();
        if (!qv.hasValue()) {
          throw new DoNotRetryIOException(
            "Missing required field: qualifer value");
        }
        byte[] value = qv.getValue().toByteArray();
        append.add(family, qualifier, value);
      }
    }
    return append;
  }

  /**
   * Convert a protocol buffer Mutate to an Increment
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Increment
   * @throws IOException
   */
  public static Increment toIncrement(
      final Mutate proto) throws IOException {
    MutateType type = proto.getMutateType();
    assert type == MutateType.INCREMENT : type.name();
    RowLock lock = null;
    if (proto.hasLockId()) {
      lock = new RowLock(proto.getLockId());
    }
    byte[] row = proto.getRow().toByteArray();
    Increment increment = new Increment(row, lock);
    increment.setWriteToWAL(proto.getWriteToWAL());
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
    for (ColumnValue column: proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv: column.getQualifierValueList()) {
        byte[] qualifier = qv.getQualifier().toByteArray();
        if (!qv.hasValue()) {
          throw new DoNotRetryIOException(
            "Missing required field: qualifer value");
        }
        long value = Bytes.toLong(qv.getValue().toByteArray());
        increment.addColumn(family, qualifier, value);
      }
    }
    return increment;
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
      NameBytesPair filter = proto.getFilter();
      scan.setFilter((Filter)toObject(filter));
    }
    if (proto.hasBatchSize()) {
      scan.setBatch(proto.getBatchSize());
    }
    for (NameBytesPair attribute: proto.getAttributeList()) {
      scan.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      for (Column column: proto.getColumnList()) {
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
    return scan;
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final Result result) {
    ClientProtos.Result.Builder builder = ClientProtos.Result.newBuilder();
    List<ByteString> protos = new ArrayList<ByteString>();
    List<KeyValue> keyValues = result.list();
    if (keyValues != null) {
      for (KeyValue keyValue: keyValues) {
        ByteString value = ByteString.copyFrom(keyValue.getBuffer(),
          keyValue.getOffset(), keyValue.getLength());
        protos.add(value);
      }
    }
    builder.addAllKeyValueBytes(protos);
    return builder.build();
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @return the converted client Result
   */
  public static Result toResult(final ClientProtos.Result proto) {
    List<ByteString> values = proto.getKeyValueBytesList();
    List<KeyValue> keyValues = new ArrayList<KeyValue>(values.size());
    for (ByteString value: values) {
      keyValues.add(new KeyValue(value.toByteArray()));
    }
    return new Result(keyValues);
  }

  /**
   * Get the HLog entries from a list of protocol buffer WALEntry
   *
   * @param protoList the list of protocol buffer WALEntry
   * @return an array of HLog entries
   */
  public static HLog.Entry[]
      toHLogEntries(final List<WALEntry> protoList) {
    List<HLog.Entry> entries = new ArrayList<HLog.Entry>();
    for (WALEntry entry: protoList) {
      WALKey walKey = entry.getKey();
      java.util.UUID clusterId = HConstants.DEFAULT_CLUSTER_ID;
      if (walKey.hasClusterId()) {
        UUID protoUuid = walKey.getClusterId();
        clusterId = new java.util.UUID(
          protoUuid.getMostSigBits(), protoUuid.getLeastSigBits());
      }
      HLogKey key = new HLogKey(walKey.getEncodedRegionName().toByteArray(),
        walKey.getTableName().toByteArray(), walKey.getLogSequenceNumber(),
        walKey.getWriteTime(), clusterId);
      WALEntry.WALEdit walEdit = entry.getEdit();
      WALEdit edit = new WALEdit();
      for (ByteString keyValue: walEdit.getKeyValueBytesList()) {
        edit.add(new KeyValue(keyValue.toByteArray()));
      }
      if (walEdit.getFamilyScopeCount() > 0) {
        TreeMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>();
        for (FamilyScope scope: walEdit.getFamilyScopeList()) {
          scopes.put(scope.getFamily().toByteArray(),
            Integer.valueOf(scope.getScopeType().ordinal()));
        }
        edit.setScopes(scopes);
      }
      entries.add(new HLog.Entry(key, edit));
    }
    return entries.toArray(new HLog.Entry[entries.size()]);
  }

  /**
   * Convert a protocol buffer Parameter to a Java object
   *
   * @param parameter the protocol buffer Parameter to convert
   * @return the converted Java object
   * @throws IOException if failed to deserialize the parameter
   */
  public static Object toObject(
      final NameBytesPair parameter) throws IOException {
    if (parameter == null || !parameter.hasValue()) return null;
    byte[] bytes = parameter.getValue().toByteArray();
    ByteArrayInputStream bais = null;
    try {
      bais = new ByteArrayInputStream(bytes);
      DataInput in = new DataInputStream(bais);
      return HbaseObjectWritable.readObject(in, null);
    } finally {
      if (bais != null) {
        bais.close();
      }
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
  public static Throwable toException(
      final NameBytesPair parameter) throws IOException {
    if (parameter == null || !parameter.hasValue()) return null;
    String desc = parameter.getValue().toStringUtf8();
    String type = parameter.getName();
    try {
      Class<? extends Throwable> c =
        (Class<? extends Throwable>)Class.forName(type);
      Constructor<? extends Throwable> cn =
        c.getDeclaredConstructor(String.class);
      return cn.newInstance(desc);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Serialize a Java Object into a Parameter. The Java Object should be a
   * Writable or protocol buffer Message
   *
   * @param value the Writable/Message object to be serialized
   * @return the converted protocol buffer Parameter
   * @throws IOException if failed to serialize the object
   */
  public static NameBytesPair toParameter(
      final Object value) throws IOException {
    Class<?> declaredClass = Object.class;
    if (value != null) {
      declaredClass = value.getClass();
    }
    return toParameter(declaredClass, value);
  }

  /**
   * Serialize a Java Object into a Parameter. The Java Object should be a
   * Writable or protocol buffer Message
   *
   * @param declaredClass the declared class of the parameter
   * @param value the Writable/Message object to be serialized
   * @return the converted protocol buffer Parameter
   * @throws IOException if failed to serialize the object
   */
  public static NameBytesPair toParameter(
      final Class<?> declaredClass, final Object value) throws IOException {
    NameBytesPair.Builder builder = NameBytesPair.newBuilder();
    builder.setName(declaredClass.getName());
    if (value != null) {
      ByteArrayOutputStream baos = null;
      try {
        baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        Class<?> clz = declaredClass;
        if (HbaseObjectWritable.getClassCode(declaredClass) == null) {
          clz = value.getClass();
        }
        HbaseObjectWritable.writeObject(out, value, clz, null);
        builder.setValue(
          ByteString.copyFrom(baos.toByteArray()));
      } finally {
        if (baos != null) {
          baos.close();
        }
      }
    }
    return builder.build();
  }

// Start helpers for Client

  /**
   * A helper to invoke a Get using client protocol.
   *
   * @param client
   * @param regionName
   * @param get
   * @return the result of the Get
   * @throws IOException
   */
  public static Result get(final ClientProtocol client,
      final byte[] regionName, final Get get) throws IOException {
    GetRequest request =
      RequestConverter.buildGetRequest(regionName, get);
    try {
      GetResponse response = client.get(null, request);
      return toResult(response.getResult());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to get a row of the closet one before using client protocol.
   *
   * @param client
   * @param regionName
   * @param row
   * @param family
   * @return the row or the closestRowBefore if it doesn't exist
   * @throws IOException
   */
  public static Result getRowOrBefore(final ClientProtocol client,
      final byte[] regionName, final byte[] row,
      final byte[] family) throws IOException {
    GetRequest request =
      RequestConverter.buildGetRowOrBeforeRequest(
        regionName, row, family);
    try {
      GetResponse response = client.get(null, request);
      if (!response.hasResult()) return null;
      return toResult(response.getResult());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to invoke a multi action using client protocol.
   *
   * @param client
   * @param multi
   * @return a multi response
   * @throws IOException
   */
  public static <R> MultiResponse multi(final ClientProtocol client,
      final MultiAction<R> multi) throws IOException {
    try {
      MultiResponse response = new MultiResponse();
      for (Map.Entry<byte[], List<Action<R>>> e: multi.actions.entrySet()) {
        byte[] regionName = e.getKey();
        int rowMutations = 0;
        List<Action<R>> actions = e.getValue();
        for (Action<R> action: actions) {
          Row row = action.getAction();
          if (row instanceof RowMutations) {
            MultiRequest request =
              RequestConverter.buildMultiRequest(regionName, (RowMutations)row);
            client.multi(null, request);
            response.add(regionName, action.getOriginalIndex(), new Result());
            rowMutations++;
          }
        }
        if (actions.size() > rowMutations) {
          MultiRequest request =
            RequestConverter.buildMultiRequest(regionName, actions);
          ClientProtos.MultiResponse
            proto = client.multi(null, request);
          List<Object> results = ResponseConverter.getResults(proto);
          for (int i = 0, n = results.size(); i < n; i++) {
            int originalIndex = actions.get(i).getOriginalIndex();
            response.add(regionName, originalIndex, results.get(i));
          }
        }
      }
      return response;
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to bulk load a list of HFiles using client protocol.
   *
   * @param client
   * @param familyPaths
   * @param regionName
   * @return true if all are loaded
   * @throws IOException
   */
  public static boolean bulkLoadHFile(final ClientProtocol client,
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName) throws IOException {
    BulkLoadHFileRequest request =
      RequestConverter.buildBulkLoadHFileRequest(familyPaths, regionName);
    try {
      BulkLoadHFileResponse response =
        client.bulkLoadHFile(null, request);
      return response.getLoaded();
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to exec a coprocessor Exec using client protocol.
   *
   * @param client
   * @param exec
   * @param regionName
   * @return the exec result
   * @throws IOException
   */
  public static ExecResult execCoprocessor(final ClientProtocol client,
      final Exec exec, final byte[] regionName) throws IOException {
    ExecCoprocessorRequest request =
      RequestConverter.buildExecCoprocessorRequest(regionName, exec);
    try {
      ExecCoprocessorResponse response =
        client.execCoprocessor(null, request);
      Object value = ProtobufUtil.toObject(response.getValue());
      return new ExecResult(regionName, value);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

// End helpers for Client
// Start helpers for Admin

  /**
   * A helper to retrieve region info given a region name
   * using admin protocol.
   *
   * @param admin
   * @param regionName
   * @return the retrieved region info
   * @throws IOException
   */
  public static HRegionInfo getRegionInfo(final AdminProtocol admin,
      final byte[] regionName) throws IOException {
    try {
      GetRegionInfoRequest request =
        RequestConverter.buildGetRegionInfoRequest(regionName);
      GetRegionInfoResponse response =
        admin.getRegionInfo(null, request);
      return toRegionInfo(response.getRegionInfo());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to close a region given a region name
   * using admin protocol.
   *
   * @param admin
   * @param regionName
   * @param transitionInZK
   * @throws IOException
   */
  public static void closeRegion(final AdminProtocol admin,
      final byte[] regionName, final boolean transitionInZK) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(regionName, transitionInZK);
    try {
      admin.closeRegion(null, closeRegionRequest);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to close a region given a region name
   * using admin protocol.
   *
   * @param admin
   * @param regionName
   * @param versionOfClosingNode
   * @return true if the region is closed
   * @throws IOException
   */
  public static boolean closeRegion(final AdminProtocol admin,
      final byte[] regionName, final int versionOfClosingNode) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(regionName, versionOfClosingNode);
    try {
      CloseRegionResponse response = admin.closeRegion(null, closeRegionRequest);
      return ResponseConverter.isClosed(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to open a region using admin protocol.
   *
   * @param admin
   * @param region
   * @param versionOfOfflineNode
   * @return the region opening state
   * @throws IOException
   */
  public static RegionOpeningState openRegion(final AdminProtocol admin,
      final HRegionInfo region, final int versionOfOfflineNode) throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(region, versionOfOfflineNode);
    try {
      OpenRegionResponse response = admin.openRegion(null, request);
      return ResponseConverter.getRegionOpeningState(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to open a list of regions using admin protocol.
   *
   * @param admin
   * @param regions
   * @throws IOException
   */
  public static void openRegion(final AdminProtocol admin,
      final List<HRegionInfo> regions) throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(regions);
    try {
      admin.openRegion(null, request);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to get the all the online regions on a region
   * server using admin protocol.
   *
   * @param admin
   * @return a list of online region info
   * @throws IOException
   */
  public static List<HRegionInfo> getOnlineRegions(
      final AdminProtocol admin) throws IOException {
    GetOnlineRegionRequest request = RequestConverter.buildGetOnlineRegionRequest();
    List<HRegionInfo> regions = null;
    try {
      GetOnlineRegionResponse response =
        admin.getOnlineRegion(null, request);
      regions = new ArrayList<HRegionInfo>();
      for (RegionInfo regionInfo: response.getRegionInfoList()) {
        regions.add(toRegionInfo(regionInfo));
      }
      return regions;
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to get the info of a region server using admin protocol.
   *
   * @param admin
   * @return the server name
   * @throws IOException
   */
  public static ServerName getServerInfo(
      final AdminProtocol admin) throws IOException {
    GetServerInfoRequest request = RequestConverter.buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(null, request);
      return toServerName(response.getServerName());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to replicate a list of HLog entries using admin protocol.
   *
   * @param admin
   * @param entries
   * @throws IOException
   */
  public static void replicateWALEntry(final AdminProtocol admin,
      final HLog.Entry[] entries) throws IOException {
    ReplicateWALEntryRequest request =
      RequestConverter.buildReplicateWALEntryRequest(entries);
    try {
      admin.replicateWALEntry(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * A helper to get the list of files of a column family
   * on a given region using admin protocol.
   *
   * @param admin
   * @param regionName
   * @param family
   * @return the list of store files
   * @throws IOException
   */
  public static List<String> getStoreFiles(final AdminProtocol admin,
      final byte[] regionName, final byte[] family) throws IOException {
    GetStoreFileRequest request =
      RequestConverter.buildGetStoreFileRequest(regionName, family);
    try {
      GetStoreFileResponse response = admin.getStoreFile(null, request);
      return response.getStoreFileList();
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

// End helpers for Admin
}