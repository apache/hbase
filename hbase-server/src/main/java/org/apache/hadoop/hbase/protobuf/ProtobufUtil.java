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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UUID;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.FamilyScope;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
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
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

import static org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType.*;

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
  private static final String PB_MAGIC_STR = Bytes.toString(PB_MAGIC);

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
    if (bytes == null || bytes.length < PB_MAGIC.length) return false;
    return Bytes.compareTo(PB_MAGIC, 0, PB_MAGIC.length, bytes, 0, PB_MAGIC.length) == 0;
  }

  /**
   * @param bytes
   * @throws DeserializationException if we are missing the pb magic prefix
   */
  public static void expectPBMagicPrefix(final byte [] bytes) throws DeserializationException {
    if (!isPBMagicPrefix(bytes)) {
      throw new DeserializationException("Missing pb magic " + PB_MAGIC_STR + " prefix");
    }
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
   * @see #toServerName(org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName)
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
    return new ServerName(hostName, port, startCode);
  }

  /**
   * Get HTableDescriptor[] from GetTableDescriptorsResponse protobuf
   *
   * @param proto the GetTableDescriptorsResponse
   * @return HTableDescriptor[]
   */
  public static HTableDescriptor[] getHTableDescriptorArray(GetTableDescriptorsResponse proto) {
    if (proto == null) return null;

    HTableDescriptor[] ret = new HTableDescriptor[proto.getTableSchemaCount()];
    for (int i = 0; i < proto.getTableSchemaCount(); ++i) {
      ret[i] = HTableDescriptor.convert(proto.getTableSchema(i));
    }
    return ret;
  }

  /**
   * get the split keys in form "byte [][]" from a CreateTableRequest proto
   *
   * @param proto the CreateTableRequest
   * @return the split keys
   */
  public static byte [][] getSplitKeysArray(final CreateTableRequest proto) {
    byte [][] splitKeys = new byte[proto.getSplitKeysCount()][];
    for (int i = 0; i < proto.getSplitKeysCount(); ++i) {
      splitKeys[i] = proto.getSplitKeys(i).toByteArray();
    }
    return splitKeys;
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
      HBaseProtos.Filter filter = proto.getFilter();
      get.setFilter(ProtobufUtil.toFilter(filter));
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
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(ByteString.copyFrom(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(ByteString.copyFrom(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(ProtobufUtil.toFilter(scan.getFilter()));
    }
    Column.Builder columnBuilder = Column.newBuilder();
    for (Map.Entry<byte[],NavigableSet<byte []>>
        family: scan.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      NavigableSet<byte []> columns = family.getValue();
      columnBuilder.clearQualifier();
      if (columns != null && columns.size() > 0) {
        for (byte [] qualifier: family.getValue()) {
          if (qualifier != null) {
            columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
          }
        }
      }
      scanBuilder.addColumn(columnBuilder.build());
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      scanBuilder.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      scanBuilder.setStoreOffset(scan.getRowOffsetPerColumnFamily());
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
      HBaseProtos.Filter filter = proto.getFilter();
      scan.setFilter(ProtobufUtil.toFilter(filter));
    }
    if (proto.hasBatchSize()) {
      scan.setBatch(proto.getBatchSize());
    }
    if (proto.hasMaxResultSize()) {
      scan.setMaxResultSize(proto.getMaxResultSize());
    }
    for (NameBytesPair attribute: proto.getAttributeList()) {
      scan.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    if (proto.getColumnCount() > 0) {
      TreeMap<byte [], NavigableSet<byte[]>> familyMap =
        new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
      for (Column column: proto.getColumnList()) {
        byte[] family = column.getFamily().toByteArray();
        TreeSet<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        for (ByteString qualifier: column.getQualifierList()) {
          set.add(qualifier.toByteArray());
        }
        familyMap.put(family, set);
      }
      scan.setFamilyMap(familyMap);
    }
    return scan;
  }


  /**
   * Create a new protocol buffer Exec based on a client Exec
   *
   * @param exec
   * @return
   * @throws IOException
   */
  public static ClientProtos.Exec toExec(
      final Exec exec) throws IOException {
    ClientProtos.Exec.Builder
      builder = ClientProtos.Exec.newBuilder();
    Configuration conf = exec.getConf();
    if (conf != null) {
      NameStringPair.Builder propertyBuilder = NameStringPair.newBuilder();
      Iterator<Entry<String, String>> iterator = conf.iterator();
      while (iterator.hasNext()) {
        Entry<String, String> entry = iterator.next();
        propertyBuilder.setName(entry.getKey());
        propertyBuilder.setValue(entry.getValue());
        builder.addProperty(propertyBuilder.build());
      }
    }
    builder.setProtocolName(exec.getProtocolName());
    builder.setMethodName(exec.getMethodName());
    builder.setRow(ByteString.copyFrom(exec.getRow()));
    Object[] parameters = exec.getParameters();
    if (parameters != null && parameters.length > 0) {
      Class<?>[] declaredClasses = exec.getParameterClasses();
      for (int i = 0, n = parameters.length; i < n; i++) {
        builder.addParameter(
          ProtobufUtil.toParameter(declaredClasses[i], parameters[i]));
      }
    }
    return builder.build();
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
    builder.setRow(ByteString.copyFrom(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getLockId() >= 0) {
      builder.setLockId(get.getLockId());
    }
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
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family: families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier: qualifiers) {
            if (qualifier != null) {
              columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
            }
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
    return builder.build();
  }

  /**
   * Convert a client Increment to a protobuf Mutate.
   *
   * @param increment
   * @return the converted mutate
   */
  public static Mutate toMutate(final Increment increment) {
    Mutate.Builder builder = Mutate.newBuilder();
    builder.setRow(ByteString.copyFrom(increment.getRow()));
    builder.setMutateType(MutateType.INCREMENT);
    builder.setWriteToWAL(increment.getWriteToWAL());
    if (increment.getLockId() >= 0) {
      builder.setLockId(increment.getLockId());
    }
    TimeRange timeRange = increment.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[],NavigableMap<byte[], Long>>
        family: increment.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      NavigableMap<byte[], Long> values = family.getValue();
      if (values != null && values.size() > 0) {
        for (Map.Entry<byte[], Long> value: values.entrySet()) {
          valueBuilder.setQualifier(ByteString.copyFrom(value.getKey()));
          valueBuilder.setValue(ByteString.copyFrom(
            Bytes.toBytes(value.getValue().longValue())));
          columnBuilder.addQualifierValue(valueBuilder.build());
        }
      }
      builder.addColumnValue(columnBuilder.build());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param mutateType
   * @param mutation
   * @return a mutate
   * @throws IOException
   */
  public static Mutate toMutate(final MutateType mutateType,
      final Mutation mutation) throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFrom(mutation.getRow()));
    mutateBuilder.setMutateType(mutateType);
    mutateBuilder.setWriteToWAL(mutation.getWriteToWAL());
    if (mutation.getLockId() >= 0) {
      mutateBuilder.setLockId(mutation.getLockId());
    }
    mutateBuilder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        mutateBuilder.addAttribute(attributeBuilder.build());
      }
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[],List<KeyValue>>
        family: mutation.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      for (KeyValue value: family.getValue()) {
        valueBuilder.setQualifier(ByteString.copyFrom(value.getQualifier()));
        valueBuilder.setValue(ByteString.copyFrom(value.getValue()));
        valueBuilder.setTimestamp(value.getTimestamp());
        if (mutateType == MutateType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(value.getType());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      mutateBuilder.addColumnValue(columnBuilder.build());
    }
    return mutateBuilder.build();
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
   * Convert a ByteArrayComparable to a protocol buffer Comparator
   *
   * @param comparator the ByteArrayComparable to convert
   * @return the converted protocol buffer Comparator
   */
  public static ComparatorProtos.Comparator toComparator(ByteArrayComparable comparator) {
    ComparatorProtos.Comparator.Builder builder = ComparatorProtos.Comparator.newBuilder();
    builder.setName(comparator.getClass().getName());
    builder.setSerializedComparator(ByteString.copyFrom(comparator.toByteArray()));
    return builder.build();
  }

  /**
   * Convert a protocol buffer Comparator to a ByteArrayComparable
   *
   * @param proto the protocol buffer Comparator to convert
   * @return the converted ByteArrayComparable
   */
  public static ByteArrayComparable toComparator(ComparatorProtos.Comparator proto)
  throws IOException {
    String type = proto.getName();
    String funcName = "parseFrom";
    byte [] value = proto.getSerializedComparator().toByteArray();
    try {
      Class<? extends ByteArrayComparable> c =
        (Class<? extends ByteArrayComparable>)(Class.forName(type));
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
  public static Filter toFilter(HBaseProtos.Filter proto) throws IOException {
    String type = proto.getName();
    final byte [] value = proto.getSerializedFilter().toByteArray();
    String funcName = "parseFrom";
    try {
      Class<? extends Filter> c =
        (Class<? extends Filter>)Class.forName(type);
      Method parseFrom = c.getMethod(funcName, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + funcName + " in type: " + type);
      }
      return (Filter)parseFrom.invoke(c, value);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static HBaseProtos.Filter toFilter(Filter filter) {
    HBaseProtos.Filter.Builder builder = HBaseProtos.Filter.newBuilder();
    builder.setName(filter.getClass().getName());
    builder.setSerializedFilter(ByteString.copyFrom(filter.toByteArray()));
    return builder.build();
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
        TreeMap<byte[], Integer> scopes =
          new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
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
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type
   * @return
   * @throws IOException
   */
  public static DeleteType toDeleteType(
      KeyValue.Type type) throws IOException {
    switch (type) {
    case Delete:
      return DeleteType.DELETE_ONE_VERSION;
    case DeleteColumn:
      return DeleteType.DELETE_MULTIPLE_VERSIONS;
    case DeleteFamily:
      return DeleteType.DELETE_FAMILY;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
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
      if (response == null) return null;
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
   * @param assignSeqNum
   * @return true if all are loaded
   * @throws IOException
   */
  public static boolean bulkLoadHFile(final ClientProtocol client,
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum) throws IOException {
    BulkLoadHFileRequest request =
      RequestConverter.buildBulkLoadHFileRequest(familyPaths, regionName, assignSeqNum);
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

  public static CoprocessorServiceResponse execService(final ClientProtocol client,
      final CoprocessorServiceCall call, final byte[] regionName) throws IOException {
    CoprocessorServiceRequest request = CoprocessorServiceRequest.newBuilder()
        .setCall(call).setRegion(
            RequestConverter.buildRegionSpecifier(REGION_NAME, regionName)).build();
    try {
      CoprocessorServiceResponse response =
          client.execService(null, request);
      return response;
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  public static <T extends Service> T newServiceStub(Class<T> service, RpcChannel channel)
      throws Exception {
    return (T)Methods.call(service, null, "newStub",
        new Class[]{ RpcChannel.class }, new Object[]{ channel });
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
      return HRegionInfo.convert(response.getRegionInfo());
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
   * A helper to close a region given a region name
   * using admin protocol.
   *
   * @param admin
   * @param regionName
   * @param versionOfClosingNode
   * @return true if the region is closed
   * @throws IOException
   */
  public static boolean closeRegion(final AdminProtocol admin, final byte[] regionName,
                                    final int versionOfClosingNode, final ServerName destinationServer) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(regionName, versionOfClosingNode, destinationServer);
    try {
      CloseRegionResponse response = admin.closeRegion(null, closeRegionRequest);
      return ResponseConverter.isClosed(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }


  /**
   * A helper to open a region using admin protocol.
   * @param admin
   * @param region
   * @throws IOException
   */
  public static void openRegion(final AdminProtocol admin,
      final HRegionInfo region) throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(region, -1);
    try {
      admin.openRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * A helper to open a list of regions using admin protocol.
   * 
   * @param admin
   * @param regions
   * @return OpenRegionResponse
   * @throws IOException
   */
  public static OpenRegionResponse openRegion(final AdminProtocol admin,
      final List<HRegionInfo> regions) throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(regions);
    try {
      return admin.openRegion(null, request);
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
  public static List<HRegionInfo> getOnlineRegions(final AdminProtocol admin) throws IOException {
    GetOnlineRegionRequest request = RequestConverter.buildGetOnlineRegionRequest();
    GetOnlineRegionResponse response = null;
    try {
      response = admin.getOnlineRegion(null, request);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
    return getRegionInfos(response);
  }

  /**
   * Get the list of region info from a GetOnlineRegionResponse
   *
   * @param proto the GetOnlineRegionResponse
   * @return the list of region info or null if <code>proto</code> is null
   */
  static List<HRegionInfo> getRegionInfos(final GetOnlineRegionResponse proto) {
    if (proto == null) return null;
    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    for (RegionInfo regionInfo: proto.getRegionInfoList()) {
      regionInfos.add(HRegionInfo.convert(regionInfo));
    }
    return regionInfos;
  }

  /**
   * A helper to get the info of a region server using admin protocol.
   *
   * @param admin
   * @return the server name
   * @throws IOException
   */
  public static ServerInfo getServerInfo(
      final AdminProtocol admin) throws IOException {
    GetServerInfoRequest request = RequestConverter.buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(null, request);
      return response.getServerInfo();
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

  /**
   * A helper to split a region using admin protocol.
   *
   * @param admin
   * @param hri
   * @param splitPoint
   * @throws IOException
   */
  public static void split(final AdminProtocol admin,
      final HRegionInfo hri, byte[] splitPoint) throws IOException {
    SplitRegionRequest request =
      RequestConverter.buildSplitRegionRequest(hri.getRegionName(), splitPoint);
    try {
      admin.splitRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

// End helpers for Admin

  /*
   * Get the total (read + write) requests from a RegionLoad pb
   * @param rl - RegionLoad pb
   * @return total (read + write) requests
   */
  public static long getTotalRequestsCount(RegionLoad rl) {
    if (rl == null) {
      return 0;
    }

    return rl.getReadRequestsCount() + rl.getWriteRequestsCount();
  }


  /**
   * @param m Message to get delimited pb serialization of (with pb magic prefix)
   */
  public static byte [] toDelimitedByteArray(final Message m) throws IOException {
    // Allocate arbitrary big size so we avoid resizing.
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    m.writeDelimitedTo(baos);
    baos.close();
    return ProtobufUtil.prependPBMagic(baos.toByteArray());
  }

  /**
   * Converts a Permission proto to a client Permission object.
   *
   * @param proto the protobuf Permission
   * @return the converted Permission
   */
  public static Permission toPermission(AccessControlProtos.Permission proto) {
    if (proto.hasTable()) {
      return toTablePermission(proto);
    } else {
      List<Permission.Action> actions = toPermissionActions(proto.getActionList());
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
    List<Permission.Action> actions = toPermissionActions(proto.getActionList());

    byte[] qualifier = null;
    byte[] family = null;
    byte[] table = null;

    if (proto.hasTable()) table = proto.getTable().toByteArray();
    if (proto.hasFamily()) family = proto.getFamily().toByteArray();
    if (proto.hasQualifier()) qualifier = proto.getQualifier().toByteArray();

    return new TablePermission(table, family, qualifier,
        actions.toArray(new Permission.Action[actions.size()]));
  }

  /**
   * Convert a client Permission to a Permission proto
   *
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    AccessControlProtos.Permission.Builder builder = AccessControlProtos.Permission.newBuilder();
    if (perm instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission)perm;
      if (tablePerm.hasTable()) {
        builder.setTable(ByteString.copyFrom(tablePerm.getTable()));
      }
      if (tablePerm.hasFamily()) {
        builder.setFamily(ByteString.copyFrom(tablePerm.getFamily()));
      }
      if (tablePerm.hasQualifier()) {
        builder.setQualifier(ByteString.copyFrom(tablePerm.getQualifier()));
      }
    }
    for (Permission.Action a : perm.getActions()) {
      builder.addAction(toPermissionAction(a));
    }
    return builder.build();
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
    AccessControlProtos.Permission.Builder permissionBuilder =
        AccessControlProtos.Permission.newBuilder();
    for (Permission.Action a : perm.getActions()) {
      permissionBuilder.addAction(toPermissionAction(a));
    }
    if (perm.hasTable()) {
      permissionBuilder.setTable(ByteString.copyFrom(perm.getTable()));
    }
    if (perm.hasFamily()) {
      permissionBuilder.setFamily(ByteString.copyFrom(perm.getFamily()));
    }
    if (perm.hasQualifier()) {
      permissionBuilder.setQualifier(ByteString.copyFrom(perm.getQualifier()));
    }

    return AccessControlProtos.UserPermission.newBuilder()
        .setUser(ByteString.copyFrom(perm.getUser()))
        .setPermission(permissionBuilder)
        .build();
  }

  /**
   * Converts a user permission proto to a client user permission object.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(AccessControlProtos.UserPermission proto) {
    AccessControlProtos.Permission permission = proto.getPermission();
    List<Permission.Action> actions = toPermissionActions(permission.getActionList());

    byte[] qualifier = null;
    byte[] family = null;
    byte[] table = null;

    if (permission.hasTable()) table = permission.getTable().toByteArray();
    if (permission.hasFamily()) family = permission.getFamily().toByteArray();
    if (permission.hasQualifier()) qualifier = permission.getQualifier().toByteArray();

    return new UserPermission(proto.getUser().toByteArray(),
        table, family, qualifier,
        actions.toArray(new Permission.Action[actions.size()]));
  }

  /**
   * Convert a ListMultimap<String, TablePermission> where key is username
   * to a protobuf UserPermission
   *
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static AccessControlProtos.UserTablePermissions toUserTablePermissions(
      ListMultimap<String, TablePermission> perm) {
    AccessControlProtos.UserTablePermissions.Builder builder =
                  AccessControlProtos.UserTablePermissions.newBuilder();
    for (Map.Entry<String, Collection<TablePermission>> entry : perm.asMap().entrySet()) {
      AccessControlProtos.UserTablePermissions.UserPermissions.Builder userPermBuilder =
                  AccessControlProtos.UserTablePermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (TablePermission tablePerm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(tablePerm));
      }
      builder.addPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  /**
   * Convert a protobuf UserTablePermissions to a
   * ListMultimap<String, TablePermission> where key is username.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static ListMultimap<String, TablePermission> toUserTablePermissions(
      AccessControlProtos.UserTablePermissions proto) {
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    AccessControlProtos.UserTablePermissions.UserPermissions userPerm;

    for (int i = 0; i < proto.getPermissionsCount(); i++) {
      userPerm = proto.getPermissions(i);
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        TablePermission tablePerm = toTablePermission(userPerm.getPermissions(j));
        perms.put(userPerm.getUser().toStringUtf8(), tablePerm);
      }
    }

    return perms;
  }

  /**
   * Unwraps an exception from a protobuf service into the underlying (expected) IOException.
   * This method will <strong>always</strong> throw an exception.
   * @param se the {@code ServiceException} instance to convert into an {@code IOException}
   */
  public static void toIOException(ServiceException se) throws IOException {
    if (se == null) {
      throw new NullPointerException("Null service exception passed!");
    }

    Throwable cause = se.getCause();
    if (cause != null && cause instanceof IOException) {
      throw (IOException)cause;
    }
    throw new IOException(se);
  }
}
