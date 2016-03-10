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


import static org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.quotas.QuotaScope;
import org.apache.hadoop.hbase.quotas.QuotaType;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * Protobufs utility.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
  justification="None. Address sometime.")
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class
public final class ProtobufUtil {

  private ProtobufUtil() {
  }

  /**
   * Primitive type to class mapping.
   */
  private final static Map<String, Class<?>>
    PRIMITIVES = new HashMap<String, Class<?>>();


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
  public static final byte [] PB_MAGIC = new byte [] {'P', 'B', 'U', 'F'};
  private static final String PB_MAGIC_STR = Bytes.toString(PB_MAGIC);

  /**
   * Prepend the passed bytes with four bytes of magic, {@link #PB_MAGIC}, to flag what
   * follows as a protobuf in hbase.  Prepend these bytes to all content written to znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</code> with magic prepended (Creates a new
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
    if (bytes == null) return false;
    return isPBMagicPrefix(bytes, 0, bytes.length);
  }

  /**
   * @param bytes Bytes to check.
   * @param offset offset to start at
   * @param len length to use
   * @return True if passed <code>bytes</code> has {@link #PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes, int offset, int len) {
    if (bytes == null || len < PB_MAGIC.length) return false;
    return Bytes.compareTo(PB_MAGIC, 0, PB_MAGIC.length, bytes, offset, PB_MAGIC.length) == 0;
  }

  /**
   * @param bytes bytes to check
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
    if (ExceptionUtil.isInterrupt(e)) {
      return ExceptionUtil.asInterrupt(e);
    }
    if (e instanceof RemoteException) {
      e = ((RemoteException) e).unwrapRemoteException();
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
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
    return ServerName.valueOf(hostName, port, startCode);
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
   * Convert a client Durability into a protbuf Durability
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
    if (proto.getCfTimeRangeCount() > 0) {
      for (HBaseProtos.ColumnFamilyTimeRange cftr : proto.getCfTimeRangeList()) {
        TimeRange timeRange = protoToTimeRange(cftr.getTimeRange());
        get.setColumnFamilyTimeRange(cftr.getColumnFamily().toByteArray(),
            timeRange.getMin(), timeRange.getMax());
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      get.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }
    if (proto.hasFilter()) {
      FilterProtos.Filter filter = proto.getFilter();
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
   * @return A client Put.
   * @throws IOException
   */
  public static Put toPut(final MutationProto proto)
  throws IOException {
    return toPut(proto, null);
  }

  /**
   * Convert a protocol buffer Mutate to a Put.
   *
   * @param proto The protocol buffer MutationProto to convert
   * @param cellScanner If non-null, the Cell data that goes with this proto.
   * @return A client Put.
   * @throws IOException
   */
  public static Put toPut(final MutationProto proto, final CellScanner cellScanner)
  throws IOException {
    // TODO: Server-side at least why do we convert back to the Client types?  Why not just pb it?
    MutationType type = proto.getMutateType();
    assert type == MutationType.PUT: type.name();
    long timestamp = proto.hasTimestamp()? proto.getTimestamp(): HConstants.LATEST_TIMESTAMP;
    Put put = proto.hasRow() ? new Put(proto.getRow().toByteArray(), timestamp) : null;
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
      if (put == null) {
        throw new IllegalArgumentException("row cannot be null");
      }
      // The proto has the metadata and the data itself
      for (ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv: column.getQualifierValueList()) {
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
    for (NameBytesPair attribute: proto.getAttributeList()) {
      put.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return put;
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted client Delete
   * @throws IOException
   */
  public static Delete toDelete(final MutationProto proto)
  throws IOException {
    return toDelete(proto, null);
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   *
   * @param proto the protocol buffer Mutate to convert
   * @param cellScanner if non-null, the data that goes with this delete.
   * @return the converted client Delete
   * @throws IOException
   */
  public static Delete toDelete(final MutationProto proto, final CellScanner cellScanner)
  throws IOException {
    MutationType type = proto.getMutateType();
    assert type == MutationType.DELETE : type.name();
    long timestamp = proto.hasTimestamp() ? proto.getTimestamp() : HConstants.LATEST_TIMESTAMP;
    Delete delete = proto.hasRow() ? new Delete(proto.getRow().toByteArray(), timestamp) : null;
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
      if (delete == null) {
        throw new IllegalArgumentException("row cannot be null");
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
          } else if (deleteType == DeleteType.DELETE_FAMILY_VERSION) {
            delete.deleteFamilyVersion(family, ts);
          } else {
            delete.deleteFamily(family, ts);
          }
        }
      }
    }
    delete.setDurability(toDurability(proto.getDurability()));
    for (NameBytesPair attribute: proto.getAttributeList()) {
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
  public static Append toAppend(final MutationProto proto, final CellScanner cellScanner)
  throws IOException {
    MutationType type = proto.getMutateType();
    assert type == MutationType.APPEND : type.name();
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
      for (ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv: column.getQualifierValueList()) {
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
    for (NameBytesPair attribute: proto.getAttributeList()) {
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
  public static Mutation toMutation(final MutationProto proto) throws IOException {
    MutationType type = proto.getMutateType();
    if (type == MutationType.APPEND) {
      return toAppend(proto, null);
    }
    if (type == MutationType.DELETE) {
      return toDelete(proto, null);
    }
    if (type == MutationType.PUT) {
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
  public static Increment toIncrement(final MutationProto proto, final CellScanner cellScanner)
  throws IOException {
    MutationType type = proto.getMutateType();
    assert type == MutationType.INCREMENT : type.name();
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
      for (ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv: column.getQualifierValueList()) {
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
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      increment.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }
    increment.setDurability(toDurability(proto.getDurability()));
    for (NameBytesPair attribute : proto.getAttributeList()) {
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
    if (scan.getAllowPartialResults()) {
      scanBuilder.setAllowPartialResults(scan.getAllowPartialResults());
    }
    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      scanBuilder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand.booleanValue());
    }
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    for (Entry<byte[], TimeRange> cftr : scan.getColumnFamilyTimeRange().entrySet()) {
      HBaseProtos.ColumnFamilyTimeRange.Builder b = HBaseProtos.ColumnFamilyTimeRange.newBuilder();
      b.setColumnFamily(ByteString.copyFrom(cftr.getKey()));
      b.setTimeRange(timeRangeToProto(cftr.getValue()));
      scanBuilder.addCfTimeRange(b);
    }
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
      Column.Builder columnBuilder = Column.newBuilder();
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
    if (proto.getCfTimeRangeCount() > 0) {
      for (HBaseProtos.ColumnFamilyTimeRange cftr : proto.getCfTimeRangeList()) {
        TimeRange timeRange = protoToTimeRange(cftr.getTimeRange());
        scan.setColumnFamilyTimeRange(cftr.getColumnFamily().toByteArray(),
            timeRange.getMin(), timeRange.getMax());
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      scan.setTimeRange(timeRange.getMin(), timeRange.getMax());
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
    if (proto.hasAllowPartialResults()) {
      scan.setAllowPartialResults(proto.getAllowPartialResults());
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
    for (Entry<byte[], TimeRange> cftr : get.getColumnFamilyTimeRange().entrySet()) {
      HBaseProtos.ColumnFamilyTimeRange.Builder b = HBaseProtos.ColumnFamilyTimeRange.newBuilder();
      b.setColumnFamily(ByteString.copyFrom(cftr.getKey()));
      b.setTimeRange(timeRangeToProto(cftr.getValue()));
      builder.addCfTimeRange(b);
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
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
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
  public static MutationProto toMutation(
    final Increment increment, final MutationProto.Builder builder, long nonce) {
    builder.setRow(ByteStringer.wrap(increment.getRow()));
    builder.setMutateType(MutationType.INCREMENT);
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
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
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
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteStringer.wrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    return builder.build();
  }

  public static MutationProto toMutation(final MutationType type, final Mutation mutation)
    throws IOException {
    return toMutation(type, mutation, HConstants.NO_NONCE);
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param type
   * @param mutation
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static MutationProto toMutation(final MutationType type, final Mutation mutation,
    final long nonce) throws IOException {
    return toMutation(type, mutation, MutationProto.newBuilder(), nonce);
  }

  public static MutationProto toMutation(final MutationType type, final Mutation mutation,
      MutationProto.Builder builder) throws IOException {
    return toMutation(type, mutation, builder, HConstants.NO_NONCE);
  }

  public static MutationProto toMutation(final MutationType type, final Mutation mutation,
      MutationProto.Builder builder, long nonce)
  throws IOException {
    builder = getMutationBuilderAndSetCommonFields(type, mutation, builder);
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
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
        if (type == MutationType.DELETE || (type == MutationType.PUT && CellUtil.isDelete(cell))) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(cell.getTypeByte());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      builder.addColumnValue(columnBuilder.build());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer MutationProto based on a client Mutation. Does NOT include data.
   * Understanding is that the Cell will be transported other than via protobuf.
   * @param type
   * @param mutation
   * @param builder
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static MutationProto toMutationNoData(final MutationType type, final Mutation mutation,
      final MutationProto.Builder builder)  throws IOException {
    return toMutationNoData(type, mutation, builder, HConstants.NO_NONCE);
  }

  /**
   * Create a protocol buffer MutationProto based on a client Mutation.  Does NOT include data.
   * Understanding is that the Cell will be transported other than via protobuf.
   * @param type
   * @param mutation
   * @return a protobuf'd Mutation
   * @throws IOException
   */
  public static MutationProto toMutationNoData(final MutationType type, final Mutation mutation)
  throws IOException {
    MutationProto.Builder builder =  MutationProto.newBuilder();
    return toMutationNoData(type, mutation, builder);
  }

  public static MutationProto toMutationNoData(final MutationType type, final Mutation mutation,
      final MutationProto.Builder builder, long nonce) throws IOException {
    getMutationBuilderAndSetCommonFields(type, mutation, builder);
    builder.setAssociatedCellCount(mutation.size());
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    return builder.build();
  }

  /**
   * Code shared by {@link #toMutation(MutationType, Mutation)} and
   * {@link #toMutationNoData(MutationType, Mutation)}
   * @param type
   * @param mutation
   * @return A partly-filled out protobuf'd Mutation.
   */
  private static MutationProto.Builder getMutationBuilderAndSetCommonFields(final MutationType type,
      final Mutation mutation, MutationProto.Builder builder) {
    builder.setRow(ByteStringer.wrap(mutation.getRow()));
    builder.setMutateType(type);
    builder.setDurability(toDurability(mutation.getDurability()));
    builder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
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
  public static DeleteType toDeleteType(
      KeyValue.Type type) throws IOException {
    switch (type) {
    case Delete:
      return DeleteType.DELETE_ONE_VERSION;
    case DeleteColumn:
      return DeleteType.DELETE_MULTIPLE_VERSIONS;
    case DeleteFamily:
      return DeleteType.DELETE_FAMILY;
    case DeleteFamilyVersion:
      return DeleteType.DELETE_FAMILY_VERSION;
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
      DeleteType type) throws IOException {
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
  public static Throwable toException(final NameBytesPair parameter) throws IOException {
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

// Start helpers for Client

  /**
   * A helper to get a row of the closet one before using client protocol.
   *
   * @param client
   * @param regionName
   * @param row
   * @param family
   * @return the row or the closestRowBefore if it doesn't exist
   * @throws IOException
   * @deprecated since 0.99 - use reversed scanner instead.
   */
  @Deprecated
  public static Result getRowOrBefore(final ClientService.BlockingInterface client,
      final byte[] regionName, final byte[] row,
      final byte[] family) throws IOException {
    GetRequest request =
      RequestConverter.buildGetRowOrBeforeRequest(
        regionName, row, family);
    try {
      GetResponse response = client.get(null, request);
      if (!response.hasResult()) return null;
      // We pass 'null' RpcController. So Result will be pure RB.
      return toResult(response.getResult());
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
  public static boolean bulkLoadHFile(final ClientService.BlockingInterface client,
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

  public static CoprocessorServiceResponse execService(final ClientService.BlockingInterface client,
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

  public static CoprocessorServiceResponse execService(
    final MasterService.BlockingInterface client, final CoprocessorServiceCall call)
  throws IOException {
    CoprocessorServiceRequest request = CoprocessorServiceRequest.newBuilder()
        .setCall(call).setRegion(
            RequestConverter.buildRegionSpecifier(REGION_NAME, HConstants.EMPTY_BYTE_ARRAY)).build();
    try {
      CoprocessorServiceResponse response =
          client.execMasterService(null, request);
      return response;
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * Make a region server endpoint call
   * @param client
   * @param call
   * @return CoprocessorServiceResponse
   * @throws IOException
   */
  public static CoprocessorServiceResponse execRegionServerService(
      final ClientService.BlockingInterface client, final CoprocessorServiceCall call)
      throws IOException {
    CoprocessorServiceRequest request =
        CoprocessorServiceRequest
            .newBuilder()
            .setCall(call)
            .setRegion(
              RequestConverter.buildRegionSpecifier(REGION_NAME, HConstants.EMPTY_BYTE_ARRAY))
            .build();
    try {
      CoprocessorServiceResponse response = client.execRegionServerService(null, request);
      return response;
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  @SuppressWarnings("unchecked")
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
  public static HRegionInfo getRegionInfo(final AdminService.BlockingInterface admin,
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
  public static void closeRegion(final AdminService.BlockingInterface admin,
      final ServerName server, final byte[] regionName, final boolean transitionInZK) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(server, regionName, transitionInZK);
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
  public static boolean closeRegion(final AdminService.BlockingInterface admin,
      final ServerName server,
      final byte[] regionName,
      final int versionOfClosingNode, final ServerName destinationServer,
      final boolean transitionInZK) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(server,
        regionName, versionOfClosingNode, destinationServer, transitionInZK);
    try {
      CloseRegionResponse response = admin.closeRegion(null, closeRegionRequest);
      return ResponseConverter.isClosed(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to warmup a region given a region name
   * using admin protocol
   *
   * @param admin
   * @param regionInfo
   *
   */
  public static void warmupRegion(final AdminService.BlockingInterface admin,
      final HRegionInfo regionInfo) throws IOException  {

    try {
      WarmupRegionRequest warmupRegionRequest =
           RequestConverter.buildWarmupRegionRequest(regionInfo);

      admin.warmupRegion(null, warmupRegionRequest);
    } catch (ServiceException e) {
      throw getRemoteException(e);
    }
  }

  /**
   * A helper to open a region using admin protocol.
   * @param admin
   * @param region
   * @throws IOException
   */
  public static void openRegion(final AdminService.BlockingInterface admin,
      ServerName server, final HRegionInfo region) throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(server, region, -1, null, null);
    try {
      admin.openRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
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
  public static List<HRegionInfo> getOnlineRegions(final AdminService.BlockingInterface admin)
  throws IOException {
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
  public static ServerInfo getServerInfo(final AdminService.BlockingInterface admin)
  throws IOException {
    GetServerInfoRequest request = RequestConverter.buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(null, request);
      return response.getServerInfo();
    } catch (ServiceException se) {
      throw getRemoteException(se);
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
  public static List<String> getStoreFiles(final AdminService.BlockingInterface admin,
      final byte[] regionName, final byte[] family)
  throws IOException {
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
  public static void split(final AdminService.BlockingInterface admin,
      final HRegionInfo hri, byte[] splitPoint) throws IOException {
    SplitRegionRequest request =
      RequestConverter.buildSplitRegionRequest(hri.getRegionName(), splitPoint);
    try {
      admin.splitRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * A helper to merge regions using admin protocol. Send request to
   * regionserver.
   * @param admin
   * @param region_a
   * @param region_b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @param user effective user
   * @throws IOException
   */
  public static void mergeRegions(final AdminService.BlockingInterface admin,
      final HRegionInfo region_a, final HRegionInfo region_b,
      final boolean forcible, final User user) throws IOException {
    final MergeRegionsRequest request = RequestConverter.buildMergeRegionsRequest(
        region_a.getRegionName(), region_b.getRegionName(),forcible);
    if (user != null) {
      try {
        user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            admin.mergeRegions(null, request);
            return null;
          }
        });
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    } else {
      try {
        admin.mergeRegions(null, request);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
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
    baos.write(PB_MAGIC);
    m.writeDelimitedTo(baos);
    return baos.toByteArray();
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
   * A utility used to grant a user global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to grant permissions
   * @param actions the permissions to be granted
   * @throws ServiceException
   */
  public static void grant(AccessControlService.BlockingInterface protocol,
      String userShortName, Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = RequestConverter.
      buildGrantRequest(userShortName, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(null, request);
  }

  /**
   * A utility used to grant a user table permissions. The permissions will
   * be for a table table/column family/qualifier.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to grant permissions
   * @param tableName optional table name
   * @param f optional column family
   * @param q optional qualifier
   * @param actions the permissions to be granted
   * @throws ServiceException
   */
  public static void grant(AccessControlService.BlockingInterface protocol,
      String userShortName, TableName tableName, byte[] f, byte[] q,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = RequestConverter.
      buildGrantRequest(userShortName, tableName, f, q, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(null, request);
  }

  /**
   * A utility used to grant a user namespace permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param namespace the short name of the user to grant permissions
   * @param actions the permissions to be granted
   * @throws ServiceException
   */
  public static void grant(AccessControlService.BlockingInterface protocol,
      String userShortName, String namespace,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = RequestConverter.
      buildGrantRequest(userShortName, namespace, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(null, request);
  }

  /**
   * A utility used to revoke a user's global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
  public static void revoke(AccessControlService.BlockingInterface protocol,
      String userShortName, Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = RequestConverter.
      buildRevokeRequest(userShortName, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(null, request);
  }

  /**
   * A utility used to revoke a user's table permissions. The permissions will
   * be for a table/column family/qualifier.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param tableName optional table name
   * @param f optional column family
   * @param q optional qualifier
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
  public static void revoke(AccessControlService.BlockingInterface protocol,
      String userShortName, TableName tableName, byte[] f, byte[] q,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = RequestConverter.
      buildRevokeRequest(userShortName, tableName, f, q, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(null, request);
  }

  /**
   * A utility used to revoke a user's namespace permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param namespace optional table name
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
  public static void revoke(AccessControlService.BlockingInterface protocol,
      String userShortName, String namespace,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(ProtobufUtil.toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = RequestConverter.
      buildRevokeRequest(userShortName, namespace, permActions.toArray(
        new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(null, request);
  }

  /**
   * A utility used to get user's global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(
      AccessControlService.BlockingInterface protocol) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
      AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    builder.setType(AccessControlProtos.Permission.Type.Global);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
      protocol.getUserPermissions(null, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(ProtobufUtil.toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get user table permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param t optional table name
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(
      AccessControlService.BlockingInterface protocol,
      TableName t) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
      AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (t != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(t));
    }
    builder.setType(AccessControlProtos.Permission.Type.Table);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
      protocol.getUserPermissions(null, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(ProtobufUtil.toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get permissions for selected namespace.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param namespace name of the namespace
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(
      AccessControlService.BlockingInterface protocol,
      byte[] namespace) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
      AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (namespace != null) {
      builder.setNamespaceName(ByteStringer.wrap(namespace));
    }
    builder.setType(AccessControlProtos.Permission.Type.Namespace);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
      protocol.getUserPermissions(null, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(ProtobufUtil.toUserPermission(perm));
    }
    return perms;
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

  /**
   * Find the HRegion encoded name based on a region specifier
   *
   * @param regionSpecifier the region specifier
   * @return the corresponding region's encoded name
   * @throws DoNotRetryIOException if the specifier type is unsupported
   */
  public static String getRegionEncodedName(
      final RegionSpecifier regionSpecifier) throws DoNotRetryIOException {
    ByteString value = regionSpecifier.getValue();
    RegionSpecifierType type = regionSpecifier.getType();
    switch (type) {
      case REGION_NAME:
        return HRegionInfo.encodeRegionName(value.toByteArray());
      case ENCODED_REGION_NAME:
        return value.toStringUtf8();
      default:
        throw new DoNotRetryIOException(
          "Unsupported region specifier type: " + type);
    }
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
    for (Entry<String, Long> e : metrics.entrySet()) {
      HBaseProtos.NameInt64Pair nameInt64Pair =
          HBaseProtos.NameInt64Pair.newBuilder()
              .setName(e.getKey())
              .setValue(e.getValue())
              .build();
      builder.addMetrics(nameInt64Pair);
    }
    return builder.build();
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

  /**
   * Get an instance of the argument type declared in a class's signature. The
   * argument type is assumed to be a PB Message subclass, and the instance is
   * created using parseFrom method on the passed ByteString.
   * @param runtimeClass the runtime type of the class
   * @param position the position of the argument in the class declaration
   * @param b the ByteString which should be parsed to get the instance created
   * @return the instance
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T extends Message>
  T getParsedGenericInstance(Class<?> runtimeClass, int position, ByteString b)
      throws IOException {
    Type type = runtimeClass.getGenericSuperclass();
    Type argType = ((ParameterizedType)type).getActualTypeArguments()[position];
    Class<T> classType = (Class<T>)argType;
    T inst;
    try {
      Method m = classType.getMethod("parseFrom", ByteString.class);
      inst = (T)m.invoke(null, b);
      return inst;
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  public static CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] family,
      List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    return toCompactionDescriptor(info, null, family, inputPaths, outputPaths, storeDir);
  }

  @SuppressWarnings("deprecation")
  public static CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] regionName,
      byte[] family, List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    // compaction descriptor contains relative paths.
    // input / output paths are relative to the store dir
    // store dir is relative to region dir
    CompactionDescriptor.Builder builder = CompactionDescriptor.newBuilder()
        .setTableName(ByteStringer.wrap(info.getTableName()))
        .setEncodedRegionName(ByteStringer.wrap(
          regionName == null ? info.getEncodedNameAsBytes() : regionName))
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

  public static FlushDescriptor toFlushDescriptor(FlushAction action, HRegionInfo hri,
      long flushSeqId, Map<byte[], List<Path>> committedFiles) {
    FlushDescriptor.Builder desc = FlushDescriptor.newBuilder()
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

  public static RegionEventDescriptor toRegionEventDescriptor(
      EventType eventType, HRegionInfo hri, long seqId, ServerName server,
      Map<byte[], List<Path>> storeFiles) {
    final byte[] tableNameAsBytes = hri.getTable().getName();
    final byte[] encodedNameAsBytes = hri.getEncodedNameAsBytes();
    final byte[] regionNameAsBytes = hri.getRegionName();
    return toRegionEventDescriptor(eventType,
        tableNameAsBytes,
        encodedNameAsBytes,
        regionNameAsBytes,
        seqId,

        server,
        storeFiles);
  }

  public static RegionEventDescriptor toRegionEventDescriptor(EventType eventType,
                                                              byte[] tableNameAsBytes,
                                                              byte[] encodedNameAsBytes,
                                                              byte[] regionNameAsBytes,
                                                               long seqId,

                                                              ServerName server,
                                                              Map<byte[], List<Path>> storeFiles) {
    RegionEventDescriptor.Builder desc = RegionEventDescriptor.newBuilder()
        .setEventType(eventType)
        .setTableName(ByteStringer.wrap(tableNameAsBytes))
        .setEncodedRegionName(ByteStringer.wrap(encodedNameAsBytes))
        .setRegionName(ByteStringer.wrap(regionNameAsBytes))
        .setLogSequenceNumber(seqId)
        .setServer(toServerName(server));

    for (Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      StoreDescriptor.Builder builder = StoreDescriptor.newBuilder()
          .setFamilyName(ByteStringer.wrap(entry.getKey()))
          .setStoreHomeDir(Bytes.toString(entry.getKey()));
      for (Path path : entry.getValue()) {
        builder.addStoreFile(path.getName());
      }

      desc.addStores(builder);
    }
    return desc.build();
  }

  /**
   * Return short version of Message toString'd, shorter than TextFormat#shortDebugString.
   * Tries to NOT print out data both because it can be big but also so we do not have data in our
   * logs. Use judiciously.
   * @param m
   * @return toString of passed <code>m</code>
   */
  public static String getShortTextFormat(Message m) {
    if (m == null) return "null";
    if (m instanceof ScanRequest) {
      // This should be small and safe to output.  No data.
      return TextFormat.shortDebugString(m);
    } else if (m instanceof RegionServerReportRequest) {
      // Print a short message only, just the servername and the requests, not the full load.
      RegionServerReportRequest r = (RegionServerReportRequest)m;
      return "server " + TextFormat.shortDebugString(r.getServer()) +
        " load { numberOfRequests: " + r.getLoad().getNumberOfRequests() + " }";
    } else if (m instanceof RegionServerStartupRequest) {
      // Should be small enough.
      return TextFormat.shortDebugString(m);
    } else if (m instanceof MutationProto) {
      return toShortString((MutationProto)m);
    } else if (m instanceof GetRequest) {
      GetRequest r = (GetRequest) m;
      return "region= " + getStringForByteString(r.getRegion().getValue()) +
          ", row=" + getStringForByteString(r.getGet().getRow());
    } else if (m instanceof ClientProtos.MultiRequest) {
      ClientProtos.MultiRequest r = (ClientProtos.MultiRequest) m;
      // Get first set of Actions.
      ClientProtos.RegionAction actions = r.getRegionActionList().get(0);
      String row = actions.getActionCount() <= 0? "":
        getStringForByteString(actions.getAction(0).hasGet()?
          actions.getAction(0).getGet().getRow():
          actions.getAction(0).getMutation().getRow());
      return "region= " + getStringForByteString(actions.getRegion().getValue()) +
          ", for " + r.getRegionActionCount() +
          " actions and 1st row key=" + row;
    } else if (m instanceof ClientProtos.MutateRequest) {
      ClientProtos.MutateRequest r = (ClientProtos.MutateRequest) m;
      return "region= " + getStringForByteString(r.getRegion().getValue()) +
          ", row=" + getStringForByteString(r.getMutation().getRow());
    }
    return "TODO: " + m.getClass().toString();
  }

  private static String getStringForByteString(ByteString bs) {
    return Bytes.toStringBinary(bs.toByteArray());
  }

  /**
   * Print out some subset of a MutationProto rather than all of it and its data
   * @param proto Protobuf to print out
   * @return Short String of mutation proto
   */
  static String toShortString(final MutationProto proto) {
    return "row=" + Bytes.toString(proto.getRow().toByteArray()) +
        ", type=" + proto.getMutateType().toString();
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

  public static TableName[] getTableNameArray(List<HBaseProtos.TableName> tableNamesList) {
    if (tableNamesList == null) {
      return new TableName[0];
    }
    TableName[] tableNames = new TableName[tableNamesList.size()];
    for (int i = 0; i < tableNamesList.size(); i++) {
      tableNames[i] = toTableName(tableNamesList.get(i));
    }
    return tableNames;
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
      ProtobufUtil.mergeFrom(builder, protoBytes);
      proto = builder.build();
    } catch (IOException e) {
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
      ProtobufUtil.mergeFrom(builder, protoBytes);
      proto = builder.build();
    } catch (IOException e) {
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
   * @param proto
   * @return the converted client TimeUnit
   */
  public static TimeUnit toTimeUnit(final HBaseProtos.TimeUnit proto) {
    switch (proto) {
    case NANOSECONDS:
      return TimeUnit.NANOSECONDS;
    case MICROSECONDS:
      return TimeUnit.MICROSECONDS;
    case MILLISECONDS:
      return TimeUnit.MILLISECONDS;
    case SECONDS:
      return TimeUnit.SECONDS;
    case MINUTES:
      return TimeUnit.MINUTES;
    case HOURS:
      return TimeUnit.HOURS;
    case DAYS:
      return TimeUnit.DAYS;
    default:
      throw new RuntimeException("Invalid TimeUnit " + proto);
    }
  }

  /**
   * Convert a client TimeUnit to a protocol buffer TimeUnit
   * @param timeUnit
   * @return the converted protocol buffer TimeUnit
   */
  public static HBaseProtos.TimeUnit toProtoTimeUnit(final TimeUnit timeUnit) {
    switch (timeUnit) {
    case NANOSECONDS:
      return HBaseProtos.TimeUnit.NANOSECONDS;
    case MICROSECONDS:
      return HBaseProtos.TimeUnit.MICROSECONDS;
    case MILLISECONDS:
      return HBaseProtos.TimeUnit.MILLISECONDS;
    case SECONDS:
      return HBaseProtos.TimeUnit.SECONDS;
    case MINUTES:
      return HBaseProtos.TimeUnit.MINUTES;
    case HOURS:
      return HBaseProtos.TimeUnit.HOURS;
    case DAYS:
      return HBaseProtos.TimeUnit.DAYS;
    default:
      throw new RuntimeException("Invalid TimeUnit " + timeUnit);
    }
  }

  /**
   * Convert a protocol buffer ThrottleType to a client ThrottleType
   * @param proto
   * @return the converted client ThrottleType
   */
  public static ThrottleType toThrottleType(final QuotaProtos.ThrottleType proto) {
    switch (proto) {
    case REQUEST_NUMBER:
      return ThrottleType.REQUEST_NUMBER;
    case REQUEST_SIZE:
      return ThrottleType.REQUEST_SIZE;
    case WRITE_NUMBER:
      return ThrottleType.WRITE_NUMBER;
    case WRITE_SIZE:
      return ThrottleType.WRITE_SIZE;
    case READ_NUMBER:
      return ThrottleType.READ_NUMBER;
    case READ_SIZE:
      return ThrottleType.READ_SIZE;
    default:
      throw new RuntimeException("Invalid ThrottleType " + proto);
    }
  }

  /**
   * Convert a client ThrottleType to a protocol buffer ThrottleType
   * @param type
   * @return the converted protocol buffer ThrottleType
   */
  public static QuotaProtos.ThrottleType toProtoThrottleType(final ThrottleType type) {
    switch (type) {
    case REQUEST_NUMBER:
      return QuotaProtos.ThrottleType.REQUEST_NUMBER;
    case REQUEST_SIZE:
      return QuotaProtos.ThrottleType.REQUEST_SIZE;
    case WRITE_NUMBER:
      return QuotaProtos.ThrottleType.WRITE_NUMBER;
    case WRITE_SIZE:
      return QuotaProtos.ThrottleType.WRITE_SIZE;
    case READ_NUMBER:
      return QuotaProtos.ThrottleType.READ_NUMBER;
    case READ_SIZE:
      return QuotaProtos.ThrottleType.READ_SIZE;
    default:
      throw new RuntimeException("Invalid ThrottleType " + type);
    }
  }

  /**
   * Convert a protocol buffer QuotaScope to a client QuotaScope
   * @param proto
   * @return the converted client QuotaScope
   */
  public static QuotaScope toQuotaScope(final QuotaProtos.QuotaScope proto) {
    switch (proto) {
    case CLUSTER:
      return QuotaScope.CLUSTER;
    case MACHINE:
      return QuotaScope.MACHINE;
    default:
      throw new RuntimeException("Invalid QuotaScope " + proto);
    }
  }

  /**
   * Convert a client QuotaScope to a protocol buffer QuotaScope
   * @param scope
   * @return the converted protocol buffer QuotaScope
   */
  public static QuotaProtos.QuotaScope toProtoQuotaScope(final QuotaScope scope) {
    switch (scope) {
    case CLUSTER:
      return QuotaProtos.QuotaScope.CLUSTER;
    case MACHINE:
      return QuotaProtos.QuotaScope.MACHINE;
    default:
      throw new RuntimeException("Invalid QuotaScope " + scope);
    }
  }

  /**
   * Convert a protocol buffer QuotaType to a client QuotaType
   * @param proto
   * @return the converted client QuotaType
   */
  public static QuotaType toQuotaScope(final QuotaProtos.QuotaType proto) {
    switch (proto) {
    case THROTTLE:
      return QuotaType.THROTTLE;
    default:
      throw new RuntimeException("Invalid QuotaType " + proto);
    }
  }

  /**
   * Convert a client QuotaType to a protocol buffer QuotaType
   * @param type
   * @return the converted protocol buffer QuotaType
   */
  public static QuotaProtos.QuotaType toProtoQuotaScope(final QuotaType type) {
    switch (type) {
    case THROTTLE:
      return QuotaProtos.QuotaType.THROTTLE;
    default:
      throw new RuntimeException("Invalid QuotaType " + type);
    }
  }

  /**
   * Build a protocol buffer TimedQuota
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the quota scope
   * @return the protocol buffer TimedQuota
   */
  public static QuotaProtos.TimedQuota toTimedQuota(final long limit, final TimeUnit timeUnit,
      final QuotaScope scope) {
    return QuotaProtos.TimedQuota.newBuilder().setSoftLimit(limit)
        .setTimeUnit(toProtoTimeUnit(timeUnit)).setScope(toProtoQuotaScope(scope)).build();
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
    BulkLoadDescriptor.Builder desc = BulkLoadDescriptor.newBuilder()
        .setTableName(ProtobufUtil.toProtoTableName(tableName))
        .setEncodedRegionName(encodedRegionName).setBulkloadSeqNum(bulkloadSeqId);

    for (Map.Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      WALProtos.StoreDescriptor.Builder builder = StoreDescriptor.newBuilder()
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
   * This version of protobuf's mergeDelimitedFrom avoids the hard-coded 64MB limit for decoding
   * buffers
   * @param builder current message builder
   * @param in Inputsream with delimited protobuf data
   * @throws IOException
   */
  public static void mergeDelimitedFrom(Message.Builder builder, InputStream in)
    throws IOException {
    // This used to be builder.mergeDelimitedFrom(in);
    // but is replaced to allow us to bump the protobuf size limit.
    final int firstByte = in.read();
    if (firstByte != -1) {
      final int size = CodedInputStream.readRawVarint32(firstByte, in);
      final InputStream limitedInput = new LimitInputStream(in, size);
      final CodedInputStream codedInput = CodedInputStream.newInstance(limitedInput);
      codedInput.setSizeLimit(size);
      builder.mergeFrom(codedInput);
      codedInput.checkLastTagWas(0);
    }
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers where the message size is known
   * @param builder current message builder
   * @param in InputStream containing protobuf data
   * @param size known size of protobuf data
   * @throws IOException 
   */
  public static void mergeFrom(Message.Builder builder, InputStream in, int size)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(size);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers where the message size is not known
   * @param builder current message builder
   * @param in InputStream containing protobuf data
   * @throws IOException 
   */
  public static void mergeFrom(Message.Builder builder, InputStream in)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(Integer.MAX_VALUE);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with ByteStrings
   * @param builder current message builder
   * @param bs ByteString containing the 
   * @throws IOException 
   */
  public static void mergeFrom(Message.Builder builder, ByteString bs) throws IOException {
    final CodedInputStream codedInput = bs.newCodedInput();
    codedInput.setSizeLimit(bs.size());
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with byte arrays
   * @param builder current message builder
   * @param b byte array
   * @throws IOException 
   */
  public static void mergeFrom(Message.Builder builder, byte[] b) throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b);
    codedInput.setSizeLimit(b.length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with byte arrays
   * @param builder current message builder
   * @param b byte array
   * @param offset
   * @param length
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, byte[] b, int offset, int length)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b, offset, length);
    codedInput.setSizeLimit(length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  public static void mergeFrom(Message.Builder builder, CodedInputStream codedInput, int length)
      throws IOException {
    codedInput.resetSizeCounter();
    int prevLimit = codedInput.setSizeLimit(length);

    int limit = codedInput.pushLimit(length);
    builder.mergeFrom(codedInput);
    codedInput.popLimit(limit);

    codedInput.checkLastTagWas(0);
    codedInput.setSizeLimit(prevLimit);
  }

  public static ReplicationLoadSink toReplicationLoadSink(
      ClusterStatusProtos.ReplicationLoadSink cls) {
    return new ReplicationLoadSink(cls.getAgeOfLastAppliedOp(), cls.getTimeStampsOfLastAppliedOp());
  }

  public static ReplicationLoadSource toReplicationLoadSource(
      ClusterStatusProtos.ReplicationLoadSource cls) {
    return new ReplicationLoadSource(cls.getPeerID(), cls.getAgeOfLastShippedOp(),
        cls.getSizeOfLogQueue(), cls.getTimeStampOfLastShippedOp(), cls.getReplicationLag());
  }

  public static List<ReplicationLoadSource> toReplicationLoadSourceList(
      List<ClusterStatusProtos.ReplicationLoadSource> clsList) {
    ArrayList<ReplicationLoadSource> rlsList = new ArrayList<ReplicationLoadSource>();
    for (ClusterStatusProtos.ReplicationLoadSource cls : clsList) {
      rlsList.add(toReplicationLoadSource(cls));
    }
    return rlsList;
  }

  /**
   * Get a protocol buffer VersionInfo
   *
   * @return the converted protocol buffer VersionInfo
   */
  public static HBaseProtos.VersionInfo getVersionInfo() {
    HBaseProtos.VersionInfo.Builder builder = HBaseProtos.VersionInfo.newBuilder();
    String version = VersionInfo.getVersion();
    builder.setVersion(version);
    String[] components = version.split("\\.");
    if (components != null && components.length > 2) {
      builder.setVersionMajor(Integer.parseInt(components[0]));
      builder.setVersionMinor(Integer.parseInt(components[1]));
    }
    builder.setUrl(VersionInfo.getUrl());
    builder.setRevision(VersionInfo.getRevision());
    builder.setUser(VersionInfo.getUser());
    builder.setDate(VersionInfo.getDate());
    builder.setSrcChecksum(VersionInfo.getSrcChecksum());
    return builder.build();
  }

  /**
   * Convert SecurityCapabilitiesResponse.Capability to SecurityCapability
   * @param capabilities capabilities returned in the SecurityCapabilitiesResponse message
   * @return the converted list of SecurityCapability elements
   */
  public static List<SecurityCapability> toSecurityCapabilityList(
      List<MasterProtos.SecurityCapabilitiesResponse.Capability> capabilities) {
    List<SecurityCapability> scList = new ArrayList<>(capabilities.size());
    for (MasterProtos.SecurityCapabilitiesResponse.Capability c: capabilities) {
      try {
        scList.add(SecurityCapability.valueOf(c.getNumber()));
      } catch (IllegalArgumentException e) {
        // Unknown capability, just ignore it. We don't understand the new capability
        // but don't care since by definition we cannot take advantage of it.
      }
    }
    return scList;
  }

  private static HBaseProtos.TimeRange.Builder timeRangeToProto(TimeRange timeRange) {
    HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
    timeRangeBuilder.setFrom(timeRange.getMin());
    timeRangeBuilder.setTo(timeRange.getMax());
    return timeRangeBuilder;
  }

  private static TimeRange protoToTimeRange(HBaseProtos.TimeRange timeRange) throws IOException {
      long minStamp = 0;
      long maxStamp = Long.MAX_VALUE;
      if (timeRange.hasFrom()) {
        minStamp = timeRange.getFrom();
      }
      if (timeRange.hasTo()) {
        maxStamp = timeRange.getTo();
      }
    return new TimeRange(minStamp, maxStamp);
  }

}
