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

import static org.apache.hadoop.hbase.protobuf.ProtobufMagic.PB_MAGIC;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.protobuf.generated.TableProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Protobufs utility.
 * NOTE: This class OVERLAPS ProtobufUtil in the subpackage 'shaded'. The latter is used
 * internally and has more methods. This Class is for Coprocessor Endpoints only though they
 * should not be using this private class. It should not be depended upon. Most methods here
 * are COPIED from the shaded ProtobufUtils with only difference being they refer to non-shaded
 * protobufs.
 * @see ProtobufUtil
 */
// TODO: Generate this class from the shaded version.
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class.
public final class ProtobufUtil {
  private ProtobufUtil() {
  }

  /**
   * Many results are simple: no cell, exists true or false. To save on object creations,
   *  we reuse them across calls.
   */
  // TODO: PICK THESE UP FROM THE SHADED PROTOBUF.
  private final static Cell[] EMPTY_CELL_ARRAY = new Cell[]{};
  private final static Result EMPTY_RESULT = Result.create(EMPTY_CELL_ARRAY);
  final static Result EMPTY_RESULT_EXISTS_TRUE = Result.create(null, true);
  final static Result EMPTY_RESULT_EXISTS_FALSE = Result.create(null, false);
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
  private final static class ClassLoaderHolder {
    private final static ClassLoader CLASS_LOADER;

    static {
      ClassLoader parent = ProtobufUtil.class.getClassLoader();
      Configuration conf = HBaseConfiguration.create();
      CLASS_LOADER = AccessController.doPrivileged((PrivilegedAction<ClassLoader>)
        () -> new DynamicClassLoader(conf, parent)
      );
    }
  }

  /**
   * Prepend the passed bytes with four bytes of magic, {@link ProtobufMagic#PB_MAGIC},
   * to flag what follows as a protobuf in hbase.  Prepend these bytes to all content written to
   * znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</code> with magic prepended (Creates a new
   * byte array that is <code>bytes.length</code> plus {@link ProtobufMagic#PB_MAGIC}.length.
   */
  public static byte [] prependPBMagic(final byte [] bytes) {
    return Bytes.add(PB_MAGIC, bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @return True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes) {
    return ProtobufMagic.isPBMagicPrefix(bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @param offset offset to start at
   * @param len length to use
   * @return True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes, int offset, int len) {
    return ProtobufMagic.isPBMagicPrefix(bytes, offset, len);
  }

  /**
   * @param bytes bytes to check
   * @throws DeserializationException if we are missing the pb magic prefix
   */
  public static void expectPBMagicPrefix(final byte[] bytes) throws DeserializationException {
    if (!isPBMagicPrefix(bytes)) {
      String bytesPrefix = bytes == null ? "null" : Bytes.toStringBinary(bytes, 0, PB_MAGIC.length);
      throw new DeserializationException(
          "Missing pb magic " + Bytes.toString(PB_MAGIC) + " prefix, bytes: " + bytesPrefix);
    }
  }

  /**
   * @return Length of {@link ProtobufMagic#lengthOfPBMagic()}
   */
  public static int lengthOfPBMagic() {
    return ProtobufMagic.lengthOfPBMagic();
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
    return makeIOExceptionOfException(se);
  }

  /**
   * Return the Exception thrown by the remote server wrapped in
   * ServiceException as cause. RemoteException are left untouched.
   *
   * @param e ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException.
   */
  public static IOException getServiceException(
      org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
    Throwable t = e.getCause();
    if (ExceptionUtil.isInterrupt(t)) {
      return ExceptionUtil.asInterrupt(t);
    }
    return t instanceof IOException ? (IOException) t : new HBaseIOException(t);
  }

  /**
   * Like {@link #getRemoteException(ServiceException)} but more generic, able to handle more than
   * just {@link ServiceException}. Prefer this method to
   * {@link #getRemoteException(ServiceException)} because trying to
   * contain direct protobuf references.
   * @param e
   */
  public static IOException handleRemoteException(Exception e) {
    return makeIOExceptionOfException(e);
  }

  private static IOException makeIOExceptionOfException(Exception e) {
    Throwable t = e;
    if (e instanceof ServiceException ||
        e instanceof org.apache.hbase.thirdparty.com.google.protobuf.ServiceException) {
      t = e.getCause();
    }
    if (ExceptionUtil.isInterrupt(t)) {
      return ExceptionUtil.asInterrupt(t);
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException)t).unwrapRemoteException();
    }
    return t instanceof IOException? (IOException)t: new HBaseIOException(t);
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
  public static Get toGet(final ClientProtos.Get proto) throws IOException {
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
    if (proto.hasConsistency()) {
      get.setConsistency(toConsistency(proto.getConsistency()));
    }
    if (proto.hasLoadColumnFamiliesOnDemand()) {
      get.setLoadColumnFamiliesOnDemand(proto.getLoadColumnFamiliesOnDemand());
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
      ExtendedCellBuilder cellBuilder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
      for (ColumnValue column: proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv: column.getQualifierValueList()) {
          if (!qv.hasValue()) {
            throw new DoNotRetryIOException(
                "Missing required field: qualifier value");
          }
          long ts = timestamp;
          if (qv.hasTimestamp()) {
            ts = qv.getTimestamp();
          }
          byte[] allTagsBytes;
          if (qv.hasTags()) {
            allTagsBytes = qv.getTags().toByteArray();
            if(qv.hasDeleteType()) {
              put.add(cellBuilder.clear()
                  .setRow(put.getRow())
                  .setFamily(family)
                  .setQualifier(qv.hasQualifier() ? qv.getQualifier().toByteArray() : null)
                  .setTimestamp(ts)
                  .setType(fromDeleteType(qv.getDeleteType()).getCode())
                  .setTags(allTagsBytes)
                  .build());
            } else {
              put.add(cellBuilder.clear()
                  .setRow(put.getRow())
                  .setFamily(family)
                  .setQualifier(qv.hasQualifier() ? qv.getQualifier().toByteArray() : null)
                  .setTimestamp(ts)
                  .setType(Cell.Type.Put)
                  .setValue(qv.hasValue() ? qv.getValue().toByteArray() : null)
                  .setTags(allTagsBytes)
                  .build());
            }
          } else {
            if(qv.hasDeleteType()) {
              put.add(cellBuilder.clear()
                  .setRow(put.getRow())
                  .setFamily(family)
                  .setQualifier(qv.hasQualifier() ? qv.getQualifier().toByteArray() : null)
                  .setTimestamp(ts)
                  .setType(fromDeleteType(qv.getDeleteType()).getCode())
                  .build());
            } else{
              put.add(cellBuilder.clear()
                  .setRow(put.getRow())
                  .setFamily(family)
                  .setQualifier(qv.hasQualifier() ? qv.getQualifier().toByteArray() : null)
                  .setTimestamp(ts)
                  .setType(Type.Put)
                  .setValue(qv.hasValue() ? qv.getValue().toByteArray() : null)
                  .build());
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
        delete.add(cell);
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
          long ts = cellTimestampOrLatest(qv);
          if (deleteType == DeleteType.DELETE_ONE_VERSION) {
            delete.addColumn(family, qualifier, ts);
          } else if (deleteType == DeleteType.DELETE_MULTIPLE_VERSIONS) {
            delete.addColumns(family, qualifier, ts);
          } else if (deleteType == DeleteType.DELETE_FAMILY_VERSION) {
            delete.addFamilyVersion(family, ts);
          } else {
            delete.addFamily(family, ts);
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

  @FunctionalInterface
  private interface ConsumerWithException <T, U> {
    void accept(T t, U u) throws IOException;
  }

  private static <T extends Mutation> T toDelta(Function<Bytes, T> supplier, ConsumerWithException<T, Cell> consumer,
      final MutationProto proto, final CellScanner cellScanner) throws IOException {
    byte[] row = proto.hasRow() ? proto.getRow().toByteArray() : null;
    T mutation = row == null ? null : supplier.apply(new Bytes(row));
    int cellCount = proto.hasAssociatedCellCount() ? proto.getAssociatedCellCount() : 0;
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
        if (mutation == null) {
          mutation = supplier.apply(new Bytes(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        }
        consumer.accept(mutation, cell);
      }
    } else {
      if (mutation == null) {
        throw new IllegalArgumentException("row cannot be null");
      }
      for (ColumnValue column : proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv : column.getQualifierValueList()) {
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
          consumer.accept(mutation, CellUtil.createCell(mutation.getRow(), family, qualifier, cellTimestampOrLatest(qv),
                  KeyValue.Type.Put, value, tags));
        }
      }
    }
    mutation.setDurability(toDurability(proto.getDurability()));
    for (NameBytesPair attribute : proto.getAttributeList()) {
      mutation.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return mutation;
  }

  private static long cellTimestampOrLatest(QualifierValue cell) {
    if (cell.hasTimestamp()) {
      return cell.getTimestamp();
    } else {
      return HConstants.LATEST_TIMESTAMP;
    }
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
    Append append = toDelta((Bytes row) -> new Append(row.get(), row.getOffset(), row.getLength()),
        Append::add, proto, cellScanner);
    if (proto.hasTimeRange()) {
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      append.setTimeRange(timeRange.getMin(), timeRange.getMax());
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
  public static Increment toIncrement(final MutationProto proto, final CellScanner cellScanner)
          throws IOException {
    MutationType type = proto.getMutateType();
    assert type == MutationType.INCREMENT : type.name();
    Increment increment = toDelta((Bytes row) -> new Increment(row.get(), row.getOffset(), row.getLength()),
            Increment::add, proto, cellScanner);
    if (proto.hasTimeRange()) {
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      increment.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }
    return increment;
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
    if (type == MutationType.INCREMENT) {
      return toIncrement(proto, null);
    }
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
   * Convert a protocol buffer Mutate to a Get.
   * @param proto the protocol buffer Mutate to convert.
   * @param cellScanner
   * @return the converted client get.
   * @throws IOException
   */
  public static Get toGet(final MutationProto proto, final CellScanner cellScanner)
      throws IOException {
    MutationType type = proto.getMutateType();
    assert type == MutationType.INCREMENT || type == MutationType.APPEND : type.name();
    byte[] row = proto.hasRow() ? proto.getRow().toByteArray() : null;
    Get get = null;
    int cellCount = proto.hasAssociatedCellCount() ? proto.getAssociatedCellCount() : 0;
    if (cellCount > 0) {
      // The proto has metadata only and the data is separate to be found in the cellScanner.
      if (cellScanner == null) {
        throw new DoNotRetryIOException("Cell count of " + cellCount + " but no cellScanner: "
            + TextFormat.shortDebugString(proto));
      }
      for (int i = 0; i < cellCount; i++) {
        if (!cellScanner.advance()) {
          throw new DoNotRetryIOException("Cell count of " + cellCount + " but at index " + i
              + " no cell returned: " + TextFormat.shortDebugString(proto));
        }
        Cell cell = cellScanner.current();
        if (get == null) {
          get = new Get(Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        }
        get.addColumn(
          Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()),
          Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength()));
      }
    } else {
      get = new Get(row);
      for (ColumnValue column : proto.getColumnValueList()) {
        byte[] family = column.getFamily().toByteArray();
        for (QualifierValue qv : column.getQualifierValueList()) {
          byte[] qualifier = qv.getQualifier().toByteArray();
          if (!qv.hasValue()) {
            throw new DoNotRetryIOException("Missing required field: qualifier value");
          }
          get.addColumn(family, qualifier);
        }
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = protoToTimeRange(proto.getTimeRange());
      get.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }
    for (NameBytesPair attribute : proto.getAttributeList()) {
      get.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    return get;
  }

  public static ClientProtos.Scan.ReadType toReadType(Scan.ReadType readType) {
    switch (readType) {
      case DEFAULT:
        return ClientProtos.Scan.ReadType.DEFAULT;
      case STREAM:
        return ClientProtos.Scan.ReadType.STREAM;
      case PREAD:
        return ClientProtos.Scan.ReadType.PREAD;
      default:
        throw new IllegalArgumentException("Unknown ReadType: " + readType);
    }
  }

  public static Scan.ReadType toReadType(ClientProtos.Scan.ReadType readType) {
    switch (readType) {
      case DEFAULT:
        return Scan.ReadType.DEFAULT;
      case STREAM:
        return Scan.ReadType.STREAM;
      case PREAD:
        return Scan.ReadType.PREAD;
      default:
        throw new IllegalArgumentException("Unknown ReadType: " + readType);
    }
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
      scanBuilder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
    }
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    scan.getColumnFamilyTimeRange().forEach((cf, timeRange) -> {
      scanBuilder.addCfTimeRange(HBaseProtos.ColumnFamilyTimeRange.newBuilder()
        .setColumnFamily(ByteStringer.wrap(cf))
        .setTimeRange(toTimeRange(timeRange))
        .build());
    });
    scanBuilder.setTimeRange(toTimeRange(scan.getTimeRange()));
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
    long mvccReadPoint = PackagePrivateFieldAccessor.getMvccReadPoint(scan);
    if (mvccReadPoint > 0) {
      scanBuilder.setMvccReadPoint(mvccReadPoint);
    }
    if (!scan.includeStartRow()) {
      scanBuilder.setIncludeStartRow(false);
    }
    scanBuilder.setIncludeStopRow(scan.includeStopRow());
    if (scan.getReadType() != Scan.ReadType.DEFAULT) {
      scanBuilder.setReadType(toReadType(scan.getReadType()));
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
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    boolean includeStartRow = true;
    boolean includeStopRow = false;
    if (proto.hasStartRow()) {
      startRow = proto.getStartRow().toByteArray();
    }
    if (proto.hasStopRow()) {
      stopRow = proto.getStopRow().toByteArray();
    }
    if (proto.hasIncludeStartRow()) {
      includeStartRow = proto.getIncludeStartRow();
    }
    if (proto.hasIncludeStopRow()) {
      includeStopRow = proto.getIncludeStopRow();
    }
    Scan scan =
        new Scan().withStartRow(startRow, includeStartRow).withStopRow(stopRow, includeStopRow);
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
    if (proto.hasMvccReadPoint()) {
      PackagePrivateFieldAccessor.setMvccReadPoint(scan, proto.getMvccReadPoint());
    }
    if (scan.isSmall()) {
      scan.setReadType(Scan.ReadType.PREAD);
    } else if (proto.hasReadType()) {
      scan.setReadType(toReadType(proto.getReadType()));
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
    get.getColumnFamilyTimeRange().forEach((cf, timeRange) ->
      builder.addCfTimeRange(HBaseProtos.ColumnFamilyTimeRange.newBuilder()
        .setColumnFamily(ByteStringer.wrap(cf))
        .setTimeRange(toTimeRange(timeRange)).build())
    );
    builder.setTimeRange(toTimeRange(get.getTimeRange()));
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
    if (get.getConsistency() != null && get.getConsistency() != Consistency.STRONG) {
      builder.setConsistency(toConsistency(get.getConsistency()));
    }

    Boolean loadColumnFamiliesOnDemand = get.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      builder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
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
    if (type == MutationType.INCREMENT) {
      builder.setTimeRange(toTimeRange(((Increment) mutation).getTimeRange()));
    }
    if (type == MutationType.APPEND) {
      builder.setTimeRange(toTimeRange(((Append) mutation).getTimeRange()));
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
    if (mutation instanceof Increment) {
      builder.setTimeRange(toTimeRange(((Increment)mutation).getTimeRange()));
    }
    if (mutation instanceof Append) {
      builder.setTimeRange(toTimeRange(((Append)mutation).getTimeRange()));
    }
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
    builder.setTimestamp(mutation.getTimestamp());
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
    builder.setPartial(result.mayHaveMoreCellsInRow());

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

    List<Cell> cells = new ArrayList<>(values.size());
    ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (CellProtos.Cell c : values) {
      cells.add(toCell(builder, c));
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
      if (!values.isEmpty() ||
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
      cells = new ArrayList<>(count + values.size());
      for (int i = 0; i < count; i++) {
        if (!scanner.advance()) throw new IOException("Failed get " + i + " of " + count);
        cells.add(scanner.current());
      }
    }

    if (!values.isEmpty()){
      if (cells == null) cells = new ArrayList<>(values.size());
      ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
      for (CellProtos.Cell c: values) {
        cells.add(toCell(builder, c));
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
      Class<?> c = Class.forName(type, true, ClassLoaderHolder.CLASS_LOADER);
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
      Class<?> c = Class.forName(type, true, ClassLoaderHolder.CLASS_LOADER);
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
        (Class<? extends Throwable>)Class.forName(type, true, ClassLoaderHolder.CLASS_LOADER);
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

  @SuppressWarnings("unchecked")
  public static <T extends Service> T newServiceStub(Class<T> service, RpcChannel channel)
      throws Exception {
    return (T)Methods.call(service, null, "newStub",
        new Class[]{ RpcChannel.class }, new Object[]{ channel });
  }

// End helpers for Client
// Start helpers for Admin

  /**
   * A helper to get the info of a region server using admin protocol.
   * @return the server name
   */
  public static ServerInfo getServerInfo(final RpcController controller,
      final AdminService.BlockingInterface admin)
  throws IOException {
    GetServerInfoRequest request = buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(controller, request);
      return response.getServerInfo();
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }


  /**
   * @see #buildGetServerInfoRequest()
   */
  private static GetServerInfoRequest GET_SERVER_INFO_REQUEST =
    GetServerInfoRequest.newBuilder().build();

  /**
   * Create a new GetServerInfoRequest
   *
   * @return a GetServerInfoRequest
   */
  public static GetServerInfoRequest buildGetServerInfoRequest() {
    return GET_SERVER_INFO_REQUEST;
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

  public static Cell toCell(ExtendedCellBuilder cellBuilder, final CellProtos.Cell cell) {
    return cellBuilder.clear()
            .setRow(cell.getRow().toByteArray())
            .setFamily(cell.getFamily().toByteArray())
            .setQualifier(cell.getQualifier().toByteArray())
            .setTimestamp(cell.getTimestamp())
            .setType((byte) cell.getCellType().getNumber())
            .setValue(cell.getValue().toByteArray())
            .build();
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

  public static TableName toTableName(TableProtos.TableName tableNamePB) {
    return TableName.valueOf(tableNamePB.getNamespace().asReadOnlyByteBuffer(),
        tableNamePB.getQualifier().asReadOnlyByteBuffer());
  }

  public static TableProtos.TableName toProtoTableName(TableName tableName) {
    return TableProtos.TableName.newBuilder()
        .setNamespace(ByteStringer.wrap(tableName.getNamespace()))
        .setQualifier(ByteStringer.wrap(tableName.getQualifier())).build();
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

  /**
   * Creates {@link org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type}
   * from {@link SnapshotType}
   * @param type the SnapshotDescription type
   * @return the protobuf SnapshotDescription type
   */
  public static HBaseProtos.SnapshotDescription.Type
      createProtosSnapShotDescType(SnapshotType type) {
    return HBaseProtos.SnapshotDescription.Type.valueOf(type.name());
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   *
   * @param type the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(
      final RegionSpecifierType type, final byte[] value) {
    RegionSpecifier.Builder regionBuilder = RegionSpecifier.newBuilder();
    regionBuilder.setValue(ByteStringer.wrap(value));
    regionBuilder.setType(type);
    return regionBuilder.build();
  }

  /**
   * Get a ServerName from the passed in data bytes.
   * @param data Data with a serialize server name in it; can handle the old style
   * servername where servername was host and port.  Works too with data that
   * begins w/ the pb 'PBUF' magic and that is then followed by a protobuf that
   * has a serialized {@link ServerName} in it.
   * @return Returns null if <code>data</code> is null else converts passed data
   * to a ServerName instance.
   * @throws DeserializationException
   */
  public static ServerName toServerName(final byte [] data) throws DeserializationException {
    if (data == null || data.length <= 0) return null;
    if (ProtobufMagic.isPBMagicPrefix(data)) {
      int prefixLen = ProtobufMagic.lengthOfPBMagic();
      try {
        ZooKeeperProtos.Master rss =
          ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
        org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName sn =
            rss.getMaster();
        return ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (/*InvalidProtocolBufferException*/IOException e) {
        // A failed parse of the znode is pretty catastrophic. Rather than loop
        // retrying hoping the bad bytes will changes, and rather than change
        // the signature on this method to add an IOE which will send ripples all
        // over the code base, throw a RuntimeException.  This should "never" happen.
        // Fail fast if it does.
        throw new DeserializationException(e);
      }
    }
    // The str returned could be old style -- pre hbase-1502 -- which was
    // hostname and port seperated by a colon rather than hostname, port and
    // startcode delimited by a ','.
    String str = Bytes.toString(data);
    int index = str.indexOf(ServerName.SERVERNAME_SEPARATOR);
    if (index != -1) {
      // Presume its ServerName serialized with versioned bytes.
      return ServerName.parseVersionedServerName(data);
    }
    // Presume it a hostname:port format.
    String hostname = Addressing.parseHostname(str);
    int port = Addressing.parsePort(str);
    return ServerName.valueOf(hostname, port, -1L);
  }

  public static RSGroupInfo toGroupInfo(RSGroupProtos.RSGroupInfo proto) {
    RSGroupInfo RSGroupInfo = new RSGroupInfo(proto.getName());
    for(HBaseProtos.ServerName el: proto.getServersList()) {
      RSGroupInfo.addServer(Address.fromParts(el.getHostName(), el.getPort()));
    }
    for(TableProtos.TableName pTableName: proto.getTablesList()) {
      RSGroupInfo.addTable(ProtobufUtil.toTableName(pTableName));
    }
    return RSGroupInfo;
  }

  public static HBaseProtos.TimeRange toTimeRange(TimeRange timeRange) {
    if (timeRange == null) {
      timeRange = TimeRange.allTime();
    }
    return HBaseProtos.TimeRange.newBuilder().setFrom(timeRange.getMin())
      .setTo(timeRange.getMax())
      .build();
  }

  public static TimeRange toTimeRange(HBaseProtos.TimeRange timeRange) {
    if (timeRange == null) {
      return TimeRange.allTime();
    }
    if (timeRange.hasFrom()) {
      if (timeRange.hasTo()) {
        return TimeRange.between(timeRange.getFrom(), timeRange.getTo());
      } else {
        return TimeRange.from(timeRange.getFrom());
      }
    } else {
      return TimeRange.until(timeRange.getTo());
    }
  }

  public static ClientProtos.Condition toCondition(final byte[] row, final byte[] family,
    final byte[] qualifier, final CompareOperator op, final byte[] value, final Filter filter,
    final TimeRange timeRange) throws IOException {

    ClientProtos.Condition.Builder builder = ClientProtos.Condition.newBuilder()
      .setRow(ByteStringer.wrap(row));

    if (filter != null) {
      builder.setFilter(ProtobufUtil.toFilter(filter));
    } else {
      builder.setFamily(ByteStringer.wrap(family))
        .setQualifier(ByteStringer.wrap(
          qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier))
        .setComparator(
          ProtobufUtil.toComparator(new BinaryComparator(value)))
        .setCompareType(HBaseProtos.CompareType.valueOf(op.name()));
    }

    return builder.setTimeRange(ProtobufUtil.toTimeRange(timeRange)).build();
  }

  public static ClientProtos.Condition toCondition(final byte[] row, final Filter filter,
    final TimeRange timeRange) throws IOException {
    return toCondition(row, null, null, null, null, filter, timeRange);
  }

  public static ClientProtos.Condition toCondition(final byte[] row, final byte[] family,
    final byte[] qualifier, final CompareOperator op, final byte[] value,
    final TimeRange timeRange) throws IOException {
    return toCondition(row, family, qualifier, op, value, null, timeRange);
  }
}
