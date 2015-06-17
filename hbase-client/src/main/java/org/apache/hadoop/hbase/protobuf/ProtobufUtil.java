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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
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
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
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
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
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
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * Protobufs utility.
 */
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class
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
   * Prepend the passed bytes with four bytes of magic, {@link ProtobufMagic#PB_MAGIC},
   * to flag what follows as a protobuf in hbase.  Prepend these bytes to all content written to
   * znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</code> with magic prepended (Creates a new
   * byte array that is <code>bytes.length</code> plus {@link ProtobufMagic#PB_MAGIC}.length.
   */
  public static byte [] prependPBMagic(final byte [] bytes) {
    return Bytes.add(ProtobufMagic.PB_MAGIC, bytes);
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
  public static void expectPBMagicPrefix(final byte [] bytes) throws DeserializationException {
    if (!isPBMagicPrefix(bytes)) {
      throw new DeserializationException("Missing pb magic " +
          Bytes.toString(ProtobufMagic.PB_MAGIC) + " prefix");
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
    return ProtobufConverter.toServerName(serverName);
  }

  /**
   * Convert a protocol buffer ServerName to a ServerName
   *
   * @param proto the protocol buffer ServerName to convert
   * @return the converted ServerName
   */
  public static ServerName toServerName(final HBaseProtos.ServerName proto) {
    return ProtobufConverter.toServerName(proto);
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
    return ProtobufConverter.toDurability(proto);
  }

  /**
   * Convert a client Durability into a protobuf Durability
   */
  public static ClientProtos.MutationProto.Durability toDurability(
      final Durability d) {
    return ProtobufConverter.toDurability(d);
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
    return ProtobufConverter.toGet(proto);
  }

  public static Consistency toConsistency(ClientProtos.Consistency consistency) {
    return ProtobufConverter.toConsistency(consistency);
  }

  public static ClientProtos.Consistency toConsistency(Consistency consistency) {
    return ProtobufConverter.toConsistency(consistency);
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
    return ProtobufConverter.toPut(proto, null);
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
    return ProtobufConverter.toPut(proto, cellScanner);
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
    return ProtobufConverter.toDelete(proto, null);
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
    return ProtobufConverter.toDelete(proto, cellScanner);
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
    return ProtobufConverter.toAppend(proto, cellScanner);
  }

  /**
   * Convert a MutateRequest to Mutation
   *
   * @param proto the protocol buffer Mutate to convert
   * @return the converted Mutation
   * @throws IOException
   */
  public static Mutation toMutation(final MutationProto proto) throws IOException {
    return ProtobufConverter.toMutation(proto);
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
    return ProtobufConverter.toIncrement(proto, cellScanner);
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
    return ProtobufConverter.toScan(scan);
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
    return ProtobufConverter.toScan(proto);
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
    return ProtobufConverter.toGet(get);
  }

  /**
   * Convert a client Increment to a protobuf Mutate.
   *
   * @param increment
   * @return the converted mutate
   */
  public static MutationProto toMutation(
    final Increment increment, final MutationProto.Builder builder, long nonce) {
    return ProtobufConverter.toMutation(increment, builder, nonce);
  }

  public static MutationProto toMutation(final MutationType type, final Mutation mutation)
    throws IOException {
    return ProtobufConverter.toMutation(type, mutation, HConstants.NO_NONCE);
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

  @SuppressWarnings("deprecation")
  public static MutationProto toMutation(final MutationType type, final Mutation mutation,
      MutationProto.Builder builder, long nonce)
  throws IOException {
    return ProtobufConverter.toMutation(type, mutation, builder, nonce);
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
    return ProtobufConverter.toMutationNoData(type, mutation, builder,
            HConstants.NO_NONCE);
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
    return ProtobufConverter.toMutationNoData(type, mutation, builder, nonce);
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final Result result) {
    return ProtobufConverter.toResult(result);
  }

  /**
   * Convert a client Result to a protocol buffer Result
   *
   * @param existence the client existence to send
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final boolean existence, boolean stale) {
    return ProtobufConverter.toResult(existence, stale);
  }

  /**
   * Convert a client Result to a protocol buffer Result.
   * The pb Result does not include the Cell data.  That is for transport otherwise.
   *
   * @param result the client Result to convert
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResultNoData(final Result result) {
    return ProtobufConverter.toResultNoData(result);
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @return the converted client Result
   */
  public static Result toResult(final ClientProtos.Result proto) {
    return ProtobufConverter.toResult(proto);
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
    return ProtobufConverter.toResult(proto, scanner);
  }

  /**
   * Convert a ByteArrayComparable to a protocol buffer Comparator
   *
   * @param comparator the ByteArrayComparable to convert
   * @return the converted protocol buffer Comparator
   */
  public static ComparatorProtos.Comparator toComparator(ByteArrayComparable comparator) {
    return ProtobufConverter.toComparator(comparator);
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
    return ProtobufConverter.toComparator(proto);
  }

  /**
   * Convert a protocol buffer Filter to a client Filter
   *
   * @param proto the protocol buffer Filter to convert
   * @return the converted Filter
   */
  @SuppressWarnings("unchecked")
  public static Filter toFilter(FilterProtos.Filter proto) throws IOException {
    return ProtobufConverter.toFilter(proto);
  }

  /**
   * Convert a client Filter to a protocol buffer Filter
   *
   * @param filter the Filter to convert
   * @return the converted protocol buffer Filter
   */
  public static FilterProtos.Filter toFilter(Filter filter) throws IOException {
    return ProtobufConverter.toFilter(filter);
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
    return ProtobufConverter.toDeleteType(type);
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
    return ProtobufConverter.fromDeleteType(type);
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
    return ProtobufConverter.toException(parameter);
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
   * @throws IOException
   */
  public static void closeRegion(final AdminService.BlockingInterface admin,
      final ServerName server, final byte[] regionName) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(server, regionName);
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
   * @return true if the region is closed
   * @throws IOException
   */
  public static boolean closeRegion(final AdminService.BlockingInterface admin,
      final ServerName server, final byte[] regionName,
      final ServerName destinationServer) throws IOException {
    CloseRegionRequest closeRegionRequest =
      RequestConverter.buildCloseRegionRequest(server,
        regionName, destinationServer);
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
      RequestConverter.buildOpenRegionRequest(server, region, null, null);
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
   * @throws IOException
   */
  public static void mergeRegions(final AdminService.BlockingInterface admin,
      final HRegionInfo region_a, final HRegionInfo region_b,
      final boolean forcible) throws IOException {
    MergeRegionsRequest request = RequestConverter.buildMergeRegionsRequest(
        region_a.getRegionName(), region_b.getRegionName(),forcible);
    try {
      admin.mergeRegions(null, request);
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
    baos.write(ProtobufMagic.PB_MAGIC);
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
    return ProtobufConverter.toPermission(proto);
  }

  /**
   * Converts a Permission proto to a client TablePermission object.
   *
   * @param proto the protobuf Permission
   * @return the converted TablePermission
   */
  public static TablePermission toTablePermission(AccessControlProtos.Permission proto) {
    return ProtobufConverter.toTablePermission(proto);
  }

  /**
   * Convert a client Permission to a Permission proto
   *
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    return ProtobufConverter.toPermission(perm);
  }

  /**
   * Converts a list of Permission.Action proto to a list of client Permission.Action objects.
   *
   * @param protoActions the list of protobuf Actions
   * @return the converted list of Actions
   */
  public static List<Permission.Action> toPermissionActions(
      List<AccessControlProtos.Permission.Action> protoActions) {
    return ProtobufConverter.toPermissionActions(protoActions);
  }

  /**
   * Converts a Permission.Action proto to a client Permission.Action object.
   *
   * @param action the protobuf Action
   * @return the converted Action
   */
  public static Permission.Action toPermissionAction(
      AccessControlProtos.Permission.Action action) {
    return ProtobufConverter.toPermissionAction(action);
  }

  /**
   * Convert a client Permission.Action to a Permission.Action proto
   *
   * @param action the client Action
   * @return the protobuf Action
   */
  public static AccessControlProtos.Permission.Action toPermissionAction(
      Permission.Action action) {
    return ProtobufConverter.toPermissionAction(action);
  }

  /**
   * Convert a client user permission to a user permission proto
   *
   * @param perm the client UserPermission
   * @return the protobuf UserPermission
   */
  public static AccessControlProtos.UserPermission toUserPermission(UserPermission perm) {
    return ProtobufConverter.toUserPermission(perm);
  }

  /**
   * Converts a user permission proto to a client user permission object.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(AccessControlProtos.UserPermission proto) {
    return ProtobufConverter.toUserPermission(proto);
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
    return ProtobufConverter.toUserTablePermissions(perm);
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
    return ProtobufConverter.toUserTablePermissions(proto);
  }

  /**
   * Converts a Token instance (with embedded identifier) to the protobuf representation.
   *
   * @param token the Token instance to copy
   * @return the protobuf Token message
   */
  public static AuthenticationProtos.Token toToken(Token<AuthenticationTokenIdentifier> token) {
    return ProtobufConverter.toToken(token);
  }

  /**
   * Converts a protobuf Token message back into a Token instance.
   *
   * @param proto the protobuf Token message
   * @return the Token instance
   */
  public static Token<AuthenticationTokenIdentifier> toToken(AuthenticationProtos.Token proto) {
    return ProtobufConverter.toToken(proto);
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
    byte[] value = regionSpecifier.getValue().toByteArray();
    RegionSpecifierType type = regionSpecifier.getType();
    switch (type) {
      case REGION_NAME:
        return HRegionInfo.encodeRegionName(value);
      case ENCODED_REGION_NAME:
        return Bytes.toString(value);
      default:
        throw new DoNotRetryIOException(
          "Unsupported region specifier type: " + type);
    }
  }

  public static ScanMetrics toScanMetrics(final byte[] bytes) {
    return ProtobufConverter.toScanMetrics(bytes);
  }

  public static MapReduceProtos.ScanMetrics toScanMetrics(ScanMetrics scanMetrics) {
    return ProtobufConverter.toScanMetrics(scanMetrics);
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
    return ProtobufConverter.toCell(kv);
  }

  public static Cell toCell(final CellProtos.Cell cell) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    return ProtobufConverter.toCell(cell);
  }

  public static HBaseProtos.NamespaceDescriptor toProtoNamespaceDescriptor(NamespaceDescriptor ns) {
    return ProtobufConverter.toProtoNamespaceDescriptor(ns);
  }

  public static NamespaceDescriptor toNamespaceDescriptor(
      HBaseProtos.NamespaceDescriptor desc) throws IOException {
    return ProtobufConverter.toNamespaceDescriptor(desc);
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

  @SuppressWarnings("deprecation")
  public static CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] family,
      List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    // compaction descriptor contains relative paths.
    // input / output paths are relative to the store dir
    // store dir is relative to region dir
    return ProtobufConverter.toCompactionDescriptor(info, family, inputPaths,
            outputPaths, storeDir);
  }

  public static FlushDescriptor toFlushDescriptor(FlushAction action, HRegionInfo hri,
      long flushSeqId, Map<byte[], List<Path>> committedFiles) {
    return ProtobufConverter.toFlushDescriptor(action, hri, flushSeqId, committedFiles);
  }

  public static RegionEventDescriptor toRegionEventDescriptor(
      EventType eventType, HRegionInfo hri, long seqId, ServerName server,
      Map<byte[], List<Path>> storeFiles) {
    return ProtobufConverter.toRegionEventDescriptor(eventType, hri, seqId,
            server, storeFiles);
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
      return ProtobufConverter.toShortString((MutationProto) m);
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

  public static TableName toTableName(HBaseProtos.TableName tableNamePB) {
    return ProtobufConverter.toTableName(tableNamePB);
  }

  public static HBaseProtos.TableName toProtoTableName(TableName tableName) {
    return ProtobufConverter.toProtoTableName(tableName);
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
    return ProtobufConverter.toCellVisibility(proto);
  }

  /**
   * Convert a protocol buffer CellVisibility bytes to a client CellVisibility
   *
   * @param protoBytes
   * @return the converted client CellVisibility
   * @throws DeserializationException
   */
  public static CellVisibility toCellVisibility(byte[] protoBytes) throws DeserializationException {
    return ProtobufConverter.toCellVisibility(protoBytes);
  }

  /**
   * Create a protocol buffer CellVisibility based on a client CellVisibility.
   *
   * @param cellVisibility
   * @return a protocol buffer CellVisibility
   */
  public static ClientProtos.CellVisibility toCellVisibility(CellVisibility cellVisibility) {
    return ProtobufConverter.toCellVisibility(cellVisibility);
  }

  /**
   * Convert a protocol buffer Authorizations to a client Authorizations
   *
   * @param proto
   * @return the converted client Authorizations
   */
  public static Authorizations toAuthorizations(ClientProtos.Authorizations proto) {
    return ProtobufConverter.toAuthorizations(proto);
  }

  /**
   * Convert a protocol buffer Authorizations bytes to a client Authorizations
   *
   * @param protoBytes
   * @return the converted client Authorizations
   * @throws DeserializationException
   */
  public static Authorizations toAuthorizations(byte[] protoBytes) throws DeserializationException {
    return ProtobufConverter.toAuthorizations(protoBytes);
  }

  /**
   * Create a protocol buffer Authorizations based on a client Authorizations.
   *
   * @param authorizations
   * @return a protocol buffer Authorizations
   */
  public static ClientProtos.Authorizations toAuthorizations(Authorizations authorizations) {
    return ProtobufConverter.toAuthorizations(authorizations);
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(String user,
      Permission perms) {
    return ProtobufConverter.toUsersAndPermissions(user, perms);
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(
      ListMultimap<String, Permission> perms) {
    return ProtobufConverter.toUsersAndPermissions(perms);
  }

  public static ListMultimap<String, Permission> toUsersAndPermissions(
      AccessControlProtos.UsersAndPermissions proto) {
    return ProtobufConverter.toUsersAndPermissions(proto);
  }

  /**
   * Convert a protocol buffer TimeUnit to a client TimeUnit
   *
   * @param proto
   * @return the converted client TimeUnit
   */
  public static TimeUnit toTimeUnit(final HBaseProtos.TimeUnit proto) {
    return ProtobufConverter.toTimeUnit(proto);
  }

  /**
   * Convert a client TimeUnit to a protocol buffer TimeUnit
   *
   * @param timeUnit
   * @return the converted protocol buffer TimeUnit
   */
  public static HBaseProtos.TimeUnit toProtoTimeUnit(final TimeUnit timeUnit) {
    return ProtobufConverter.toProtoTimeUnit(timeUnit);
  }

  /**
   * Convert a protocol buffer ThrottleType to a client ThrottleType
   *
   * @param proto
   * @return the converted client ThrottleType
   */
  public static ThrottleType toThrottleType(final QuotaProtos.ThrottleType proto) {
    return ProtobufConverter.toThrottleType(proto);
  }

  /**
   * Convert a client ThrottleType to a protocol buffer ThrottleType
   *
   * @param type
   * @return the converted protocol buffer ThrottleType
   */
  public static QuotaProtos.ThrottleType toProtoThrottleType(final ThrottleType type) {
    return ProtobufConverter.toProtoThrottleType(type);
  }

  /**
   * Convert a protocol buffer QuotaScope to a client QuotaScope
   *
   * @param proto
   * @return the converted client QuotaScope
   */
  public static QuotaScope toQuotaScope(final QuotaProtos.QuotaScope proto) {
    return ProtobufConverter.toQuotaScope(proto);
  }

  /**
   * Convert a client QuotaScope to a protocol buffer QuotaScope
   *
   * @param scope
   * @return the converted protocol buffer QuotaScope
   */
  public static QuotaProtos.QuotaScope toProtoQuotaScope(final QuotaScope scope) {
    return ProtobufConverter.toProtoQuotaScope(scope);
  }

  /**
   * Convert a protocol buffer QuotaType to a client QuotaType
   *
   * @param proto
   * @return the converted client QuotaType
   */
  public static QuotaType toQuotaScope(final QuotaProtos.QuotaType proto) {
    return ProtobufConverter.toQuotaScope(proto);
  }

  /**
   * Convert a client QuotaType to a protocol buffer QuotaType
   *
   * @param type
   * @return the converted protocol buffer QuotaType
   */
  public static QuotaProtos.QuotaType toProtoQuotaScope(final QuotaType type) {
    return ProtobufConverter.toProtoQuotaScope(type);
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
    return ProtobufConverter.toTimedQuota(limit, timeUnit, scope);
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
    return ProtobufConverter.toBulkLoadDescriptor(tableName, encodedRegionName,
            storeFiles, bulkloadSeqId);
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
  public static RPCProtos.VersionInfo getVersionInfo() {
    RPCProtos.VersionInfo.Builder builder = RPCProtos.VersionInfo.newBuilder();
    builder.setVersion(VersionInfo.getVersion());
    builder.setUrl(VersionInfo.getUrl());
    builder.setRevision(VersionInfo.getRevision());
    builder.setUser(VersionInfo.getUser());
    builder.setDate(VersionInfo.getDate());
    builder.setSrcChecksum(VersionInfo.getSrcChecksum());
    return builder.build();
  }
}
