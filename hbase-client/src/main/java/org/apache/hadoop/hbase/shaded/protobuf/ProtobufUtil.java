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
package org.apache.hadoop.hbase.shaded.protobuf;

import static org.apache.hadoop.hbase.protobuf.ProtobufMagic.PB_MAGIC;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsBuilder;
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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ServerTask;
import org.apache.hadoop.hbase.ServerTaskBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Cursor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLoadStats;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SlowLogParams;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.protobuf.ProtobufMessageConverter;
import org.apache.hadoop.hbase.quotas.QuotaScope;
import org.apache.hadoop.hbase.quotas.QuotaType;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionExceptionMessage;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HFileProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Protobufs utility.
 * Be aware that a class named org.apache.hadoop.hbase.protobuf.ProtobufUtil (i.e. no 'shaded' in
 * the package name) carries a COPY of a subset of this class for non-shaded
 * users; e.g. Coprocessor Endpoints. If you make change in here, be sure to make change in
 * the companion class too (not the end of the world, especially if you are adding new functionality
 * but something to be aware of.
 */
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class
public final class ProtobufUtil {

  private ProtobufUtil() {
  }

  /**
   * Many results are simple: no cell, exists true or false. To save on object creations,
   *  we reuse them across calls.
   */
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

  private static volatile boolean classLoaderLoaded = false;

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
      classLoaderLoaded = true;
    }
  }

  public static boolean isClassLoaderLoaded() {
    return classLoaderLoaded;
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
          "Missing pb magic " + Bytes.toString(PB_MAGIC) + " prefix" + ", bytes: " + bytesPrefix);
    }
  }

  /**
   * @return Length of {@link ProtobufMagic#lengthOfPBMagic()}
   */
  public static int lengthOfPBMagic() {
    return ProtobufMagic.lengthOfPBMagic();
  }

  public static ComparatorProtos.ByteArrayComparable toByteArrayComparable(final byte [] value) {
    ComparatorProtos.ByteArrayComparable.Builder builder =
      ComparatorProtos.ByteArrayComparable.newBuilder();
    if (value != null) builder.setValue(UnsafeByteOperations.unsafeWrap(value));
    return builder.build();
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
   * Like {@link #getRemoteException(ServiceException)} but more generic, able to handle more than
   * just {@link ServiceException}. Prefer this method to
   * {@link #getRemoteException(ServiceException)} because trying to
   * contain direct protobuf references.
   */
  public static IOException handleRemoteException(Throwable e) {
    return makeIOExceptionOfException(e);
  }

  private static IOException makeIOExceptionOfException(Throwable e) {
    Throwable t = e;
    if (e instanceof ServiceException) {
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
   * @see #toServerName(org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName)
   */
  public static HBaseProtos.ServerName toServerName(final ServerName serverName) {
    if (serverName == null) {
      return null;
    }
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
   * Get a ServerName from the passed in data bytes.
   * @param data Data with a serialize server name in it; can handle the old style servername where
   *          servername was host and port. Works too with data that begins w/ the pb 'PBUF' magic
   *          and that is then followed by a protobuf that has a serialized {@link ServerName} in
   *          it.
   * @return Returns null if <code>data</code> is null else converts passed data to a ServerName
   *         instance.
   */
  public static ServerName toServerName(final byte[] data) throws DeserializationException {
    if (data == null || data.length <= 0) {
      return null;
    }
    if (ProtobufMagic.isPBMagicPrefix(data)) {
      int prefixLen = ProtobufMagic.lengthOfPBMagic();
      try {
        ZooKeeperProtos.Master rss =
          ZooKeeperProtos.Master.parser().parseFrom(data, prefixLen, data.length - prefixLen);
        HBaseProtos.ServerName sn = rss.getMaster();
        return ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (/* InvalidProtocolBufferException */IOException e) {
        // A failed parse of the znode is pretty catastrophic. Rather than loop
        // retrying hoping the bad bytes will changes, and rather than change
        // the signature on this method to add an IOE which will send ripples all
        // over the code base, throw a RuntimeException. This should "never" happen.
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

  /**
   * Convert a list of protocol buffer ServerName to a list of ServerName
   * @param proto protocol buffer ServerNameList
   * @return a list of ServerName
   */
  public static List<ServerName> toServerNameList(
          List<HBaseProtos.ServerName> proto) {
    return proto.stream().map(ProtobufUtil::toServerName)
            .collect(Collectors.toList());
  }

  /**
   * Get a list of NamespaceDescriptor from ListNamespaceDescriptorsResponse protobuf
   * @param proto the ListNamespaceDescriptorsResponse
   * @return a list of NamespaceDescriptor
   */
  public static List<NamespaceDescriptor> toNamespaceDescriptorList(
      ListNamespaceDescriptorsResponse proto) {
    return proto.getNamespaceDescriptorList().stream().map(ProtobufUtil::toNamespaceDescriptor)
        .collect(Collectors.toList());
  }

  /**
   * Get a list of TableDescriptor from GetTableDescriptorsResponse protobuf
   * @param proto the GetTableDescriptorsResponse
   * @return a list of TableDescriptor
   */
  public static List<TableDescriptor> toTableDescriptorList(GetTableDescriptorsResponse proto) {
    if (proto == null) {
      return new ArrayList<>();
    }
    return proto.getTableSchemaList().stream().map(ProtobufUtil::toTableDescriptor)
        .collect(Collectors.toList());
  }

  /**
   * Get a list of TableDescriptor from ListTableDescriptorsByNamespaceResponse protobuf
   * @param proto the ListTableDescriptorsByNamespaceResponse
   * @return a list of TableDescriptor
   */
  public static List<TableDescriptor>
      toTableDescriptorList(ListTableDescriptorsByNamespaceResponse proto) {
    if (proto == null) return new ArrayList<>();
    return proto.getTableSchemaList().stream().map(ProtobufUtil::toTableDescriptor)
        .collect(Collectors.toList());
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
  public static Get toGet(final ClientProtos.Get proto) throws IOException {
    if (proto == null) return null;
    byte[] row = proto.getRow().toByteArray();
    Get get = new Get(row);
    if (proto.hasCacheBlocks()) {
      get.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.hasMaxVersions()) {
      get.readVersions(proto.getMaxVersions());
    }
    if (proto.hasStoreLimit()) {
      get.setMaxResultsPerColumnFamily(proto.getStoreLimit());
    }
    if (proto.hasStoreOffset()) {
      get.setRowOffsetPerColumnFamily(proto.getStoreOffset());
    }
    if (proto.getCfTimeRangeCount() > 0) {
      for (HBaseProtos.ColumnFamilyTimeRange cftr : proto.getCfTimeRangeList()) {
        TimeRange timeRange = toTimeRange(cftr.getTimeRange());
        get.setColumnFamilyTimeRange(cftr.getColumnFamily().toByteArray(),
            timeRange.getMin(), timeRange.getMax());
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = toTimeRange(proto.getTimeRange());
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
                  .setRow(proto.getRow().toByteArray())
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
          consumer.accept(mutation, ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                  .setRow(mutation.getRow())
                  .setFamily(family)
                  .setQualifier(qualifier)
                  .setTimestamp(cellTimestampOrLatest(qv))
                  .setType(KeyValue.Type.Put.getCode())
                  .setValue(value)
                  .setTags(tags)
                  .build());
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
      TimeRange timeRange = toTimeRange(proto.getTimeRange());
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
      TimeRange timeRange = toTimeRange(proto.getTimeRange());
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
        .setColumnFamily(UnsafeByteOperations.unsafeWrap(cf))
        .setTimeRange(toTimeRange(timeRange))
        .build());
    });
    scanBuilder.setTimeRange(ProtobufUtil.toTimeRange(scan.getTimeRange()));
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(UnsafeByteOperations.unsafeWrap(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(UnsafeByteOperations.unsafeWrap(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(UnsafeByteOperations.unsafeWrap(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(ProtobufUtil.toFilter(scan.getFilter()));
    }
    if (scan.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
      for (Map.Entry<byte[],NavigableSet<byte []>>
          family: scan.getFamilyMap().entrySet()) {
        columnBuilder.setFamily(UnsafeByteOperations.unsafeWrap(family.getKey()));
        NavigableSet<byte []> qualifiers = family.getValue();
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte [] qualifier: qualifiers) {
            columnBuilder.addQualifier(UnsafeByteOperations.unsafeWrap(qualifier));
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
    if (scan.isNeedCursorResult()) {
      scanBuilder.setNeedCursorResult(true);
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
    } else {
      // old client without this flag, we should consider start=end as a get.
      if (ClientUtil.areScanStartRowAndStopRowEqual(startRow, stopRow)) {
        includeStopRow = true;
      }
    }
    Scan scan =
        new Scan().withStartRow(startRow, includeStartRow).withStopRow(stopRow, includeStopRow);
    if (proto.hasCacheBlocks()) {
      scan.setCacheBlocks(proto.getCacheBlocks());
    }
    if (proto.hasMaxVersions()) {
      scan.readVersions(proto.getMaxVersions());
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
        TimeRange timeRange = toTimeRange(cftr.getTimeRange());
        scan.setColumnFamilyTimeRange(cftr.getColumnFamily().toByteArray(),
            timeRange.getMin(), timeRange.getMax());
      }
    }
    if (proto.hasTimeRange()) {
      TimeRange timeRange = toTimeRange(proto.getTimeRange());
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
    if (proto.hasReadType()) {
      scan.setReadType(toReadType(proto.getReadType()));
    }
    if (proto.getNeedCursorResult()) {
      scan.setNeedCursorResult(true);
    }
    return scan;
  }

  public static ClientProtos.Cursor toCursor(Cursor cursor) {
    ClientProtos.Cursor.Builder builder = ClientProtos.Cursor.newBuilder();
    ClientProtos.Cursor.newBuilder().setRow(ByteString.copyFrom(cursor.getRow()));
    return builder.build();
  }

  public static ClientProtos.Cursor toCursor(Cell cell) {
    return ClientProtos.Cursor.newBuilder()
        .setRow(ByteString.copyFrom(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
        .build();
  }

  public static Cursor toCursor(ClientProtos.Cursor cursor) {
    return ClientUtil.createCursor(cursor.getRow().toByteArray());
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
    builder.setRow(UnsafeByteOperations.unsafeWrap(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getFilter() != null) {
      builder.setFilter(ProtobufUtil.toFilter(get.getFilter()));
    }
    get.getColumnFamilyTimeRange().forEach((cf, timeRange) -> {
      builder.addCfTimeRange(HBaseProtos.ColumnFamilyTimeRange.newBuilder()
        .setColumnFamily(UnsafeByteOperations.unsafeWrap(cf))
        .setTimeRange(toTimeRange(timeRange))
        .build());
    });
    builder.setTimeRange(ProtobufUtil.toTimeRange(get.getTimeRange()));
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(UnsafeByteOperations.unsafeWrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family: families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(UnsafeByteOperations.unsafeWrap(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier: qualifiers) {
            columnBuilder.addQualifier(UnsafeByteOperations.unsafeWrap(qualifier));
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
      builder.setTimeRange(ProtobufUtil.toTimeRange(((Increment) mutation).getTimeRange()));
    }
    if (type == MutationType.APPEND) {
      builder.setTimeRange(ProtobufUtil.toTimeRange(((Append) mutation).getTimeRange()));
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[],List<Cell>> family: mutation.getFamilyCellMap().entrySet()) {
      columnBuilder.clear();
      columnBuilder.setFamily(UnsafeByteOperations.unsafeWrap(family.getKey()));
      for (Cell cell: family.getValue()) {
        valueBuilder.clear();
        valueBuilder.setQualifier(UnsafeByteOperations.unsafeWrap(
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
        valueBuilder.setValue(UnsafeByteOperations.unsafeWrap(
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
      builder.setTimeRange(ProtobufUtil.toTimeRange(((Increment) mutation).getTimeRange()));
    }
    if (mutation instanceof Append) {
      builder.setTimeRange(ProtobufUtil.toTimeRange(((Append) mutation).getTimeRange()));
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
    builder.setRow(UnsafeByteOperations.unsafeWrap(mutation.getRow()));
    builder.setMutateType(type);
    builder.setDurability(toDurability(mutation.getDurability()));
    builder.setTimestamp(mutation.getTimestamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(UnsafeByteOperations.unsafeWrap(attribute.getValue()));
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
    return toResult(result, false);
  }

  /**
   *  Convert a client Result to a protocol buffer Result
   * @param result the client Result to convert
   * @param encodeTags whether to includeTags in converted protobuf result or not
   *                   When @encodeTags is set to true, it will return all the tags in the response.
   *                   These tags may contain some sensitive data like acl permissions, etc.
   *                   Only the tools like Export, Import which needs to take backup needs to set
   *                   it to true so that cell tags are persisted in backup.
   *                   Refer to HBASE-25246 for more context.
   * @return the converted protocol buffer Result
   */
  public static ClientProtos.Result toResult(final Result result, boolean encodeTags) {
    if (result.getExists() != null) {
      return toResult(result.getExists(), result.isStale());
    }

    Cell[] cells = result.rawCells();
    if (cells == null || cells.length == 0) {
      return result.isStale() ? EMPTY_RESULT_PB_STALE : EMPTY_RESULT_PB;
    }

    ClientProtos.Result.Builder builder = ClientProtos.Result.newBuilder();
    for (Cell c : cells) {
      builder.addCell(toCell(c, encodeTags));
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
    return toResult(proto, false);
  }

  /**
   * Convert a protocol buffer Result to a client Result
   *
   * @param proto the protocol buffer Result to convert
   * @param decodeTags whether to decode tags into converted client Result
   *                   When @decodeTags is set to true, it will decode all the tags from the
   *                   response. These tags may contain some sensitive data like acl permissions,
   *                   etc. Only the tools like Export, Import which needs to take backup needs to
   *                   set it to true so that cell tags are persisted in backup.
   *                   Refer to HBASE-25246 for more context.
   * @return the converted client Result
   */
  public static Result toResult(final ClientProtos.Result proto, boolean decodeTags) {
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
      cells.add(toCell(builder, c, decodeTags));
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
        cells.add(toCell(builder, c, false));
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
    builder.setSerializedComparator(UnsafeByteOperations.unsafeWrap(comparator.toByteArray()));
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
    builder.setSerializedFilter(UnsafeByteOperations.unsafeWrap(filter.toByteArray()));
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
   * A helper to retrieve region info given a region name or an
   * encoded region name using admin protocol.
   *
   * @return the retrieved region info
   */
  public static org.apache.hadoop.hbase.client.RegionInfo getRegionInfo(
      final RpcController controller, final AdminService.BlockingInterface admin,
      final byte[] regionName) throws IOException {
    try {
      GetRegionInfoRequest request = getGetRegionInfoRequest(regionName);
      GetRegionInfoResponse response = admin.getRegionInfo(controller,
        getGetRegionInfoRequest(regionName));
      return toRegionInfo(response.getRegionInfo());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * @return A GetRegionInfoRequest for the passed in regionName.
   */
  public static GetRegionInfoRequest getGetRegionInfoRequest(final byte [] regionName)
    throws IOException {
    return org.apache.hadoop.hbase.client.RegionInfo.isEncodedRegionName(regionName)?
        GetRegionInfoRequest.newBuilder().setRegion(RequestConverter.
          buildRegionSpecifier(RegionSpecifierType.ENCODED_REGION_NAME, regionName)).build():
        RequestConverter.buildGetRegionInfoRequest(regionName);
  }

  /**
   * A helper to close a region given a region name
   * using admin protocol.
   *
   * @param admin
   * @param regionName
   * @throws IOException
   */
  public static void closeRegion(final RpcController controller,
      final AdminService.BlockingInterface admin, final ServerName server, final byte[] regionName)
          throws IOException {
    CloseRegionRequest closeRegionRequest =
      ProtobufUtil.buildCloseRegionRequest(server, regionName);
    try {
      admin.closeRegion(controller, closeRegionRequest);
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
  public static void warmupRegion(final RpcController controller,
      final AdminService.BlockingInterface admin, final org.apache.hadoop.hbase.client.RegionInfo regionInfo) throws IOException {

    try {
      WarmupRegionRequest warmupRegionRequest =
           RequestConverter.buildWarmupRegionRequest(regionInfo);

      admin.warmupRegion(controller, warmupRegionRequest);
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
  public static void openRegion(final RpcController controller,
      final AdminService.BlockingInterface admin, ServerName server, final org.apache.hadoop.hbase.client.RegionInfo region)
          throws IOException {
    OpenRegionRequest request =
      RequestConverter.buildOpenRegionRequest(server, region, null);
    try {
      admin.openRegion(controller, request);
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
  public static List<org.apache.hadoop.hbase.client.RegionInfo> getOnlineRegions(final AdminService.BlockingInterface admin)
      throws IOException {
    return getOnlineRegions(null, admin);
  }

  /**
   * A helper to get the all the online regions on a region
   * server using admin protocol.
   * @return a list of online region info
   */
  public static List<org.apache.hadoop.hbase.client.RegionInfo> getOnlineRegions(final RpcController controller,
      final AdminService.BlockingInterface admin)
  throws IOException {
    GetOnlineRegionRequest request = RequestConverter.buildGetOnlineRegionRequest();
    GetOnlineRegionResponse response = null;
    try {
      response = admin.getOnlineRegion(controller, request);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
    return getRegionInfos(response);
  }

  /**
   * Get the list of region info from a GetOnlineRegionResponse
   *
   * @param proto the GetOnlineRegionResponse
   * @return the list of region info or empty if <code>proto</code> is null
   */
  public static List<org.apache.hadoop.hbase.client.RegionInfo> getRegionInfos(final GetOnlineRegionResponse proto) {
    if (proto == null) return Collections.EMPTY_LIST;
    List<org.apache.hadoop.hbase.client.RegionInfo> regionInfos = new ArrayList<>(proto.getRegionInfoList().size());
    for (RegionInfo regionInfo: proto.getRegionInfoList()) {
      regionInfos.add(toRegionInfo(regionInfo));
    }
    return regionInfos;
  }

  /**
   * A helper to get the info of a region server using admin protocol.
   * @return the server name
   */
  public static ServerInfo getServerInfo(final RpcController controller,
      final AdminService.BlockingInterface admin)
  throws IOException {
    GetServerInfoRequest request = RequestConverter.buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(controller, request);
      return response.getServerInfo();
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to get the list of files of a column family
   * on a given region using admin protocol.
   *
   * @return the list of store files
   */
  public static List<String> getStoreFiles(final AdminService.BlockingInterface admin,
      final byte[] regionName, final byte[] family)
  throws IOException {
    return getStoreFiles(null, admin, regionName, family);
  }

  /**
   * A helper to get the list of files of a column family
   * on a given region using admin protocol.
   *
   * @return the list of store files
   */
  public static List<String> getStoreFiles(final RpcController controller,
      final AdminService.BlockingInterface admin, final byte[] regionName, final byte[] family)
  throws IOException {
    GetStoreFileRequest request =
      ProtobufUtil.buildGetStoreFileRequest(regionName, family);
    try {
      GetStoreFileResponse response = admin.getStoreFile(controller, request);
      return response.getStoreFileList();
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
    baos.write(PB_MAGIC);
    m.writeDelimitedTo(baos);
    return baos.toByteArray();
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
        return org.apache.hadoop.hbase.client.RegionInfo.encodeRegionName(value.toByteArray());
      case ENCODED_REGION_NAME:
        return value.toStringUtf8();
      default:
        throw new DoNotRetryIOException(
          "Unsupported region specifier type: " + type);
    }
  }

  public static ScanMetrics toScanMetrics(final byte[] bytes) {
    MapReduceProtos.ScanMetrics pScanMetrics = null;
    try {
      pScanMetrics = MapReduceProtos.ScanMetrics.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      // Ignored there are just no key values to add.
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

  public static MapReduceProtos.ScanMetrics toScanMetrics(ScanMetrics scanMetrics, boolean reset) {
    MapReduceProtos.ScanMetrics.Builder builder = MapReduceProtos.ScanMetrics.newBuilder();
    Map<String, Long> metrics = scanMetrics.getMetricsMap(reset);
    for (Entry<String, Long> e : metrics.entrySet()) {
      HBaseProtos.NameInt64Pair nameInt64Pair =
          HBaseProtos.NameInt64Pair.newBuilder().setName(e.getKey()).setValue(e.getValue()).build();
      builder.addMetrics(nameInt64Pair);
    }
    return builder.build();
  }

  /**
   * Unwraps an exception from a protobuf service into the underlying (expected) IOException. This
   * method will <strong>always</strong> throw an exception.
   * @param se the {@code ServiceException} instance to convert into an {@code IOException}
   * @throws NullPointerException if {@code se} is {@code null}
   */
  public static void toIOException(ServiceException se) throws IOException {
    Objects.requireNonNull(se, "Service exception cannot be null");

    Throwable cause = se.getCause();
    if (cause != null && cause instanceof IOException) {
      throw (IOException)cause;
    }
    throw new IOException(se);
  }

  public static CellProtos.Cell toCell(final Cell kv, boolean encodeTags) {
    // Doing this is going to kill us if we do it for all data passed.
    // St.Ack 20121205
    CellProtos.Cell.Builder kvbuilder = CellProtos.Cell.newBuilder();
    if (kv instanceof ByteBufferExtendedCell) {
      kvbuilder.setRow(wrap(((ByteBufferExtendedCell) kv).getRowByteBuffer(),
        ((ByteBufferExtendedCell) kv).getRowPosition(), kv.getRowLength()));
      kvbuilder.setFamily(wrap(((ByteBufferExtendedCell) kv).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) kv).getFamilyPosition(), kv.getFamilyLength()));
      kvbuilder.setQualifier(wrap(((ByteBufferExtendedCell) kv).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) kv).getQualifierPosition(), kv.getQualifierLength()));
      kvbuilder.setCellType(CellProtos.CellType.valueOf(kv.getTypeByte()));
      kvbuilder.setTimestamp(kv.getTimestamp());
      kvbuilder.setValue(wrap(((ByteBufferExtendedCell) kv).getValueByteBuffer(),
        ((ByteBufferExtendedCell) kv).getValuePosition(), kv.getValueLength()));
      if (encodeTags) {
        kvbuilder.setTags(wrap(((ByteBufferExtendedCell) kv).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) kv).getTagsPosition(), kv.getTagsLength()));
      }
    } else {
      kvbuilder.setRow(
        UnsafeByteOperations.unsafeWrap(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
      kvbuilder.setFamily(UnsafeByteOperations.unsafeWrap(kv.getFamilyArray(), kv.getFamilyOffset(),
        kv.getFamilyLength()));
      kvbuilder.setQualifier(UnsafeByteOperations.unsafeWrap(kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength()));
      kvbuilder.setCellType(CellProtos.CellType.valueOf(kv.getTypeByte()));
      kvbuilder.setTimestamp(kv.getTimestamp());
      kvbuilder.setValue(UnsafeByteOperations.unsafeWrap(kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      if (encodeTags) {
        kvbuilder.setTags(UnsafeByteOperations.unsafeWrap(kv.getTagsArray(), kv.getTagsOffset(),
          kv.getTagsLength()));
      }
    }
    return kvbuilder.build();
  }

  private static ByteString wrap(ByteBuffer b, int offset, int length) {
    ByteBuffer dup = b.duplicate();
    dup.position(offset);
    dup.limit(offset + length);
    return UnsafeByteOperations.unsafeWrap(dup);
  }

  public static Cell toCell(ExtendedCellBuilder cellBuilder, final CellProtos.Cell cell,
                            boolean decodeTags) {
    ExtendedCellBuilder builder = cellBuilder.clear()
        .setRow(cell.getRow().toByteArray())
        .setFamily(cell.getFamily().toByteArray())
        .setQualifier(cell.getQualifier().toByteArray())
        .setTimestamp(cell.getTimestamp())
        .setType((byte) cell.getCellType().getNumber())
        .setValue(cell.getValue().toByteArray());
    if (decodeTags && cell.hasTags()) {
      builder.setTags(cell.getTags().toByteArray());
    }
    return builder.build();
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

  public static NamespaceDescriptor toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor desc) {
    NamespaceDescriptor.Builder b = NamespaceDescriptor.create(desc.getName().toStringUtf8());
    for (HBaseProtos.NameStringPair prop : desc.getConfigurationList()) {
      b.addConfiguration(prop.getName(), prop.getValue());
    }
    return b.build();
  }

  public static CompactionDescriptor toCompactionDescriptor(
      org.apache.hadoop.hbase.client.RegionInfo info, byte[] family,
      List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    return toCompactionDescriptor(info, null, family, inputPaths, outputPaths, storeDir);
  }

  public static CompactionDescriptor toCompactionDescriptor(
      org.apache.hadoop.hbase.client.RegionInfo info, byte[] regionName,
      byte[] family, List<Path> inputPaths, List<Path> outputPaths, Path storeDir) {
    // compaction descriptor contains relative paths.
    // input / output paths are relative to the store dir
    // store dir is relative to region dir
    CompactionDescriptor.Builder builder = CompactionDescriptor.newBuilder()
        .setTableName(UnsafeByteOperations.unsafeWrap(info.getTable().toBytes()))
        .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(
          regionName == null ? info.getEncodedNameAsBytes() : regionName))
        .setFamilyName(UnsafeByteOperations.unsafeWrap(family))
        .setStoreHomeDir(storeDir.getName()); //make relative
    for (Path inputPath : inputPaths) {
      builder.addCompactionInput(inputPath.getName()); //relative path
    }
    for (Path outputPath : outputPaths) {
      builder.addCompactionOutput(outputPath.getName());
    }
    builder.setRegionName(UnsafeByteOperations.unsafeWrap(info.getRegionName()));
    return builder.build();
  }

  public static FlushDescriptor toFlushDescriptor(FlushAction action, org.apache.hadoop.hbase.client.RegionInfo hri,
      long flushSeqId, Map<byte[], List<Path>> committedFiles) {
    FlushDescriptor.Builder desc = FlushDescriptor.newBuilder()
        .setAction(action)
        .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(hri.getEncodedNameAsBytes()))
        .setRegionName(UnsafeByteOperations.unsafeWrap(hri.getRegionName()))
        .setFlushSequenceNumber(flushSeqId)
        .setTableName(UnsafeByteOperations.unsafeWrap(hri.getTable().getName()));

    for (Map.Entry<byte[], List<Path>> entry : committedFiles.entrySet()) {
      WALProtos.FlushDescriptor.StoreFlushDescriptor.Builder builder =
          WALProtos.FlushDescriptor.StoreFlushDescriptor.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
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
      EventType eventType, org.apache.hadoop.hbase.client.RegionInfo hri, long seqId, ServerName server,
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
        .setTableName(UnsafeByteOperations.unsafeWrap(tableNameAsBytes))
        .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(encodedNameAsBytes))
        .setRegionName(UnsafeByteOperations.unsafeWrap(regionNameAsBytes))
        .setLogSequenceNumber(seqId)
        .setServer(toServerName(server));

    for (Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      StoreDescriptor.Builder builder = StoreDescriptor.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
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

      // Get the number of Actions
      int actionsCount = r.getRegionActionList()
          .stream()
          .mapToInt(ClientProtos.RegionAction::getActionCount)
          .sum();

      // Get first set of Actions.
      ClientProtos.RegionAction actions = r.getRegionActionList().get(0);
      String row = actions.getActionCount() <= 0? "":
        getStringForByteString(actions.getAction(0).hasGet()?
          actions.getAction(0).getGet().getRow():
          actions.getAction(0).getMutation().getRow());
      return "region= " + getStringForByteString(actions.getRegion().getValue()) +
          ", for " + actionsCount + " action(s) and 1st row key=" + row;
    } else if (m instanceof ClientProtos.MutateRequest) {
      ClientProtos.MutateRequest r = (ClientProtos.MutateRequest) m;
      return "region= " + getStringForByteString(r.getRegion().getValue()) +
          ", row=" + getStringForByteString(r.getMutation().getRow());
    } else if (m instanceof ClientProtos.CoprocessorServiceRequest) {
      ClientProtos.CoprocessorServiceRequest r = (ClientProtos.CoprocessorServiceRequest) m;
      return "coprocessorService= " + r.getCall().getServiceName() + ":" + r.getCall().getMethodName();
    }
    return "TODO: " + m.getClass().toString();
  }

  private static String getStringForByteString(ByteString bs) {
    return Bytes.toStringBinary(bs.toByteArray());
  }

  /**
   * Return SlowLogParams to maintain recent online slowlog responses
   *
   * @param message Message object {@link Message}
   * @return SlowLogParams with regionName(for filter queries) and params
   */
  public static SlowLogParams getSlowLogParams(Message message) {
    if (message == null) {
      return null;
    }
    if (message instanceof ScanRequest) {
      ScanRequest scanRequest = (ScanRequest) message;
      String regionName = getStringForByteString(scanRequest.getRegion().getValue());
      String params = TextFormat.shortDebugString(message);
      return new SlowLogParams(regionName, params);
    } else if (message instanceof MutationProto) {
      MutationProto mutationProto = (MutationProto) message;
      String params = "type= " + mutationProto.getMutateType().toString();
      return new SlowLogParams(params);
    } else if (message instanceof GetRequest) {
      GetRequest getRequest = (GetRequest) message;
      String regionName = getStringForByteString(getRequest.getRegion().getValue());
      String params = "region= " + regionName + ", row= "
        + getStringForByteString(getRequest.getGet().getRow());
      return new SlowLogParams(regionName, params);
    } else if (message instanceof MultiRequest) {
      MultiRequest multiRequest = (MultiRequest) message;
      int actionsCount = multiRequest.getRegionActionList()
        .stream()
        .mapToInt(ClientProtos.RegionAction::getActionCount)
        .sum();
      RegionAction actions = multiRequest.getRegionActionList().get(0);
      String regionName = getStringForByteString(actions.getRegion().getValue());
      String params = "region= " + regionName + ", for " + actionsCount + " action(s)";
      return new SlowLogParams(regionName, params);
    } else if (message instanceof MutateRequest) {
      MutateRequest mutateRequest = (MutateRequest) message;
      String regionName = getStringForByteString(mutateRequest.getRegion().getValue());
      String params = "region= " + regionName;
      return new SlowLogParams(regionName, params);
    } else if (message instanceof CoprocessorServiceRequest) {
      CoprocessorServiceRequest coprocessorServiceRequest = (CoprocessorServiceRequest) message;
      String params = "coprocessorService= "
        + coprocessorServiceRequest.getCall().getServiceName()
        + ":" + coprocessorServiceRequest.getCall().getMethodName();
      return new SlowLogParams(params);
    }
    String params = message.getClass().toString();
    return new SlowLogParams(params);
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
        .setNamespace(UnsafeByteOperations.unsafeWrap(tableName.getNamespace()))
        .setQualifier(UnsafeByteOperations.unsafeWrap(tableName.getQualifier())).build();
  }

  public static List<HBaseProtos.TableName> toProtoTableNameList(List<TableName> tableNameList) {
    if (tableNameList == null) {
      return new ArrayList<>();
    }
    return tableNameList.stream().map(ProtobufUtil::toProtoTableName).collect(Collectors.toList());
  }

  public static List<TableName> toTableNameList(List<HBaseProtos.TableName> tableNamesList) {
    if (tableNamesList == null) {
      return new ArrayList<>();
    }
    return tableNamesList.stream().map(ProtobufUtil::toTableName).collect(Collectors.toList());
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
      case REQUEST_NUMBER:
        return ThrottleType.REQUEST_NUMBER;
      case REQUEST_SIZE:
        return ThrottleType.REQUEST_SIZE;
      case REQUEST_CAPACITY_UNIT:
        return ThrottleType.REQUEST_CAPACITY_UNIT;
      case WRITE_NUMBER:
        return ThrottleType.WRITE_NUMBER;
      case WRITE_SIZE:
        return ThrottleType.WRITE_SIZE;
      case READ_NUMBER:
        return ThrottleType.READ_NUMBER;
      case READ_SIZE:
        return ThrottleType.READ_SIZE;
      case READ_CAPACITY_UNIT:
        return ThrottleType.READ_CAPACITY_UNIT;
      case WRITE_CAPACITY_UNIT:
        return ThrottleType.WRITE_CAPACITY_UNIT;
      default:
        throw new RuntimeException("Invalid ThrottleType " + proto);
    }
  }

  /**
   * Convert a client ThrottleType to a protocol buffer ThrottleType
   *
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
      case REQUEST_CAPACITY_UNIT:
        return QuotaProtos.ThrottleType.REQUEST_CAPACITY_UNIT;
      case READ_CAPACITY_UNIT:
        return QuotaProtos.ThrottleType.READ_CAPACITY_UNIT;
      case WRITE_CAPACITY_UNIT:
        return QuotaProtos.ThrottleType.WRITE_CAPACITY_UNIT;
      default:
        throw new RuntimeException("Invalid ThrottleType " + type);
    }
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
      case SPACE: return QuotaType.SPACE;
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
      case SPACE: return QuotaProtos.QuotaType.SPACE;
      default: throw new RuntimeException("Invalid QuotaType " + type);
    }
  }

  /**
   * Converts a protocol buffer SpaceViolationPolicy to a client SpaceViolationPolicy.
   *
   * @param proto The protocol buffer space violation policy.
   * @return The corresponding client SpaceViolationPolicy.
   */
  public static SpaceViolationPolicy toViolationPolicy(
      final QuotaProtos.SpaceViolationPolicy proto) {
    switch (proto) {
      case DISABLE: return SpaceViolationPolicy.DISABLE;
      case NO_WRITES_COMPACTIONS: return SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
      case NO_WRITES: return SpaceViolationPolicy.NO_WRITES;
      case NO_INSERTS: return SpaceViolationPolicy.NO_INSERTS;
    }
    throw new RuntimeException("Invalid SpaceViolationPolicy " + proto);
  }

  /**
   * Converts a client SpaceViolationPolicy to a protocol buffer SpaceViolationPolicy.
   *
   * @param policy The client SpaceViolationPolicy object.
   * @return The corresponding protocol buffer SpaceViolationPolicy.
   */
  public static QuotaProtos.SpaceViolationPolicy toProtoViolationPolicy(
      final SpaceViolationPolicy policy) {
    switch (policy) {
      case DISABLE: return QuotaProtos.SpaceViolationPolicy.DISABLE;
      case NO_WRITES_COMPACTIONS: return QuotaProtos.SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
      case NO_WRITES: return QuotaProtos.SpaceViolationPolicy.NO_WRITES;
      case NO_INSERTS: return QuotaProtos.SpaceViolationPolicy.NO_INSERTS;
    }
    throw new RuntimeException("Invalid SpaceViolationPolicy " + policy);
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
   * Builds a protocol buffer SpaceQuota.
   *
   * @param limit The maximum space usage for the quota in bytes.
   * @param violationPolicy The policy to apply when the quota is violated.
   * @return The protocol buffer SpaceQuota.
   */
  public static QuotaProtos.SpaceQuota toProtoSpaceQuota(
      final long limit, final SpaceViolationPolicy violationPolicy) {
    return QuotaProtos.SpaceQuota.newBuilder()
        .setSoftLimit(limit)
        .setViolationPolicy(toProtoViolationPolicy(violationPolicy))
        .build();
  }

  /**
   * Generates a marker for the WAL so that we propagate the notion of a bulk region load
   * throughout the WAL.
   *
   * @param tableName         The tableName into which the bulk load is being imported into.
   * @param encodedRegionName Encoded region name of the region which is being bulk loaded.
   * @param storeFiles        A set of store files of a column family are bulk loaded.
   * @param storeFilesSize  Map of store files and their lengths
   * @param bulkloadSeqId     sequence ID (by a force flush) used to create bulk load hfile
   *                          name
   * @return The WAL log marker for bulk loads.
   */
  public static WALProtos.BulkLoadDescriptor toBulkLoadDescriptor(TableName tableName,
    ByteString encodedRegionName, Map<byte[], List<Path>> storeFiles,
    Map<String, Long> storeFilesSize, long bulkloadSeqId) {
    return toBulkLoadDescriptor(tableName, encodedRegionName, storeFiles,
      storeFilesSize, bulkloadSeqId, null, true);
  }

  public static WALProtos.BulkLoadDescriptor toBulkLoadDescriptor(TableName tableName,
      ByteString encodedRegionName, Map<byte[], List<Path>> storeFiles,
      Map<String, Long> storeFilesSize, long bulkloadSeqId,
      List<String> clusterIds, boolean replicate) {
    BulkLoadDescriptor.Builder desc =
        BulkLoadDescriptor.newBuilder()
          .setTableName(ProtobufUtil.toProtoTableName(tableName))
          .setEncodedRegionName(encodedRegionName)
          .setBulkloadSeqNum(bulkloadSeqId)
          .setReplicate(replicate);
    if(clusterIds != null) {
      desc.addAllClusterIds(clusterIds);
    }

    for (Map.Entry<byte[], List<Path>> entry : storeFiles.entrySet()) {
      WALProtos.StoreDescriptor.Builder builder = StoreDescriptor.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
          .setStoreHomeDir(Bytes.toString(entry.getKey())); // relative to region
      for (Path path : entry.getValue()) {
        String name = path.getName();
        builder.addStoreFile(name);
        Long size = storeFilesSize.get(name) == null ? (Long) 0L : storeFilesSize.get(name);
        builder.setStoreFileSizeBytes(size);
      }
      desc.addStores(builder);
    }

    return desc.build();
  }

  /**
   * This version of protobuf's mergeDelimitedFrom avoid the hard-coded 64MB limit for decoding
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
      final InputStream limitedInput = ByteStreams.limit(in, size);
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
      ClusterStatusProtos.ReplicationLoadSink rls) {
    ReplicationLoadSink.ReplicationLoadSinkBuilder builder = ReplicationLoadSink.newBuilder();
    builder.setAgeOfLastAppliedOp(rls.getAgeOfLastAppliedOp()).
      setTimestampsOfLastAppliedOp(rls.getTimeStampsOfLastAppliedOp()).
      setTimestampStarted(rls.hasTimestampStarted()? rls.getTimestampStarted(): -1L).
      setTotalOpsProcessed(rls.hasTotalOpsProcessed()? rls.getTotalOpsProcessed(): -1L);
    return builder.build();
  }

  public static ReplicationLoadSource toReplicationLoadSource(
      ClusterStatusProtos.ReplicationLoadSource rls) {
    ReplicationLoadSource.ReplicationLoadSourceBuilder builder = ReplicationLoadSource.newBuilder();
    builder.setPeerID(rls.getPeerID()).
        setAgeOfLastShippedOp(rls.getAgeOfLastShippedOp()).
        setSizeOfLogQueue(rls.getSizeOfLogQueue()).
        setTimestampOfLastShippedOp(rls.getTimeStampOfLastShippedOp()).
        setTimeStampOfNextToReplicate(rls.getTimeStampOfNextToReplicate()).
        setReplicationLag(rls.getReplicationLag()).
        setQueueId(rls.getQueueId()).
        setRecovered(rls.getRecovered()).
        setRunning(rls.getRunning()).
        setEditsSinceRestart(rls.getEditsSinceRestart()).
        setEditsRead(rls.getEditsRead()).
        setoPsShipped(rls.getOPsShipped());
    return builder.build();
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

  /**
   * Converts an ColumnFamilyDescriptor to ColumnFamilySchema
   * @param hcd the ColumnFamilySchema
   * @return Convert this instance to a the pb column family type
   */
  public static ColumnFamilySchema toColumnFamilySchema(ColumnFamilyDescriptor hcd) {
    ColumnFamilySchema.Builder builder = ColumnFamilySchema.newBuilder();
    builder.setName(UnsafeByteOperations.unsafeWrap(hcd.getName()));
    for (Map.Entry<Bytes, Bytes> e : hcd.getValues().entrySet()) {
      BytesBytesPair.Builder aBuilder = BytesBytesPair.newBuilder();
      aBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getKey().get()));
      aBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getValue().get()));
      builder.addAttributes(aBuilder.build());
    }
    for (Map.Entry<String, String> e : hcd.getConfiguration().entrySet()) {
      NameStringPair.Builder aBuilder = NameStringPair.newBuilder();
      aBuilder.setName(e.getKey());
      aBuilder.setValue(e.getValue());
      builder.addConfiguration(aBuilder.build());
    }
    return builder.build();
  }

  /**
   * Converts a ColumnFamilySchema to ColumnFamilyDescriptor
   * @param cfs the ColumnFamilySchema
   * @return An {@link ColumnFamilyDescriptor} made from the passed in <code>cfs</code>
   */
  public static ColumnFamilyDescriptor toColumnFamilyDescriptor(final ColumnFamilySchema cfs) {
    // Use the empty constructor so we preserve the initial values set on construction for things
    // like maxVersion.  Otherwise, we pick up wrong values on deserialization which makes for
    // unrelated-looking test failures that are hard to trace back to here.
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(cfs.getName().toByteArray());
    cfs.getAttributesList().forEach(a -> builder.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray()));
    cfs.getConfigurationList().forEach(a -> builder.setConfiguration(a.getName(), a.getValue()));
    return builder.build();
  }

  /**
   * Converts an TableDescriptor to TableSchema
   * @param htd the TableDescriptor
   * @return Convert the current {@link TableDescriptor} into a pb TableSchema instance.
   */
  public static TableSchema toTableSchema(TableDescriptor htd) {
    TableSchema.Builder builder = TableSchema.newBuilder();
    builder.setTableName(toProtoTableName(htd.getTableName()));
    for (Map.Entry<Bytes, Bytes> e : htd.getValues().entrySet()) {
      BytesBytesPair.Builder aBuilder = BytesBytesPair.newBuilder();
      aBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getKey().get()));
      aBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getValue().get()));
      builder.addAttributes(aBuilder.build());
    }
    for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
      builder.addColumnFamilies(toColumnFamilySchema(hcd));
    }
    return builder.build();
  }

  /**
   * Converts a TableSchema to TableDescriptor
   * @param ts A pb TableSchema instance.
   * @return An {@link TableDescriptor} made from the passed in pb <code>ts</code>.
   */
  public static TableDescriptor toTableDescriptor(final TableSchema ts) {
    TableDescriptorBuilder builder
      = TableDescriptorBuilder.newBuilder(ProtobufUtil.toTableName(ts.getTableName()));
    ts.getColumnFamiliesList()
      .stream()
      .map(ProtobufUtil::toColumnFamilyDescriptor)
      .forEach(builder::setColumnFamily);
    ts.getAttributesList()
      .forEach(a -> builder.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray()));
    ts.getConfigurationList()
      .forEach(a -> builder.setValue(a.getName(), a.getValue()));
    return builder.build();
  }

  /**
   * Creates {@link CompactionState} from {@link GetRegionInfoResponse.CompactionState} state
   * @param state the protobuf CompactionState
   * @return CompactionState
   */
  public static CompactionState createCompactionState(GetRegionInfoResponse.CompactionState state) {
    return CompactionState.valueOf(state.toString());
  }

  public static GetRegionInfoResponse.CompactionState createCompactionState(CompactionState state) {
    return GetRegionInfoResponse.CompactionState.valueOf(state.toString());
  }

  /**
   * Creates {@link CompactionState} from {@link RegionLoad.CompactionState} state
   * @param state the protobuf CompactionState
   * @return CompactionState
   */
  public static CompactionState createCompactionStateForRegionLoad(
      RegionLoad.CompactionState state) {
    return CompactionState.valueOf(state.toString());
  }

  public static RegionLoad.CompactionState createCompactionStateForRegionLoad(
      CompactionState state) {
    return RegionLoad.CompactionState.valueOf(state.toString());
  }

  public static Optional<Long> toOptionalTimestamp(MajorCompactionTimestampResponse resp) {
    long timestamp = resp.getCompactionTimestamp();
    return timestamp == 0 ? Optional.empty() : Optional.of(timestamp);
  }

  /**
   * Creates {@link SnapshotProtos.SnapshotDescription.Type} from {@link SnapshotType}
   * @param type the SnapshotDescription type
   * @return the protobuf SnapshotDescription type
   */
  public static SnapshotProtos.SnapshotDescription.Type
      createProtosSnapShotDescType(SnapshotType type) {
    return SnapshotProtos.SnapshotDescription.Type.valueOf(type.name());
  }

  /**
   * Creates {@link SnapshotProtos.SnapshotDescription.Type} from the type of SnapshotDescription
   * string
   * @param snapshotDesc string representing the snapshot description type
   * @return the protobuf SnapshotDescription type
   */
  public static SnapshotProtos.SnapshotDescription.Type
      createProtosSnapShotDescType(String snapshotDesc) {
    return SnapshotProtos.SnapshotDescription.Type.valueOf(snapshotDesc.toUpperCase(Locale.ROOT));
  }

  /**
   * Creates {@link SnapshotType} from the {@link SnapshotProtos.SnapshotDescription.Type}
   * @param  type the snapshot description type
   * @return      the protobuf SnapshotDescription type
   */
  public static SnapshotType createSnapshotType(SnapshotProtos.SnapshotDescription.Type type) {
    return SnapshotType.valueOf(type.toString());
  }

  /**
   * Convert from {@link SnapshotDescription} to {@link SnapshotProtos.SnapshotDescription}
   * @param snapshotDesc the POJO SnapshotDescription
   * @return the protobuf SnapshotDescription
   */
  public static SnapshotProtos.SnapshotDescription
      createHBaseProtosSnapshotDesc(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription.Builder builder = SnapshotProtos.SnapshotDescription.newBuilder();
    if (snapshotDesc.getTableName() != null) {
      builder.setTable(snapshotDesc.getTableNameAsString());
    }
    if (snapshotDesc.getName() != null) {
      builder.setName(snapshotDesc.getName());
    }
    if (snapshotDesc.getOwner() != null) {
      builder.setOwner(snapshotDesc.getOwner());
    }
    if (snapshotDesc.getCreationTime() != -1L) {
      builder.setCreationTime(snapshotDesc.getCreationTime());
    }
    if (snapshotDesc.getTtl() != -1L &&
        snapshotDesc.getTtl() < TimeUnit.MILLISECONDS.toSeconds(Long.MAX_VALUE)) {
      builder.setTtl(snapshotDesc.getTtl());
    }
    if (snapshotDesc.getVersion() != -1) {
      builder.setVersion(snapshotDesc.getVersion());
    }
    if (snapshotDesc.getMaxFileSize() != -1) {
      builder.setMaxFileSize(snapshotDesc.getMaxFileSize());
    }
    builder.setType(ProtobufUtil.createProtosSnapShotDescType(snapshotDesc.getType()));
    return builder.build();
  }

  /**
   * Convert from {@link SnapshotProtos.SnapshotDescription} to {@link SnapshotDescription}
   * @param snapshotDesc the protobuf SnapshotDescription
   * @return the POJO SnapshotDescription
   */
  public static SnapshotDescription
      createSnapshotDesc(SnapshotProtos.SnapshotDescription snapshotDesc) {
    final Map<String, Object> snapshotProps = new HashMap<>();
    snapshotProps.put("TTL", snapshotDesc.getTtl());
    snapshotProps.put(TableDescriptorBuilder.MAX_FILESIZE, snapshotDesc.getMaxFileSize());
    return new SnapshotDescription(snapshotDesc.getName(),
            snapshotDesc.hasTable() ? TableName.valueOf(snapshotDesc.getTable()) : null,
            createSnapshotType(snapshotDesc.getType()), snapshotDesc.getOwner(),
            snapshotDesc.getCreationTime(), snapshotDesc.getVersion(), snapshotProps);
  }

  public static RegionLoadStats createRegionLoadStats(ClientProtos.RegionLoadStats stats) {
    return new RegionLoadStats(stats.getMemStoreLoad(), stats.getHeapOccupancy(),
        stats.getCompactionPressure());
  }

  /**
   * @param msg
   * @return A String version of the passed in <code>msg</code>
   */
  public static String toText(Message msg) {
    return TextFormat.shortDebugString(msg);
  }

  public static byte [] toBytes(ByteString bs) {
    return bs.toByteArray();
  }

  /**
   * Contain ServiceException inside here. Take a callable that is doing our pb rpc and run it.
   * @throws IOException
   */
  public static <T> T call(Callable<T> callable) throws IOException {
    try {
      return callable.call();
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
    * Create a protocol buffer GetStoreFileRequest for a given region name
    *
    * @param regionName the name of the region to get info
    * @param family the family to get store file list
    * @return a protocol buffer GetStoreFileRequest
    */
   public static GetStoreFileRequest
       buildGetStoreFileRequest(final byte[] regionName, final byte[] family) {
     GetStoreFileRequest.Builder builder = GetStoreFileRequest.newBuilder();
     RegionSpecifier region = RequestConverter.buildRegionSpecifier(
       RegionSpecifierType.REGION_NAME, regionName);
     builder.setRegion(region);
     builder.addFamily(UnsafeByteOperations.unsafeWrap(family));
     return builder.build();
   }

  /**
   * Create a CloseRegionRequest for a given region name
   * @param regionName the name of the region to close
   * @return a CloseRegionRequest
   */
  public static CloseRegionRequest buildCloseRegionRequest(ServerName server, byte[] regionName) {
    return ProtobufUtil.buildCloseRegionRequest(server, regionName, null);
  }

  public static CloseRegionRequest buildCloseRegionRequest(ServerName server, byte[] regionName,
      ServerName destinationServer) {
    return buildCloseRegionRequest(server, regionName, destinationServer, -1);
  }

  public static CloseRegionRequest buildCloseRegionRequest(ServerName server, byte[] regionName,
      ServerName destinationServer, long closeProcId) {
    CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
    RegionSpecifier region =
      RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (destinationServer != null) {
      builder.setDestinationServer(toServerName(destinationServer));
    }
    if (server != null) {
      builder.setServerStartCode(server.getStartcode());
    }
    builder.setCloseProcId(closeProcId);
    return builder.build();
  }

  public static ProcedureDescription buildProcedureDescription(String signature, String instance,
      Map<String, String> props) {
    ProcedureDescription.Builder builder =
        ProcedureDescription.newBuilder().setSignature(signature).setInstance(instance);
    if (props != null && !props.isEmpty()) {
      props.entrySet().forEach(entry -> builder.addConfiguration(
        NameStringPair.newBuilder().setName(entry.getKey()).setValue(entry.getValue()).build()));
    }
    return builder.build();
  }

  /**
   * Get the Meta region state from the passed data bytes. Can handle both old and new style
   * server names.
   * @param data protobuf serialized data with meta server name.
   * @param replicaId replica ID for this region
   * @return RegionState instance corresponding to the serialized data.
   * @throws DeserializationException if the data is invalid.
   */
  public static RegionState parseMetaRegionStateFrom(final byte[] data, int replicaId)
      throws DeserializationException {
    RegionState.State state = RegionState.State.OPEN;
    ServerName serverName;
    if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
      try {
        int prefixLen = ProtobufUtil.lengthOfPBMagic();
        ZooKeeperProtos.MetaRegionServer rl =
            ZooKeeperProtos.MetaRegionServer.parser().parseFrom(data, prefixLen,
                data.length - prefixLen);
        if (rl.hasState()) {
          state = RegionState.State.convert(rl.getState());
        }
        HBaseProtos.ServerName sn = rl.getServer();
        serverName = ServerName.valueOf(
            sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException("Unable to parse meta region location");
      }
    } else {
      // old style of meta region location?
      serverName = parseServerNameFrom(data);
    }
    if (serverName == null) {
      state = RegionState.State.OFFLINE;
    }
    return new RegionState(RegionReplicaUtil.getRegionInfoForReplica(
        RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId), state, serverName);
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
  public static ServerName parseServerNameFrom(final byte [] data) throws DeserializationException {
    if (data == null || data.length <= 0) return null;
    if (ProtobufMagic.isPBMagicPrefix(data)) {
      int prefixLen = ProtobufMagic.lengthOfPBMagic();
      try {
        ZooKeeperProtos.Master rss =
          ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
        org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName sn =
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

  /**
   * Helper to convert the protobuf Procedure to JSON String
   * @return Convert the current Protocol Buffers Procedure to JSON String
   */
  public static String toProcedureJson(List<ProcedureProtos.Procedure> procProtos) {
    JsonArray procJsons = new JsonArray(procProtos.size());
    for (ProcedureProtos.Procedure procProto : procProtos) {
      try {
        JsonElement procJson = ProtobufMessageConverter.toJsonElement(procProto);
        procJsons.add(procJson);
      } catch (InvalidProtocolBufferException e) {
        procJsons.add(e.toString());
      }
    }
    return procJsons.toString();
  }

  public static String toLockJson(List<LockServiceProtos.LockedResource> lockedResourceProtos) {
    JsonArray lockedResourceJsons = new JsonArray(lockedResourceProtos.size());
    for (LockServiceProtos.LockedResource lockedResourceProto : lockedResourceProtos) {
      try {
        JsonElement lockedResourceJson = ProtobufMessageConverter.toJsonElement(lockedResourceProto);
        lockedResourceJsons.add(lockedResourceJson);
      } catch (InvalidProtocolBufferException e) {
        lockedResourceJsons.add(e.toString());
      }
    }
    return lockedResourceJsons.toString();
  }

  /**
   * Convert a RegionInfo to a Proto RegionInfo
   *
   * @param info the RegionInfo to convert
   * @return the converted Proto RegionInfo
   */
  public static HBaseProtos.RegionInfo toRegionInfo(final org.apache.hadoop.hbase.client.RegionInfo info) {
    if (info == null) {
      return null;
    }
    HBaseProtos.RegionInfo.Builder builder = HBaseProtos.RegionInfo.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(info.getTable()));
    builder.setRegionId(info.getRegionId());
    if (info.getStartKey() != null) {
      builder.setStartKey(UnsafeByteOperations.unsafeWrap(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      builder.setEndKey(UnsafeByteOperations.unsafeWrap(info.getEndKey()));
    }
    builder.setOffline(info.isOffline());
    builder.setSplit(info.isSplit());
    builder.setReplicaId(info.getReplicaId());
    return builder.build();
  }

  /**
   * Convert HBaseProto.RegionInfo to a RegionInfo
   *
   * @param proto the RegionInfo to convert
   * @return the converted RegionInfo
   */
  public static org.apache.hadoop.hbase.client.RegionInfo toRegionInfo(final HBaseProtos.RegionInfo proto) {
    if (proto == null) {
      return null;
    }
    TableName tableName = ProtobufUtil.toTableName(proto.getTableName());
    long regionId = proto.getRegionId();
    int defaultReplicaId = org.apache.hadoop.hbase.client.RegionInfo.DEFAULT_REPLICA_ID;
    int replicaId = proto.hasReplicaId()? proto.getReplicaId(): defaultReplicaId;
    if (tableName.equals(TableName.META_TABLE_NAME) && replicaId == defaultReplicaId) {
      return RegionInfoBuilder.FIRST_META_REGIONINFO;
    }
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
    RegionInfoBuilder rib = RegionInfoBuilder.newBuilder(tableName)
    .setStartKey(startKey)
    .setEndKey(endKey)
    .setRegionId(regionId)
    .setReplicaId(replicaId)
    .setSplit(split);
    if (proto.hasOffline()) {
      rib.setOffline(proto.getOffline());
    }
    return rib.build();
  }

  public static HBaseProtos.RegionLocation toRegionLocation(HRegionLocation loc) {
    HBaseProtos.RegionLocation.Builder builder = HBaseProtos.RegionLocation.newBuilder();
    builder.setRegionInfo(toRegionInfo(loc.getRegion()));
    if (loc.getServerName() != null) {
      builder.setServerName(toServerName(loc.getServerName()));
    }
    builder.setSeqNum(loc.getSeqNum());
    return builder.build();
  }

  public static HRegionLocation toRegionLocation(HBaseProtos.RegionLocation proto) {
    org.apache.hadoop.hbase.client.RegionInfo regionInfo = toRegionInfo(proto.getRegionInfo());
    ServerName serverName = proto.hasServerName() ? toServerName(proto.getServerName()) : null;
    return new HRegionLocation(regionInfo, serverName, proto.getSeqNum());
  }

  public static List<SnapshotDescription> toSnapshotDescriptionList(
      GetCompletedSnapshotsResponse response, Pattern pattern) {
    return response.getSnapshotsList().stream().map(ProtobufUtil::createSnapshotDesc)
        .filter(snap -> pattern != null ? pattern.matcher(snap.getName()).matches() : true)
        .collect(Collectors.toList());
  }

  public static CacheEvictionStats toCacheEvictionStats(
      HBaseProtos.CacheEvictionStats stats) throws IOException{
    CacheEvictionStatsBuilder builder = CacheEvictionStats.builder();
    builder.withEvictedBlocks(stats.getEvictedBlocks())
        .withMaxCacheSize(stats.getMaxCacheSize());
    if (stats.getExceptionCount() > 0) {
      for (HBaseProtos.RegionExceptionMessage exception : stats.getExceptionList()) {
        HBaseProtos.RegionSpecifier rs = exception.getRegion();
        byte[] regionName = rs.getValue().toByteArray();
        builder.addException(regionName, ProtobufUtil.toException(exception.getException()));
      }
    }
    return builder.build();
  }

  public static HBaseProtos.CacheEvictionStats toCacheEvictionStats(
      CacheEvictionStats cacheEvictionStats) {
    HBaseProtos.CacheEvictionStats.Builder builder
        = HBaseProtos.CacheEvictionStats.newBuilder();
    for (Map.Entry<byte[], Throwable> entry : cacheEvictionStats.getExceptions().entrySet()) {
      builder.addException(
          RegionExceptionMessage.newBuilder()
          .setRegion(RequestConverter.buildRegionSpecifier(
                  RegionSpecifierType.REGION_NAME, entry.getKey()))
          .setException(ResponseConverter.buildException(entry.getValue()))
          .build()
      );
    }
    return builder
        .setEvictedBlocks(cacheEvictionStats.getEvictedBlocks())
        .setMaxCacheSize(cacheEvictionStats.getMaxCacheSize())
        .build();
  }

  public static ClusterStatusProtos.ReplicationLoadSource toReplicationLoadSource(
      ReplicationLoadSource rls) {
    return ClusterStatusProtos.ReplicationLoadSource.newBuilder()
        .setPeerID(rls.getPeerID())
        .setAgeOfLastShippedOp(rls.getAgeOfLastShippedOp())
        .setSizeOfLogQueue((int) rls.getSizeOfLogQueue())
        .setTimeStampOfLastShippedOp(rls.getTimestampOfLastShippedOp())
        .setReplicationLag(rls.getReplicationLag())
        .setQueueId(rls.getQueueId())
        .setRecovered(rls.isRecovered())
        .setRunning(rls.isRunning())
        .setEditsSinceRestart(rls.hasEditsSinceRestart())
        .setTimeStampOfNextToReplicate(rls.getTimeStampOfNextToReplicate())
        .setOPsShipped(rls.getOPsShipped())
        .setEditsRead(rls.getEditsRead())
        .build();
  }

  public static ClusterStatusProtos.ReplicationLoadSink toReplicationLoadSink(
      ReplicationLoadSink rls) {
    return ClusterStatusProtos.ReplicationLoadSink.newBuilder()
        .setAgeOfLastAppliedOp(rls.getAgeOfLastAppliedOp())
        .setTimeStampsOfLastAppliedOp(rls.getTimestampsOfLastAppliedOp())
        .setTimestampStarted(rls.getTimestampStarted())
        .setTotalOpsProcessed(rls.getTotalOpsProcessed())
        .build();
  }

  public static HBaseProtos.TimeRange toTimeRange(TimeRange timeRange) {
    if (timeRange == null) {
      timeRange = TimeRange.allTime();
    }
    return HBaseProtos.TimeRange.newBuilder().setFrom(timeRange.getMin())
      .setTo(timeRange.getMax())
      .build();
  }

  public static byte[] toCompactionEventTrackerBytes(Set<String> storeFiles) {
    HFileProtos.CompactionEventTracker.Builder builder =
        HFileProtos.CompactionEventTracker.newBuilder();
    storeFiles.forEach(sf -> builder.addCompactedStoreFile(ByteString.copyFromUtf8(sf)));
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  public static Set<String> toCompactedStoreFiles(byte[] bytes) throws IOException {
    if (bytes != null && ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pbLen = ProtobufUtil.lengthOfPBMagic();
      HFileProtos.CompactionEventTracker.Builder builder =
          HFileProtos.CompactionEventTracker.newBuilder();
      ProtobufUtil.mergeFrom(builder, bytes, pbLen, bytes.length - pbLen);
      HFileProtos.CompactionEventTracker compactionEventTracker = builder.build();
      List<ByteString> compactedStoreFiles = compactionEventTracker.getCompactedStoreFileList();
      if (compactedStoreFiles != null && compactedStoreFiles.size() != 0) {
        return compactedStoreFiles.stream().map(ByteString::toStringUtf8)
            .collect(Collectors.toSet());
      }
    }
    return Collections.emptySet();
  }

  public static ClusterStatusProtos.RegionStatesCount toTableRegionStatesCount(
      RegionStatesCount regionStatesCount) {
    int openRegions = 0;
    int splitRegions = 0;
    int closedRegions = 0;
    int regionsInTransition = 0;
    int totalRegions = 0;
    if (regionStatesCount != null) {
      openRegions = regionStatesCount.getOpenRegions();
      splitRegions = regionStatesCount.getSplitRegions();
      closedRegions = regionStatesCount.getClosedRegions();
      regionsInTransition = regionStatesCount.getRegionsInTransition();
      totalRegions = regionStatesCount.getTotalRegions();
    }
    return ClusterStatusProtos.RegionStatesCount.newBuilder()
      .setOpenRegions(openRegions)
      .setSplitRegions(splitRegions)
      .setClosedRegions(closedRegions)
      .setRegionsInTransition(regionsInTransition)
      .setTotalRegions(totalRegions)
      .build();
  }

  public static RegionStatesCount toTableRegionStatesCount(
    ClusterStatusProtos.RegionStatesCount regionStatesCount) {
    int openRegions = 0;
    int splitRegions = 0;
    int closedRegions = 0;
    int regionsInTransition = 0;
    int totalRegions = 0;
    if (regionStatesCount != null) {
      closedRegions = regionStatesCount.getClosedRegions();
      regionsInTransition = regionStatesCount.getRegionsInTransition();
      openRegions = regionStatesCount.getOpenRegions();
      splitRegions = regionStatesCount.getSplitRegions();
      totalRegions = regionStatesCount.getTotalRegions();
    }
    return new RegionStatesCount.RegionStatesCountBuilder()
      .setOpenRegions(openRegions)
      .setSplitRegions(splitRegions)
      .setClosedRegions(closedRegions)
      .setRegionsInTransition(regionsInTransition)
      .setTotalRegions(totalRegions)
      .build();
  }

  /**
   * Convert Protobuf class
   * {@link org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload}
   * To client SlowLog Payload class {@link OnlineLogRecord}
   *
   * @param slowLogPayload SlowLog Payload protobuf instance
   * @return SlowLog Payload for client usecase
   */
  private static LogEntry getSlowLogRecord(
      final TooSlowLog.SlowLogPayload slowLogPayload) {
    OnlineLogRecord onlineLogRecord = new OnlineLogRecord.OnlineLogRecordBuilder()
      .setCallDetails(slowLogPayload.getCallDetails())
      .setClientAddress(slowLogPayload.getClientAddress())
      .setMethodName(slowLogPayload.getMethodName())
      .setMultiGetsCount(slowLogPayload.getMultiGets())
      .setMultiMutationsCount(slowLogPayload.getMultiMutations())
      .setMultiServiceCalls(slowLogPayload.getMultiServiceCalls())
      .setParam(slowLogPayload.getParam())
      .setProcessingTime(slowLogPayload.getProcessingTime())
      .setQueueTime(slowLogPayload.getQueueTime())
      .setRegionName(slowLogPayload.getRegionName())
      .setResponseSize(slowLogPayload.getResponseSize())
      .setServerClass(slowLogPayload.getServerClass())
      .setStartTime(slowLogPayload.getStartTime())
      .setUserName(slowLogPayload.getUserName())
      .build();
    return onlineLogRecord;
  }

  /**
   * Convert  AdminProtos#SlowLogResponses to list of {@link OnlineLogRecord}
   *
   * @param logEntry slowlog response protobuf instance
   * @return list of SlowLog payloads for client usecase
   */
  public static List<LogEntry> toSlowLogPayloads(
      final HBaseProtos.LogEntry logEntry) {
    try {
      final String logClassName = logEntry.getLogClassName();
      Class<?> logClass = Class.forName(logClassName).asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("SlowLogResponses")) {
        AdminProtos.SlowLogResponses slowLogResponses = (AdminProtos.SlowLogResponses) method
          .invoke(null, logEntry.getLogMessage());
        return slowLogResponses.getSlowLogPayloadsList().stream()
          .map(ProtobufUtil::getSlowLogRecord).collect(Collectors.toList());
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
      | InvocationTargetException e) {
      throw new RuntimeException("Error while retrieving response from server");
    }
    throw new RuntimeException("Invalid response from server");
  }

  /**
   * Convert {@link ClearSlowLogResponses} to boolean
   *
   * @param clearSlowLogResponses Clear slowlog response protobuf instance
   * @return boolean representing clear slowlog response
   */
  public static boolean toClearSlowLogPayload(final ClearSlowLogResponses clearSlowLogResponses) {
    return clearSlowLogResponses.getIsCleaned();
  }

  public static void populateBalanceRSGroupResponse(RSGroupAdminProtos.BalanceRSGroupResponse.Builder responseBuilder, BalanceResponse response) {
    responseBuilder
      .setBalanceRan(response.isBalancerRan())
      .setMovesCalculated(response.getMovesCalculated())
      .setMovesExecuted(response.getMovesExecuted());
  }

  public static BalanceResponse toBalanceResponse(RSGroupAdminProtos.BalanceRSGroupResponse response) {
    return BalanceResponse.newBuilder()
      .setBalancerRan(response.getBalanceRan())
      .setMovesExecuted(response.hasMovesExecuted() ? response.getMovesExecuted() : 0)
      .setMovesCalculated(response.hasMovesCalculated() ? response.getMovesCalculated() : 0)
      .build();
  }

  public static RSGroupAdminProtos.BalanceRSGroupRequest createBalanceRSGroupRequest(String groupName, BalanceRequest request) {
    return RSGroupAdminProtos.BalanceRSGroupRequest.newBuilder()
      .setRSGroupName(groupName)
      .setDryRun(request.isDryRun())
      .setIgnoreRit(request.isIgnoreRegionsInTransition())
      .build();
  }

  public static BalanceRequest toBalanceRequest(RSGroupAdminProtos.BalanceRSGroupRequest request) {
    return BalanceRequest.newBuilder()
      .setDryRun(request.hasDryRun() && request.getDryRun())
      .setIgnoreRegionsInTransition(request.hasIgnoreRit() && request.getIgnoreRit())
      .build();
  }

  public static RSGroupInfo toGroupInfo(RSGroupProtos.RSGroupInfo proto) {
    RSGroupInfo rsGroupInfo = new RSGroupInfo(proto.getName());

    Collection<Address> addresses = proto.getServersList().parallelStream()
      .map(serverName -> Address.fromParts(serverName.getHostName(), serverName.getPort()))
      .collect(Collectors.toList());
    rsGroupInfo.addAllServers(addresses);

    Collection<TableName> tables = proto.getTablesList().parallelStream()
      .map(ProtobufUtil::toTableName).collect(Collectors.toList());
    rsGroupInfo.addAllTables(tables);

    proto.getConfigurationList().forEach(pair ->
        rsGroupInfo.setConfiguration(pair.getName(), pair.getValue()));
    return rsGroupInfo;
  }

  public static RSGroupProtos.RSGroupInfo toProtoGroupInfo(RSGroupInfo pojo) {
    List<HBaseProtos.TableName> tables = new ArrayList<>(pojo.getTables().size());
    for (TableName arg : pojo.getTables()) {
      tables.add(ProtobufUtil.toProtoTableName(arg));
    }
    List<HBaseProtos.ServerName> hostports = new ArrayList<>(pojo.getServers().size());
    for (Address el : pojo.getServers()) {
      hostports.add(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
          .setPort(el.getPort()).build());
    }
    List<NameStringPair> configuration = pojo.getConfiguration().entrySet()
        .stream().map(entry -> NameStringPair.newBuilder()
            .setName(entry.getKey()).setValue(entry.getValue()).build())
        .collect(Collectors.toList());
    return RSGroupProtos.RSGroupInfo.newBuilder().setName(pojo.getName()).addAllServers(hostports)
        .addAllTables(tables).addAllConfiguration(configuration).build();
  }

  public static CheckAndMutate toCheckAndMutate(ClientProtos.Condition condition,
    MutationProto mutation, CellScanner cellScanner) throws IOException {
    byte[] row = condition.getRow().toByteArray();
    CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(row);
    Filter filter = condition.hasFilter() ? ProtobufUtil.toFilter(condition.getFilter()) : null;
    if (filter != null) {
      builder.ifMatches(filter);
    } else {
      builder.ifMatches(condition.getFamily().toByteArray(),
        condition.getQualifier().toByteArray(),
        CompareOperator.valueOf(condition.getCompareType().name()),
        ProtobufUtil.toComparator(condition.getComparator()).getValue());
    }
    TimeRange timeRange = condition.hasTimeRange() ?
      ProtobufUtil.toTimeRange(condition.getTimeRange()) : TimeRange.allTime();
    builder.timeRange(timeRange);

    try {
      MutationType type = mutation.getMutateType();
      switch (type) {
        case PUT:
          return builder.build(ProtobufUtil.toPut(mutation, cellScanner));
        case DELETE:
          return builder.build(ProtobufUtil.toDelete(mutation, cellScanner));
        case INCREMENT:
          return builder.build(ProtobufUtil.toIncrement(mutation, cellScanner));
        case APPEND:
          return builder.build(ProtobufUtil.toAppend(mutation, cellScanner));
        default:
          throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
  }

  public static CheckAndMutate toCheckAndMutate(ClientProtos.Condition condition,
    List<Mutation> mutations) throws IOException {
    assert mutations.size() > 0;
    byte[] row = condition.getRow().toByteArray();
    CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(row);
    Filter filter = condition.hasFilter() ? ProtobufUtil.toFilter(condition.getFilter()) : null;
    if (filter != null) {
      builder.ifMatches(filter);
    } else {
      builder.ifMatches(condition.getFamily().toByteArray(),
        condition.getQualifier().toByteArray(),
        CompareOperator.valueOf(condition.getCompareType().name()),
        ProtobufUtil.toComparator(condition.getComparator()).getValue());
    }
    TimeRange timeRange = condition.hasTimeRange() ?
      ProtobufUtil.toTimeRange(condition.getTimeRange()) : TimeRange.allTime();
    builder.timeRange(timeRange);

    try {
      if (mutations.size() == 1) {
        Mutation m = mutations.get(0);
        if (m instanceof Put) {
          return builder.build((Put) m);
        } else if (m instanceof Delete) {
          return builder.build((Delete) m);
        } else if (m instanceof Increment) {
          return builder.build((Increment) m);
        } else if (m instanceof Append) {
          return builder.build((Append) m);
        } else {
          throw new DoNotRetryIOException("Unsupported mutate type: " + m.getClass()
            .getSimpleName().toUpperCase());
        }
      } else {
        return builder.build(new RowMutations(mutations.get(0).getRow()).add(mutations));
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
  }

  public static ClientProtos.Condition toCondition(final byte[] row, final byte[] family,
    final byte[] qualifier, final CompareOperator op, final byte[] value, final Filter filter,
    final TimeRange timeRange) throws IOException {

    ClientProtos.Condition.Builder builder = ClientProtos.Condition.newBuilder()
      .setRow(UnsafeByteOperations.unsafeWrap(row));

    if (filter != null) {
      builder.setFilter(ProtobufUtil.toFilter(filter));
    } else {
      builder.setFamily(UnsafeByteOperations.unsafeWrap(family))
        .setQualifier(UnsafeByteOperations.unsafeWrap(
          qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier))
        .setComparator(ProtobufUtil.toComparator(new BinaryComparator(value)))
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

  public static List<LogEntry> toBalancerDecisionResponse(
      HBaseProtos.LogEntry logEntry) {
    try {
      final String logClassName = logEntry.getLogClassName();
      Class<?> logClass = Class.forName(logClassName).asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("BalancerDecisionsResponse")) {
        MasterProtos.BalancerDecisionsResponse response =
          (MasterProtos.BalancerDecisionsResponse) method
            .invoke(null, logEntry.getLogMessage());
        return getBalancerDecisionEntries(response);
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
      | InvocationTargetException e) {
      throw new RuntimeException("Error while retrieving response from server");
    }
    throw new RuntimeException("Invalid response from server");
  }

  public static List<LogEntry> toBalancerRejectionResponse(
    HBaseProtos.LogEntry logEntry) {
    try {
      final String logClassName = logEntry.getLogClassName();
      Class<?> logClass = Class.forName(logClassName).asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("BalancerRejectionsResponse")) {
        MasterProtos.BalancerRejectionsResponse response =
          (MasterProtos.BalancerRejectionsResponse) method
            .invoke(null, logEntry.getLogMessage());
        return getBalancerRejectionEntries(response);
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
      | InvocationTargetException e) {
      throw new RuntimeException("Error while retrieving response from server");
    }
    throw new RuntimeException("Invalid response from server");
  }

  public static List<LogEntry> getBalancerDecisionEntries(
      MasterProtos.BalancerDecisionsResponse response) {
    List<RecentLogs.BalancerDecision> balancerDecisions = response.getBalancerDecisionList();
    if (CollectionUtils.isEmpty(balancerDecisions)) {
      return Collections.emptyList();
    }
    return balancerDecisions.stream().map(balancerDecision -> new BalancerDecision.Builder()
      .setInitTotalCost(balancerDecision.getInitTotalCost())
      .setInitialFunctionCosts(balancerDecision.getInitialFunctionCosts())
      .setComputedTotalCost(balancerDecision.getComputedTotalCost())
      .setFinalFunctionCosts(balancerDecision.getFinalFunctionCosts())
      .setComputedSteps(balancerDecision.getComputedSteps())
      .setRegionPlans(balancerDecision.getRegionPlansList()).build())
      .collect(Collectors.toList());
  }

  public static List<LogEntry> getBalancerRejectionEntries(
    MasterProtos.BalancerRejectionsResponse response) {
    List<RecentLogs.BalancerRejection> balancerRejections = response.getBalancerRejectionList();
    if (CollectionUtils.isEmpty(balancerRejections)) {
      return Collections.emptyList();
    }
    return balancerRejections.stream().map(balancerRejection -> new BalancerRejection.Builder()
      .setReason(balancerRejection.getReason())
      .setCostFuncInfoList(balancerRejection.getCostFuncInfoList())
      .build())
      .collect(Collectors.toList());
  }

  public static HBaseProtos.LogRequest toBalancerDecisionRequest(int limit) {
    MasterProtos.BalancerDecisionsRequest balancerDecisionsRequest =
      MasterProtos.BalancerDecisionsRequest.newBuilder().setLimit(limit).build();
    return HBaseProtos.LogRequest.newBuilder()
      .setLogClassName(balancerDecisionsRequest.getClass().getName())
      .setLogMessage(balancerDecisionsRequest.toByteString())
      .build();
  }

  public static HBaseProtos.LogRequest toBalancerRejectionRequest(int limit) {
    MasterProtos.BalancerRejectionsRequest balancerRejectionsRequest =
      MasterProtos.BalancerRejectionsRequest.newBuilder().setLimit(limit).build();
    return HBaseProtos.LogRequest.newBuilder()
      .setLogClassName(balancerRejectionsRequest.getClass().getName())
      .setLogMessage(balancerRejectionsRequest.toByteString())
      .build();
  }

  public static MasterProtos.BalanceRequest toBalanceRequest(BalanceRequest request) {
    return MasterProtos.BalanceRequest.newBuilder()
      .setDryRun(request.isDryRun())
      .setIgnoreRit(request.isIgnoreRegionsInTransition())
      .build();
  }

  public static BalanceRequest toBalanceRequest(MasterProtos.BalanceRequest request) {
    return BalanceRequest.newBuilder()
      .setDryRun(request.hasDryRun() && request.getDryRun())
      .setIgnoreRegionsInTransition(request.hasIgnoreRit() && request.getIgnoreRit())
      .build();
  }

  public static MasterProtos.BalanceResponse toBalanceResponse(BalanceResponse response) {
    return MasterProtos.BalanceResponse.newBuilder()
      .setBalancerRan(response.isBalancerRan())
      .setMovesCalculated(response.getMovesCalculated())
      .setMovesExecuted(response.getMovesExecuted())
      .build();
  }

  public static BalanceResponse toBalanceResponse(MasterProtos.BalanceResponse response) {
    return BalanceResponse.newBuilder()
      .setBalancerRan(response.hasBalancerRan() && response.getBalancerRan())
      .setMovesCalculated(response.hasMovesCalculated() ? response.getMovesExecuted() : 0)
      .setMovesExecuted(response.hasMovesExecuted() ? response.getMovesExecuted() : 0)
      .build();
  }

  public static ServerTask getServerTask(ClusterStatusProtos.ServerTask task) {
    return ServerTaskBuilder.newBuilder()
      .setDescription(task.getDescription())
      .setStatus(task.getStatus())
      .setState(ServerTask.State.valueOf(task.getState().name()))
      .setStartTime(task.getStartTime())
      .setCompletionTime(task.getCompletionTime())
      .build();
  }

  public static ClusterStatusProtos.ServerTask toServerTask(ServerTask task) {
    return ClusterStatusProtos.ServerTask.newBuilder()
      .setDescription(task.getDescription())
      .setStatus(task.getStatus())
      .setState(ClusterStatusProtos.ServerTask.State.valueOf(task.getState().name()))
      .setStartTime(task.getStartTime())
      .setCompletionTime(task.getCompletionTime())
      .build();
  }

}
