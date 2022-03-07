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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.HIGH_QOS;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.util.FutureUtils.unwrapCompletionException;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsAggregator;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.RegionMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.AdminRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.ServerRequestCallerBuilder;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ShadedAccessControlUtil;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
import org.apache.hbase.thirdparty.io.netty.util.TimerTask;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnStoreFileTrackerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnStoreFileTrackerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableStoreFileTrackerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableStoreFileTrackerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse.RegionSizes;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.AddRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.AddRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetConfiguredNamespacesAndTablesInRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetConfiguredNamespacesAndTablesInRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.ListTablesInRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.ListTablesInRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.MoveServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RenameRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RenameRSGroupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.UpdateRSGroupConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.UpdateRSGroupConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * The implementation of AsyncAdmin.
 * <p>
 * The word 'Raw' means that this is a low level class. The returned {@link CompletableFuture} will
 * be finished inside the rpc framework thread, which means that the callbacks registered to the
 * {@link CompletableFuture} will also be executed inside the rpc framework thread. So users who use
 * this class should not try to do time consuming tasks in the callbacks.
 * @since 2.0.0
 * @see AsyncHBaseAdmin
 * @see AsyncConnection#getAdmin()
 * @see AsyncConnection#getAdminBuilder()
 */
@InterfaceAudience.Private
class RawAsyncHBaseAdmin implements AsyncAdmin {

  public static final String FLUSH_TABLE_PROCEDURE_SIGNATURE = "flush-table-proc";

  private static final Logger LOG = LoggerFactory.getLogger(AsyncHBaseAdmin.class);

  private final AsyncConnectionImpl connection;

  private final HashedWheelTimer retryTimer;

  private final AsyncTable<AdvancedScanResultConsumer> metaTable;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final long pauseNsForServerOverloaded;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final NonceGenerator ng;

  RawAsyncHBaseAdmin(AsyncConnectionImpl connection, HashedWheelTimer retryTimer,
      AsyncAdminBuilderBase builder) {
    this.connection = connection;
    this.retryTimer = retryTimer;
    this.metaTable = connection.getTable(META_TABLE_NAME);
    this.rpcTimeoutNs = builder.rpcTimeoutNs;
    this.operationTimeoutNs = builder.operationTimeoutNs;
    this.pauseNs = builder.pauseNs;
    if (builder.pauseNsForServerOverloaded < builder.pauseNs) {
      LOG.warn(
        "Configured value of pauseNsForServerOverloaded is {} ms, which is less than" +
          " the normal pause value {} ms, use the greater one instead",
        TimeUnit.NANOSECONDS.toMillis(builder.pauseNsForServerOverloaded),
        TimeUnit.NANOSECONDS.toMillis(builder.pauseNs));
      this.pauseNsForServerOverloaded = builder.pauseNs;
    } else {
      this.pauseNsForServerOverloaded = builder.pauseNsForServerOverloaded;
    }
    this.maxAttempts = builder.maxAttempts;
    this.startLogErrorsCnt = builder.startLogErrorsCnt;
    this.ng = connection.getNonceGenerator();
  }

  <T> MasterRequestCallerBuilder<T> newMasterCaller() {
    return this.connection.callerFactory.<T> masterRequest()
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
      .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
      .pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForServerOverloaded(pauseNsForServerOverloaded, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt);
  }

  private <T> AdminRequestCallerBuilder<T> newAdminCaller() {
    return this.connection.callerFactory.<T> adminRequest()
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
      .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
      .pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForServerOverloaded(pauseNsForServerOverloaded, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt);
  }

  @FunctionalInterface
  private interface MasterRpcCall<RESP, REQ> {
    void call(MasterService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface AdminRpcCall<RESP, REQ> {
    void call(AdminService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface Converter<D, S> {
    D convert(S src) throws IOException;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> call(HBaseRpcController controller,
      MasterService.Interface stub, PREQ preq, MasterRpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          try {
            future.complete(respConverter.convert(resp));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }
      }
    });
    return future;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> adminCall(HBaseRpcController controller,
      AdminService.Interface stub, PREQ preq, AdminRpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          try {
            future.complete(respConverter.convert(resp));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }
      }
    });
    return future;
  }

  private <PREQ, PRESP> CompletableFuture<Void> procedureCall(PREQ preq,
      MasterRpcCall<PRESP, PREQ> rpcCall, Converter<Long, PRESP> respConverter,
      ProcedureBiConsumer consumer) {
    return procedureCall(b -> {
    }, preq, rpcCall, respConverter, consumer);
  }

  private <PREQ, PRESP> CompletableFuture<Void> procedureCall(TableName tableName, PREQ preq,
      MasterRpcCall<PRESP, PREQ> rpcCall, Converter<Long, PRESP> respConverter,
      ProcedureBiConsumer consumer) {
    return procedureCall(b -> b.priority(tableName), preq, rpcCall, respConverter, consumer);
  }

  private <PREQ, PRESP> CompletableFuture<Void> procedureCall(
      Consumer<MasterRequestCallerBuilder<?>> prioritySetter, PREQ preq,
      MasterRpcCall<PRESP, PREQ> rpcCall, Converter<Long, PRESP> respConverter,
      ProcedureBiConsumer consumer) {
    MasterRequestCallerBuilder<Long> builder = this.<Long> newMasterCaller().action((controller,
        stub) -> this.<PREQ, PRESP, Long> call(controller, stub, preq, rpcCall, respConverter));
    prioritySetter.accept(builder);
    CompletableFuture<Long> procFuture = builder.call();
    CompletableFuture<Void> future = waitProcedureResult(procFuture);
    addListener(future, consumer);
    return future;
  }

  @FunctionalInterface
  private interface TableOperator {
    CompletableFuture<Void> operate(TableName table);
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return CompletableFuture.completedFuture(true);
    }
    return ClientMetaTableAccessor.tableExists(metaTable, tableName);
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables) {
    return getTableDescriptors(RequestConverter.buildGetTableDescriptorsRequest(null,
      includeSysTables));
  }

  /**
   * {@link #listTableDescriptors(boolean)}
   */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(Pattern pattern,
      boolean includeSysTables) {
    Preconditions.checkNotNull(pattern,
      "pattern is null. If you don't specify a pattern, "
          + "use listTableDescriptors(boolean) instead");
    return getTableDescriptors(RequestConverter.buildGetTableDescriptorsRequest(pattern,
      includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(List<TableName> tableNames) {
    Preconditions.checkNotNull(tableNames,
      "tableNames is null. If you don't specify tableNames, "
          + "use listTableDescriptors(boolean) instead");
    if (tableNames.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    return getTableDescriptors(RequestConverter.buildGetTableDescriptorsRequest(tableNames));
  }

  private CompletableFuture<List<TableDescriptor>>
      getTableDescriptors(GetTableDescriptorsRequest request) {
    return this.<List<TableDescriptor>> newMasterCaller()
        .action((controller, stub) -> this
            .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<TableDescriptor>> call(
              controller, stub, request, (s, c, req, done) -> s.getTableDescriptors(c, req, done),
              (resp) -> ProtobufUtil.toTableDescriptorList(resp)))
        .call();
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables) {
    return getTableNames(RequestConverter.buildGetTableNamesRequest(null, includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableName>>
      listTableNames(Pattern pattern, boolean includeSysTables) {
    Preconditions.checkNotNull(pattern,
        "pattern is null. If you don't specify a pattern, use listTableNames(boolean) instead");
    return getTableNames(RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables));
  }

  private CompletableFuture<List<TableName>> getTableNames(GetTableNamesRequest request) {
    return this
        .<List<TableName>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetTableNamesRequest, GetTableNamesResponse, List<TableName>> call(controller,
                stub, request, (s, c, req, done) -> s.getTableNames(c, req, done),
                (resp) -> ProtobufUtil.toTableNameList(resp.getTableNamesList()))).call();
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptorsByNamespace(String name) {
    return this.<List<TableDescriptor>> newMasterCaller().action((controller, stub) -> this
        .<ListTableDescriptorsByNamespaceRequest, ListTableDescriptorsByNamespaceResponse,
        List<TableDescriptor>> call(
          controller, stub,
          ListTableDescriptorsByNamespaceRequest.newBuilder().setNamespaceName(name).build(),
          (s, c, req, done) -> s.listTableDescriptorsByNamespace(c, req, done),
          (resp) -> ProtobufUtil.toTableDescriptorList(resp)))
        .call();
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNamesByNamespace(String name) {
    return this.<List<TableName>> newMasterCaller().action((controller, stub) -> this
        .<ListTableNamesByNamespaceRequest, ListTableNamesByNamespaceResponse,
        List<TableName>> call(
          controller, stub,
          ListTableNamesByNamespaceRequest.newBuilder().setNamespaceName(name).build(),
          (s, c, req, done) -> s.listTableNamesByNamespace(c, req, done),
          (resp) -> ProtobufUtil.toTableNameList(resp.getTableNameList())))
        .call();
  }

  @Override
  public CompletableFuture<TableDescriptor> getDescriptor(TableName tableName) {
    CompletableFuture<TableDescriptor> future = new CompletableFuture<>();
    addListener(this.<List<TableSchema>> newMasterCaller().priority(tableName)
      .action((controller, stub) -> this
        .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<TableSchema>> call(
          controller, stub, RequestConverter.buildGetTableDescriptorsRequest(tableName),
          (s, c, req, done) -> s.getTableDescriptors(c, req, done),
          (resp) -> resp.getTableSchemaList()))
      .call(), (tableSchemas, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        if (!tableSchemas.isEmpty()) {
          future.complete(ProtobufUtil.toTableDescriptor(tableSchemas.get(0)));
        } else {
          future.completeExceptionally(new TableNotFoundException(tableName.getNameAsString()));
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc) {
    return createTable(desc.getTableName(),
      RequestConverter.buildCreateTableRequest(desc, null, ng.getNonceGroup(), ng.newNonce()));
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    try {
      return createTable(desc, getSplitKeys(startKey, endKey, numRegions));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
    Preconditions.checkNotNull(splitKeys, "splitKeys is null. If you don't specify splitKeys,"
        + " use createTable(TableDescriptor) instead");
    try {
      verifySplitKeys(splitKeys);
      return createTable(desc.getTableName(), RequestConverter.buildCreateTableRequest(desc,
        splitKeys, ng.getNonceGroup(), ng.newNonce()));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  private CompletableFuture<Void> createTable(TableName tableName, CreateTableRequest request) {
    Preconditions.checkNotNull(tableName, "table name is null");
    return this.<CreateTableRequest, CreateTableResponse> procedureCall(tableName, request,
      (s, c, req, done) -> s.createTable(c, req, done), (resp) -> resp.getProcId(),
      new CreateTableProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> modifyTable(TableDescriptor desc) {
    return this.<ModifyTableRequest, ModifyTableResponse> procedureCall(desc.getTableName(),
      RequestConverter.buildModifyTableRequest(desc.getTableName(), desc, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.modifyTable(c, req, done),
      (resp) -> resp.getProcId(), new ModifyTableProcedureBiConsumer(this, desc.getTableName()));
  }

  @Override
  public CompletableFuture<Void> modifyTableStoreFileTracker(TableName tableName, String dstSFT) {
    return this
      .<ModifyTableStoreFileTrackerRequest, ModifyTableStoreFileTrackerResponse> procedureCall(
        tableName,
        RequestConverter.buildModifyTableStoreFileTrackerRequest(tableName, dstSFT,
          ng.getNonceGroup(), ng.newNonce()),
        (s, c, req, done) -> s.modifyTableStoreFileTracker(c, req, done),
        (resp) -> resp.getProcId(),
        new ModifyTableStoreFileTrackerProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return this.<DeleteTableRequest, DeleteTableResponse> procedureCall(tableName,
      RequestConverter.buildDeleteTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.deleteTable(c, req, done), (resp) -> resp.getProcId(),
      new DeleteTableProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    return this.<TruncateTableRequest, TruncateTableResponse> procedureCall(tableName,
      RequestConverter.buildTruncateTableRequest(tableName, preserveSplits, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.truncateTable(c, req, done),
      (resp) -> resp.getProcId(), new TruncateTableProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    return this.<EnableTableRequest, EnableTableResponse> procedureCall(tableName,
      RequestConverter.buildEnableTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.enableTable(c, req, done), (resp) -> resp.getProcId(),
      new EnableTableProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    return this.<DisableTableRequest, DisableTableResponse> procedureCall(tableName,
      RequestConverter.buildDisableTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.disableTable(c, req, done), (resp) -> resp.getProcId(),
      new DisableTableProcedureBiConsumer(tableName));
  }

  /**
   * Utility for completing passed TableState {@link CompletableFuture} <code>future</code>
   * using passed parameters. Sets error or boolean result ('true' if table matches
   * the passed-in targetState).
   */
  private static CompletableFuture<Boolean> completeCheckTableState(
      CompletableFuture<Boolean> future, TableState tableState, Throwable error,
      TableState.State targetState, TableName tableName) {
    if (error != null) {
      future.completeExceptionally(error);
    } else {
      if (tableState != null) {
        future.complete(tableState.inStates(targetState));
      } else {
        future.completeExceptionally(new TableNotFoundException(tableName));
      }
    }
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return CompletableFuture.completedFuture(true);
    }
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    addListener(ClientMetaTableAccessor.getTableState(metaTable, tableName),
      (tableState, error) -> {
        completeCheckTableState(future, tableState.isPresent() ? tableState.get() : null, error,
          TableState.State.ENABLED, tableName);
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return CompletableFuture.completedFuture(false);
    }
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    addListener(ClientMetaTableAccessor.getTableState(metaTable, tableName),
      (tableState, error) -> {
        completeCheckTableState(future, tableState.isPresent() ? tableState.get() : null, error,
          TableState.State.DISABLED, tableName);
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return connection.registry.getMetaRegionLocations().thenApply(locs -> Stream
        .of(locs.getRegionLocations()).allMatch(loc -> loc != null && loc.getServerName() != null));
    }
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    addListener(isTableEnabled(tableName), (enabled, error) -> {
      if (error != null) {
        if (error instanceof TableNotFoundException) {
          future.complete(false);
        } else {
          future.completeExceptionally(error);
        }
        return;
      }
      if (!enabled) {
        future.complete(false);
      } else {
        addListener(
          ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName),
          (locations, error1) -> {
            if (error1 != null) {
              future.completeExceptionally(error1);
              return;
            }
            List<HRegionLocation> notDeployedRegions = locations.stream()
              .filter(loc -> loc.getServerName() == null).collect(Collectors.toList());
            if (notDeployedRegions.size() > 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Table " + tableName + " has " + notDeployedRegions.size() + " regions");
              }
              future.complete(false);
              return;
            }
            future.complete(true);
          });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(
      TableName tableName, ColumnFamilyDescriptor columnFamily) {
    return this.<AddColumnRequest, AddColumnResponse> procedureCall(tableName,
      RequestConverter.buildAddColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.addColumn(c, req, done), (resp) -> resp.getProcId(),
      new AddColumnFamilyProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnFamily) {
    return this.<DeleteColumnRequest, DeleteColumnResponse> procedureCall(tableName,
      RequestConverter.buildDeleteColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.deleteColumn(c, req, done),
      (resp) -> resp.getProcId(), new DeleteColumnFamilyProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName tableName,
      ColumnFamilyDescriptor columnFamily) {
    return this.<ModifyColumnRequest, ModifyColumnResponse> procedureCall(tableName,
      RequestConverter.buildModifyColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.modifyColumn(c, req, done),
      (resp) -> resp.getProcId(), new ModifyColumnFamilyProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamilyStoreFileTracker(TableName tableName,
    byte[] family, String dstSFT) {
    return this
      .<ModifyColumnStoreFileTrackerRequest, ModifyColumnStoreFileTrackerResponse> procedureCall(
        tableName,
        RequestConverter.buildModifyColumnStoreFileTrackerRequest(tableName, family, dstSFT,
          ng.getNonceGroup(), ng.newNonce()),
        (s, c, req, done) -> s.modifyColumnStoreFileTracker(c, req, done),
        (resp) -> resp.getProcId(),
        new ModifyColumnFamilyStoreFileTrackerProcedureBiConsumer(tableName));
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor descriptor) {
    return this.<CreateNamespaceRequest, CreateNamespaceResponse> procedureCall(
      RequestConverter.buildCreateNamespaceRequest(descriptor),
      (s, c, req, done) -> s.createNamespace(c, req, done), (resp) -> resp.getProcId(),
      new CreateNamespaceProcedureBiConsumer(descriptor.getName()));
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor descriptor) {
    return this.<ModifyNamespaceRequest, ModifyNamespaceResponse> procedureCall(
      RequestConverter.buildModifyNamespaceRequest(descriptor),
      (s, c, req, done) -> s.modifyNamespace(c, req, done), (resp) -> resp.getProcId(),
      new ModifyNamespaceProcedureBiConsumer(descriptor.getName()));
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String name) {
    return this.<DeleteNamespaceRequest, DeleteNamespaceResponse> procedureCall(
      RequestConverter.buildDeleteNamespaceRequest(name),
      (s, c, req, done) -> s.deleteNamespace(c, req, done), (resp) -> resp.getProcId(),
      new DeleteNamespaceProcedureBiConsumer(name));
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String name) {
    return this
        .<NamespaceDescriptor> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetNamespaceDescriptorRequest, GetNamespaceDescriptorResponse, NamespaceDescriptor>
                  call(controller, stub, RequestConverter.buildGetNamespaceDescriptorRequest(name),
                    (s, c, req, done) -> s.getNamespaceDescriptor(c, req, done), (resp)
                      -> ProtobufUtil.toNamespaceDescriptor(resp.getNamespaceDescriptor()))).call();
  }

  @Override
  public CompletableFuture<List<String>> listNamespaces() {
    return this
        .<List<String>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListNamespacesRequest, ListNamespacesResponse, List<String>> call(
                controller, stub, ListNamespacesRequest.newBuilder().build(), (s, c, req,
                  done) -> s.listNamespaces(c, req, done),
                (resp) -> resp.getNamespaceNameList())).call();
  }

  @Override
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    return this
        .<List<NamespaceDescriptor>> newMasterCaller().action((controller, stub) -> this
              .<ListNamespaceDescriptorsRequest, ListNamespaceDescriptorsResponse,
                  List<NamespaceDescriptor>> call(controller, stub,
                  ListNamespaceDescriptorsRequest.newBuilder().build(), (s, c, req, done) ->
                      s.listNamespaceDescriptors(c, req, done),
                    (resp) -> ProtobufUtil.toNamespaceDescriptorList(resp))).call();
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(ServerName serverName) {
    return this.<List<RegionInfo>> newAdminCaller()
        .action((controller, stub) -> this
            .<GetOnlineRegionRequest, GetOnlineRegionResponse, List<RegionInfo>> adminCall(
              controller, stub, RequestConverter.buildGetOnlineRegionRequest(),
              (s, c, req, done) -> s.getOnlineRegion(c, req, done),
              resp -> ProtobufUtil.getRegionInfos(resp)))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(TableName tableName) {
    if (tableName.equals(META_TABLE_NAME)) {
      return connection.registry.getMetaRegionLocations()
        .thenApply(locs -> Stream.of(locs.getRegionLocations()).map(HRegionLocation::getRegion)
          .collect(Collectors.toList()));
    } else {
      return ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName)
        .thenApply(
          locs -> locs.stream().map(HRegionLocation::getRegion).collect(Collectors.toList()));
    }
  }
  @Override
  public CompletableFuture<Void> flush(TableName tableName) {
    return flush(tableName, null);
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName, byte[] columnFamily) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (!exists) {
        future.completeExceptionally(new TableNotFoundException(tableName));
      } else {
        addListener(isTableEnabled(tableName), (tableEnabled, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else if (!tableEnabled) {
            future.completeExceptionally(new TableNotEnabledException(tableName));
          } else {
            Map<String, String> props = new HashMap<>();
            if (columnFamily != null) {
              props.put(HConstants.FAMILY_KEY_STR, Bytes.toString(columnFamily));
            }
            addListener(execProcedure(FLUSH_TABLE_PROCEDURE_SIGNATURE, tableName.getNameAsString(),
              props), (ret, err3) -> {
                if (err3 != null) {
                  future.completeExceptionally(err3);
                } else {
                  future.complete(ret);
                }
              });
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName) {
    return flushRegionInternal(regionName, null, false).thenAccept(r -> {
    });
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName, byte[] columnFamily) {
    Preconditions.checkNotNull(columnFamily, "columnFamily is null."
      + "If you don't specify a columnFamily, use flushRegion(regionName) instead");
    return flushRegionInternal(regionName, columnFamily, false)
      .thenAccept(r -> {});
  }

  /**
   * This method is for internal use only, where we need the response of the flush.
   * <p/>
   * As it exposes the protobuf message, please do <strong>NOT</strong> try to expose it as a public
   * API.
   */
  CompletableFuture<FlushRegionResponse> flushRegionInternal(byte[] regionName,
    byte[] columnFamily, boolean writeFlushWALMarker) {
    CompletableFuture<FlushRegionResponse> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      ServerName serverName = location.getServerName();
      if (serverName == null) {
        future
          .completeExceptionally(new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      addListener(
        flush(serverName, location.getRegion(), columnFamily, writeFlushWALMarker),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }});
    });
    return future;
  }

  private CompletableFuture<FlushRegionResponse> flush(ServerName serverName, RegionInfo regionInfo,
    byte[] columnFamily, boolean writeFlushWALMarker) {
    return this.<FlushRegionResponse> newAdminCaller().serverName(serverName)
      .action((controller, stub) -> this
        .<FlushRegionRequest, FlushRegionResponse, FlushRegionResponse> adminCall(controller, stub,
          RequestConverter.buildFlushRegionRequest(regionInfo.getRegionName(),
            columnFamily, writeFlushWALMarker),
          (s, c, req, done) -> s.flushRegion(c, req, done), resp -> resp))
      .call();
  }

  @Override
  public CompletableFuture<Void> flushRegionServer(ServerName sn) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegions(sn), (hRegionInfos, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      if (hRegionInfos != null) {
        hRegionInfos.forEach(
          region -> compactFutures.add(
            flush(sn, region, null, false).thenAccept(r -> {})
          )
        );
      }
      addListener(CompletableFuture.allOf(
        compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()])), (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName, CompactType compactType) {
    return compact(tableName, null, false, compactType);
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily,
      CompactType compactType) {
    Preconditions.checkNotNull(columnFamily, "columnFamily is null. "
        + "If you don't specify a columnFamily, use compact(TableName) instead");
    return compact(tableName, columnFamily, false, compactType);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName) {
    return compactRegion(regionName, null, false);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName, byte[] columnFamily) {
    Preconditions.checkNotNull(columnFamily, "columnFamily is null."
        + " If you don't specify a columnFamily, use compactRegion(regionName) instead");
    return compactRegion(regionName, columnFamily, false);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, CompactType compactType) {
    return compact(tableName, null, true, compactType);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily,
      CompactType compactType) {
    Preconditions.checkNotNull(columnFamily, "columnFamily is null."
        + "If you don't specify a columnFamily, use compact(TableName) instead");
    return compact(tableName, columnFamily, true, compactType);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName) {
    return compactRegion(regionName, null, true);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName, byte[] columnFamily) {
    Preconditions.checkNotNull(columnFamily, "columnFamily is null."
        + " If you don't specify a columnFamily, use majorCompactRegion(regionName) instead");
    return compactRegion(regionName, columnFamily, true);
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName sn) {
    return compactRegionServer(sn, false);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName sn) {
    return compactRegionServer(sn, true);
  }

  private CompletableFuture<Void> compactRegionServer(ServerName sn, boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegions(sn), (hRegionInfos, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      if (hRegionInfos != null) {
        hRegionInfos.forEach(region -> compactFutures.add(compact(sn, region, major, null)));
      }
      addListener(CompletableFuture.allOf(
        compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()])), (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  private CompletableFuture<Void> compactRegion(byte[] regionName, byte[] columnFamily,
      boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      ServerName serverName = location.getServerName();
      if (serverName == null) {
        future
          .completeExceptionally(new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      addListener(compact(location.getServerName(), location.getRegion(), major, columnFamily),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  /**
   * List all region locations for the specific table.
   */
  private CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(TableName tableName) {
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
      addListener(connection.registry.getMetaRegionLocations(), (metaRegions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (metaRegions == null || metaRegions.isEmpty() ||
          metaRegions.getDefaultRegionLocation() == null) {
          future.completeExceptionally(new IOException("meta region does not found"));
        } else {
          future.complete(Collections.singletonList(metaRegions.getDefaultRegionLocation()));
        }
      });
      return future;
    } else {
      // For non-meta table, we fetch all locations by scanning hbase:meta table
      return ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName);
    }
  }

  /**
   * Compact column family of a table, Asynchronous operation even if CompletableFuture.get()
   */
  private CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily, boolean major,
      CompactType compactType) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    switch (compactType) {
      case MOB:
        addListener(connection.registry.getActiveMaster(), (serverName, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          RegionInfo regionInfo = RegionInfo.createMobRegionInfo(tableName);
          addListener(compact(serverName, regionInfo, major, columnFamily), (ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
        });
        break;
      case NORMAL:
        addListener(getTableHRegionLocations(tableName), (locations, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          if (locations == null || locations.isEmpty()) {
            future.completeExceptionally(new TableNotFoundException(tableName));
          }
          CompletableFuture<?>[] compactFutures =
            locations.stream().filter(l -> l.getRegion() != null)
              .filter(l -> !l.getRegion().isOffline()).filter(l -> l.getServerName() != null)
              .map(l -> compact(l.getServerName(), l.getRegion(), major, columnFamily))
              .toArray(CompletableFuture<?>[]::new);
          // future complete unless all of the compact futures are completed.
          addListener(CompletableFuture.allOf(compactFutures), (ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
        });
        break;
      default:
        throw new IllegalArgumentException("Unknown compactType: " + compactType);
    }
    return future;
  }

  /**
   * Compact the region at specific region server.
   */
  private CompletableFuture<Void> compact(final ServerName sn, final RegionInfo hri,
      final boolean major, byte[] columnFamily) {
    return this
        .<Void> newAdminCaller()
        .serverName(sn)
        .action(
          (controller, stub) -> this.<CompactRegionRequest, CompactRegionResponse, Void> adminCall(
            controller, stub, RequestConverter.buildCompactRegionRequest(hri.getRegionName(),
              major, columnFamily), (s, c, req, done) -> s.compactRegion(c, req, done),
            resp -> null)).call();
  }

  private byte[] toEncodeRegionName(byte[] regionName) {
    return RegionInfo.isEncodedRegionName(regionName) ? regionName :
      Bytes.toBytes(RegionInfo.encodeRegionName(regionName));
  }

  private void checkAndGetTableName(byte[] encodeRegionName, AtomicReference<TableName> tableName,
      CompletableFuture<TableName> result) {
    addListener(getRegionLocation(encodeRegionName), (location, err) -> {
      if (err != null) {
        result.completeExceptionally(err);
        return;
      }
      RegionInfo regionInfo = location.getRegion();
      if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        result.completeExceptionally(
          new IllegalArgumentException("Can't invoke merge on non-default regions directly"));
        return;
      }
      if (!tableName.compareAndSet(null, regionInfo.getTable())) {
        if (!tableName.get().equals(regionInfo.getTable())) {
          // tables of this two region should be same.
          result.completeExceptionally(
            new IllegalArgumentException("Cannot merge regions from two different tables " +
              tableName.get() + " and " + regionInfo.getTable()));
        } else {
          result.complete(tableName.get());
        }
      }
    });
  }

  private CompletableFuture<TableName> checkRegionsAndGetTableName(byte[][] encodedRegionNames) {
    AtomicReference<TableName> tableNameRef = new AtomicReference<>();
    CompletableFuture<TableName> future = new CompletableFuture<>();
    for (byte[] encodedRegionName : encodedRegionNames) {
      checkAndGetTableName(encodedRegionName, tableNameRef, future);
    }
    return future;
  }

  @Override
  public CompletableFuture<Boolean> mergeSwitch(boolean enabled, boolean drainMerges) {
    return setSplitOrMergeOn(enabled, drainMerges, MasterSwitchType.MERGE);
  }

  @Override
  public CompletableFuture<Boolean> isMergeEnabled() {
    return isSplitOrMergeOn(MasterSwitchType.MERGE);
  }

  @Override
  public CompletableFuture<Boolean> splitSwitch(boolean enabled, boolean drainSplits) {
    return setSplitOrMergeOn(enabled, drainSplits, MasterSwitchType.SPLIT);
  }

  @Override
  public CompletableFuture<Boolean> isSplitEnabled() {
    return isSplitOrMergeOn(MasterSwitchType.SPLIT);
  }

  private CompletableFuture<Boolean> setSplitOrMergeOn(boolean enabled, boolean synchronous,
      MasterSwitchType switchType) {
    SetSplitOrMergeEnabledRequest request =
      RequestConverter.buildSetSplitOrMergeEnabledRequest(enabled, synchronous, switchType);
    return this.<Boolean> newMasterCaller()
      .action((controller, stub) -> this
        .<SetSplitOrMergeEnabledRequest, SetSplitOrMergeEnabledResponse, Boolean> call(controller,
          stub, request, (s, c, req, done) -> s.setSplitOrMergeEnabled(c, req, done),
          (resp) -> resp.getPrevValueList().get(0)))
      .call();
  }

  private CompletableFuture<Boolean> isSplitOrMergeOn(MasterSwitchType switchType) {
    IsSplitOrMergeEnabledRequest request =
        RequestConverter.buildIsSplitOrMergeEnabledRequest(switchType);
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsSplitOrMergeEnabledRequest, IsSplitOrMergeEnabledResponse, Boolean> call(
                controller, stub, request,
                (s, c, req, done) -> s.isSplitOrMergeEnabled(c, req, done),
                (resp) -> resp.getEnabled())).call();
  }

  @Override
  public CompletableFuture<Void> mergeRegions(List<byte[]> nameOfRegionsToMerge, boolean forcible) {
    if (nameOfRegionsToMerge.size() < 2) {
      return failedFuture(new IllegalArgumentException(
        "Can not merge only " + nameOfRegionsToMerge.size() + " region"));
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    byte[][] encodedNameOfRegionsToMerge =
      nameOfRegionsToMerge.stream().map(this::toEncodeRegionName).toArray(byte[][]::new);

    addListener(checkRegionsAndGetTableName(encodedNameOfRegionsToMerge), (tableName, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }

      final MergeTableRegionsRequest request;
      try {
        request = RequestConverter.buildMergeTableRegionsRequest(encodedNameOfRegionsToMerge,
          forcible, ng.getNonceGroup(), ng.newNonce());
      } catch (DeserializationException e) {
        future.completeExceptionally(e);
        return;
      }

      addListener(
        this.procedureCall(tableName, request,
          MasterService.Interface::mergeTableRegions, MergeTableRegionsResponse::getProcId,
          new MergeTableRegionProcedureBiConsumer(tableName)),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exist, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (!exist) {
        future.completeExceptionally(new TableNotFoundException(tableName));
        return;
      }
      addListener(metaTable
        .scanAll(new Scan().setReadType(ReadType.PREAD).addFamily(HConstants.CATALOG_FAMILY)
          .withStartRow(ClientMetaTableAccessor.getTableStartRowForMeta(tableName,
            ClientMetaTableAccessor.QueryType.REGION))
          .withStopRow(ClientMetaTableAccessor.getTableStopRowForMeta(tableName,
            ClientMetaTableAccessor.QueryType.REGION))),
        (results, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
            return;
          }
          if (results != null && !results.isEmpty()) {
            List<CompletableFuture<Void>> splitFutures = new ArrayList<>();
            for (Result r : results) {
              if (r.isEmpty() || CatalogFamilyFormat.getRegionInfo(r) == null) {
                continue;
              }
              RegionLocations rl = CatalogFamilyFormat.getRegionLocations(r);
              if (rl != null) {
                for (HRegionLocation h : rl.getRegionLocations()) {
                  if (h != null && h.getServerName() != null) {
                    RegionInfo hri = h.getRegion();
                    if (hri == null || hri.isSplitParent() ||
                      hri.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
                      continue;
                    }
                    splitFutures.add(split(hri, null));
                  }
                }
              }
            }
            addListener(
              CompletableFuture
                .allOf(splitFutures.toArray(new CompletableFuture<?>[splitFutures.size()])),
              (ret, exception) -> {
                if (exception != null) {
                  future.completeExceptionally(exception);
                  return;
                }
                future.complete(ret);
              });
          } else {
            future.complete(null);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName, byte[] splitPoint) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (splitPoint == null) {
      return failedFuture(new IllegalArgumentException("splitPoint can not be null."));
    }
    addListener(connection.getRegionLocator(tableName).getRegionLocation(splitPoint, true),
      (loc, err) -> {
        if (err != null) {
          result.completeExceptionally(err);
        } else if (loc == null || loc.getRegion() == null) {
          result.completeExceptionally(new IllegalArgumentException(
            "Region does not found: rowKey=" + Bytes.toStringBinary(splitPoint)));
        } else {
          addListener(splitRegion(loc.getRegion().getRegionName(), splitPoint), (ret, err2) -> {
            if (err2 != null) {
              result.completeExceptionally(err2);
            } else {
              result.complete(ret);
            }

          });
        }
      });
    return result;
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      RegionInfo regionInfo = location.getRegion();
      if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        future
          .completeExceptionally(new IllegalArgumentException("Can't split replicas directly. " +
            "Replicas are auto-split when their primary is split."));
        return;
      }
      ServerName serverName = location.getServerName();
      if (serverName == null) {
        future
          .completeExceptionally(new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      addListener(split(regionInfo, null), (ret, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(ret);
        }
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName, byte[] splitPoint) {
    Preconditions.checkNotNull(splitPoint,
      "splitPoint is null. If you don't specify a splitPoint, use splitRegion(byte[]) instead");
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      RegionInfo regionInfo = location.getRegion();
      if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        future
          .completeExceptionally(new IllegalArgumentException("Can't split replicas directly. " +
            "Replicas are auto-split when their primary is split."));
        return;
      }
      ServerName serverName = location.getServerName();
      if (serverName == null) {
        future
          .completeExceptionally(new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      if (regionInfo.getStartKey() != null &&
        Bytes.compareTo(regionInfo.getStartKey(), splitPoint) == 0) {
        future.completeExceptionally(
          new IllegalArgumentException("should not give a splitkey which equals to startkey!"));
        return;
      }
      addListener(split(regionInfo, splitPoint), (ret, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(ret);
        }
      });
    });
    return future;
  }

  private CompletableFuture<Void> split(final RegionInfo hri, byte[] splitPoint) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    TableName tableName = hri.getTable();
    final SplitTableRegionRequest request;
    try {
      request = RequestConverter.buildSplitTableRegionRequest(hri, splitPoint, ng.getNonceGroup(),
        ng.newNonce());
    } catch (DeserializationException e) {
      future.completeExceptionally(e);
      return future;
    }

    addListener(
      this.procedureCall(tableName,
        request, MasterService.Interface::splitRegion, SplitTableRegionResponse::getProcId,
        new SplitTableRegionProcedureBiConsumer(tableName)),
      (ret, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(ret);
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> assign(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionInfo(regionName), (regionInfo, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(this.<Void> newMasterCaller().priority(regionInfo.getTable())
        .action(((controller, stub) -> this.<AssignRegionRequest, AssignRegionResponse, Void> call(
          controller, stub, RequestConverter.buildAssignRegionRequest(regionInfo.getRegionName()),
          (s, c, req, done) -> s.assignRegion(c, req, done), resp -> null)))
        .call(), (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionInfo(regionName), (regionInfo, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(
        this.<Void> newMasterCaller().priority(regionInfo.getTable())
          .action(((controller, stub) -> this
            .<UnassignRegionRequest, UnassignRegionResponse, Void> call(controller, stub,
              RequestConverter.buildUnassignRegionRequest(regionInfo.getRegionName()),
              (s, c, req, done) -> s.unassignRegion(c, req, done), resp -> null)))
          .call(),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> offline(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionInfo(regionName), (regionInfo, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(
        this.<Void> newMasterCaller().priority(regionInfo.getTable())
          .action(((controller, stub) -> this
            .<OfflineRegionRequest, OfflineRegionResponse, Void> call(controller, stub,
              RequestConverter.buildOfflineRegionRequest(regionInfo.getRegionName()),
              (s, c, req, done) -> s.offlineRegion(c, req, done), resp -> null)))
          .call(),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionInfo(regionName), (regionInfo, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(
        moveRegion(regionInfo,
          RequestConverter.buildMoveRegionRequest(regionInfo.getEncodedNameAsBytes(), null)),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName, ServerName destServerName) {
    Preconditions.checkNotNull(destServerName,
      "destServerName is null. If you don't specify a destServerName, use move(byte[]) instead");
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getRegionInfo(regionName), (regionInfo, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(
        moveRegion(regionInfo, RequestConverter
          .buildMoveRegionRequest(regionInfo.getEncodedNameAsBytes(), destServerName)),
        (ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
    });
    return future;
  }

  private CompletableFuture<Void> moveRegion(RegionInfo regionInfo, MoveRegionRequest request) {
    return this.<Void> newMasterCaller().priority(regionInfo.getTable())
      .action(
        (controller, stub) -> this.<MoveRegionRequest, MoveRegionResponse, Void> call(controller,
          stub, request, (s, c, req, done) -> s.moveRegion(c, req, done), resp -> null))
      .call();
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings quota) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<SetQuotaRequest, SetQuotaResponse, Void> call(controller,
            stub, QuotaSettings.buildSetQuotaRequestProto(quota),
            (s, c, req, done) -> s.setQuota(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter filter) {
    CompletableFuture<List<QuotaSettings>> future = new CompletableFuture<>();
    Scan scan = QuotaTableUtil.makeScan(filter);
    this.connection.getTableBuilder(QuotaTableUtil.QUOTA_TABLE_NAME).build()
        .scan(scan, new AdvancedScanResultConsumer() {
          List<QuotaSettings> settings = new ArrayList<>();

          @Override
          public void onNext(Result[] results, ScanController controller) {
            for (Result result : results) {
              try {
                QuotaTableUtil.parseResultToCollection(result, settings);
              } catch (IOException e) {
                controller.terminate();
                future.completeExceptionally(e);
              }
            }
          }

          @Override
          public void onError(Throwable error) {
            future.completeExceptionally(error);
          }

          @Override
          public void onComplete() {
            future.complete(settings);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig, boolean enabled) {
    return this.<AddReplicationPeerRequest, AddReplicationPeerResponse> procedureCall(
      RequestConverter.buildAddReplicationPeerRequest(peerId, peerConfig, enabled),
      (s, c, req, done) -> s.addReplicationPeer(c, req, done), (resp) -> resp.getProcId(),
      new ReplicationProcedureBiConsumer(peerId, () -> "ADD_REPLICATION_PEER"));
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String peerId) {
    return this.<RemoveReplicationPeerRequest, RemoveReplicationPeerResponse> procedureCall(
      RequestConverter.buildRemoveReplicationPeerRequest(peerId),
      (s, c, req, done) -> s.removeReplicationPeer(c, req, done), (resp) -> resp.getProcId(),
      new ReplicationProcedureBiConsumer(peerId, () -> "REMOVE_REPLICATION_PEER"));
  }

  @Override
  public CompletableFuture<Void> enableReplicationPeer(String peerId) {
    return this.<EnableReplicationPeerRequest, EnableReplicationPeerResponse> procedureCall(
      RequestConverter.buildEnableReplicationPeerRequest(peerId),
      (s, c, req, done) -> s.enableReplicationPeer(c, req, done), (resp) -> resp.getProcId(),
      new ReplicationProcedureBiConsumer(peerId, () -> "ENABLE_REPLICATION_PEER"));
  }

  @Override
  public CompletableFuture<Void> disableReplicationPeer(String peerId) {
    return this.<DisableReplicationPeerRequest, DisableReplicationPeerResponse> procedureCall(
      RequestConverter.buildDisableReplicationPeerRequest(peerId),
      (s, c, req, done) -> s.disableReplicationPeer(c, req, done), (resp) -> resp.getProcId(),
      new ReplicationProcedureBiConsumer(peerId, () -> "DISABLE_REPLICATION_PEER"));
  }

  @Override
  public CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String peerId) {
    return this.<ReplicationPeerConfig> newMasterCaller().action((controller, stub) -> this
      .<GetReplicationPeerConfigRequest, GetReplicationPeerConfigResponse, ReplicationPeerConfig>
          call(controller, stub, RequestConverter.buildGetReplicationPeerConfigRequest(peerId),
            (s, c, req, done) -> s.getReplicationPeerConfig(c, req, done),
            (resp) -> ReplicationPeerConfigUtil.convert(resp.getPeerConfig()))).call();
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(String peerId,
      ReplicationPeerConfig peerConfig) {
    return this
      .<UpdateReplicationPeerConfigRequest, UpdateReplicationPeerConfigResponse> procedureCall(
        RequestConverter.buildUpdateReplicationPeerConfigRequest(peerId, peerConfig),
        (s, c, req, done) -> s.updateReplicationPeerConfig(c, req, done),
        (resp) -> resp.getProcId(),
        new ReplicationProcedureBiConsumer(peerId, () -> "UPDATE_REPLICATION_PEER_CONFIG"));
  }

  @Override
  public CompletableFuture<Void> transitReplicationPeerSyncReplicationState(String peerId,
      SyncReplicationState clusterState) {
    return this.<TransitReplicationPeerSyncReplicationStateRequest,
        TransitReplicationPeerSyncReplicationStateResponse> procedureCall(
        RequestConverter.buildTransitReplicationPeerSyncReplicationStateRequest(peerId,
          clusterState),
          (s, c, req, done) -> s.transitReplicationPeerSyncReplicationState(c, req, done),
          (resp) -> resp.getProcId(), new ReplicationProcedureBiConsumer(peerId,
            () -> "TRANSIT_REPLICATION_PEER_SYNCHRONOUS_REPLICATION_STATE"));
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String id,
      Map<TableName, List<String>> tableCfs) {
    if (tableCfs == null) {
      return failedFuture(new ReplicationException("tableCfs is null"));
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    addListener(getReplicationPeerConfig(id), (peerConfig, error) -> {
      if (!completeExceptionally(future, error)) {
        ReplicationPeerConfig newPeerConfig =
          ReplicationPeerConfigUtil.appendTableCFsToReplicationPeerConfig(tableCfs, peerConfig);
        addListener(updateReplicationPeerConfig(id, newPeerConfig), (result, err) -> {
          if (!completeExceptionally(future, error)) {
            future.complete(result);
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String id,
      Map<TableName, List<String>> tableCfs) {
    if (tableCfs == null) {
      return failedFuture(new ReplicationException("tableCfs is null"));
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    addListener(getReplicationPeerConfig(id), (peerConfig, error) -> {
      if (!completeExceptionally(future, error)) {
        ReplicationPeerConfig newPeerConfig = null;
        try {
          newPeerConfig = ReplicationPeerConfigUtil
            .removeTableCFsFromReplicationPeerConfig(tableCfs, peerConfig, id);
        } catch (ReplicationException e) {
          future.completeExceptionally(e);
          return;
        }
        addListener(updateReplicationPeerConfig(id, newPeerConfig), (result, err) -> {
          if (!completeExceptionally(future, error)) {
            future.complete(result);
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    return listReplicationPeers(RequestConverter.buildListReplicationPeersRequest(null));
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern pattern) {
    Preconditions.checkNotNull(pattern,
      "pattern is null. If you don't specify a pattern, use listReplicationPeers() instead");
    return listReplicationPeers(RequestConverter.buildListReplicationPeersRequest(pattern));
  }

  private CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(
      ListReplicationPeersRequest request) {
    return this
        .<List<ReplicationPeerDescription>> newMasterCaller()
        .action(
          (controller, stub) -> this.<ListReplicationPeersRequest, ListReplicationPeersResponse,
              List<ReplicationPeerDescription>> call(controller, stub, request,
                (s, c, req, done) -> s.listReplicationPeers(c, req, done),
                (resp) -> resp.getPeerDescList().stream()
                    .map(ReplicationPeerConfigUtil::toReplicationPeerDescription)
                    .collect(Collectors.toList()))).call();
  }

  @Override
  public CompletableFuture<List<TableCFs>> listReplicatedTableCFs() {
    CompletableFuture<List<TableCFs>> future = new CompletableFuture<List<TableCFs>>();
    addListener(listTableDescriptors(), (tables, error) -> {
      if (!completeExceptionally(future, error)) {
        List<TableCFs> replicatedTableCFs = new ArrayList<>();
        tables.forEach(table -> {
          Map<String, Integer> cfs = new HashMap<>();
          Stream.of(table.getColumnFamilies())
            .filter(column -> column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL)
            .forEach(column -> {
              cfs.put(column.getNameAsString(), column.getScope());
            });
          if (!cfs.isEmpty()) {
            replicatedTableCFs.add(new TableCFs(table.getTableName(), cfs));
          }
        });
        future.complete(replicatedTableCFs);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription snapshot =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final SnapshotRequest request =
      SnapshotRequest.newBuilder().setSnapshot(snapshot).setNonceGroup(ng.getNonceGroup())
        .setNonce(ng.newNonce()).build();
    addListener(this.<SnapshotResponse> newMasterCaller()
      .action((controller, stub) ->
        this.<SnapshotRequest, SnapshotResponse, SnapshotResponse> call(controller, stub,
          request, (s, c, req, done) -> s.snapshot(c, req, done), resp -> resp))
      .call(), (resp, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        waitSnapshotFinish(snapshotDesc, future, resp);
      });
    return future;
  }

  // This is for keeping compatibility with old implementation.
  // If there is a procId field in the response, then the snapshot will be operated with a
  // SnapshotProcedure, otherwise the snapshot will be coordinated by zk.
  private void waitSnapshotFinish(SnapshotDescription snapshot,
      CompletableFuture<Void> future, SnapshotResponse resp) {
    if (resp.hasProcId()) {
      getProcedureResult(resp.getProcId(), future, 0);
      addListener(future, new SnapshotProcedureBiConsumer(snapshot.getTableName()));
    } else {
      long expectedTimeout = resp.getExpectedTimeout();
      TimerTask pollingTask = new TimerTask() {
        int tries = 0;
        long startTime = EnvironmentEdgeManager.currentTime();
        long endTime = startTime + expectedTimeout;
        long maxPauseTime = expectedTimeout / maxAttempts;

        @Override
        public void run(Timeout timeout) throws Exception {
          if (EnvironmentEdgeManager.currentTime() < endTime) {
            addListener(isSnapshotFinished(snapshot), (done, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else if (done) {
                future.complete(null);
              } else {
                // retry again after pauseTime.
                long pauseTime = ConnectionUtils
                  .getPauseTime(TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                pauseTime = Math.min(pauseTime, maxPauseTime);
                AsyncConnectionImpl.RETRY_TIMER
                  .newTimeout(this, pauseTime, TimeUnit.MILLISECONDS);
              }
            });
          } else {
            future.completeExceptionally(new SnapshotCreationException(
              "Snapshot '" + snapshot.getName() + "' wasn't completed in expectedTime:"
                + expectedTimeout + " ms", snapshot));
          }
        }
      };
      AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<IsSnapshotDoneRequest, IsSnapshotDoneResponse, Boolean> call(
            controller,
            stub,
            IsSnapshotDoneRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.isSnapshotDone(c, req, done), resp -> resp.getDone())).call();
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    boolean takeFailSafeSnapshot = this.connection.getConfiguration().getBoolean(
      HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
      HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
    return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot,
      boolean restoreAcl) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(listSnapshots(Pattern.compile(snapshotName)), (snapshotDescriptions, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      TableName tableName = null;
      if (snapshotDescriptions != null && !snapshotDescriptions.isEmpty()) {
        for (SnapshotDescription snap : snapshotDescriptions) {
          if (snap.getName().equals(snapshotName)) {
            tableName = snap.getTableName();
            break;
          }
        }
      }
      if (tableName == null) {
        future.completeExceptionally(new RestoreSnapshotException(
          "Unable to find the table name for snapshot=" + snapshotName));
        return;
      }
      final TableName finalTableName = tableName;
      addListener(tableExists(finalTableName), (exists, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else if (!exists) {
          // if table does not exist, then just clone snapshot into new table.
          completeConditionalOnFuture(future,
            internalRestoreSnapshot(snapshotName, finalTableName, restoreAcl, null));
        } else {
          addListener(isTableDisabled(finalTableName), (disabled, err4) -> {
            if (err4 != null) {
              future.completeExceptionally(err4);
            } else if (!disabled) {
              future.completeExceptionally(new TableNotDisabledException(finalTableName));
            } else {
              completeConditionalOnFuture(future,
                restoreSnapshot(snapshotName, finalTableName, takeFailSafeSnapshot, restoreAcl));
            }
          });
        }
      });
    });
    return future;
  }

  private CompletableFuture<Void> restoreSnapshot(String snapshotName, TableName tableName,
      boolean takeFailSafeSnapshot, boolean restoreAcl) {
    if (takeFailSafeSnapshot) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      // Step.1 Take a snapshot of the current state
      String failSafeSnapshotSnapshotNameFormat =
        this.connection.getConfiguration().get(HConstants.SNAPSHOT_RESTORE_FAILSAFE_NAME,
          HConstants.DEFAULT_SNAPSHOT_RESTORE_FAILSAFE_NAME);
      final String failSafeSnapshotSnapshotName =
        failSafeSnapshotSnapshotNameFormat.replace("{snapshot.name}", snapshotName)
          .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
          .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTime()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      addListener(snapshot(failSafeSnapshotSnapshotName, tableName), (ret, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          // Step.2 Restore snapshot
          addListener(internalRestoreSnapshot(snapshotName, tableName, restoreAcl, null),
            (void2, err2) -> {
              if (err2 != null) {
                // Step.3.a Something went wrong during the restore and try to rollback.
                addListener(
                  internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName, restoreAcl,
                    null),
                  (void3, err3) -> {
                    if (err3 != null) {
                      future.completeExceptionally(err3);
                    } else {
                      String msg =
                        "Restore snapshot=" + snapshotName + " failed. Rollback to snapshot=" +
                          failSafeSnapshotSnapshotName + " succeeded.";
                      future.completeExceptionally(new RestoreSnapshotException(msg, err2));
                    }
                  });
              } else {
                // Step.3.b If the restore is succeeded, delete the pre-restore snapshot.
                LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
                addListener(deleteSnapshot(failSafeSnapshotSnapshotName), (ret3, err3) -> {
                  if (err3 != null) {
                    LOG.error(
                      "Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName,
                      err3);
                  }
                  future.complete(ret3);
                });
              }
            });
        }
      });
      return future;
    } else {
      return internalRestoreSnapshot(snapshotName, tableName, restoreAcl, null);
    }
  }

  private <T> void completeConditionalOnFuture(CompletableFuture<T> dependentFuture,
      CompletableFuture<T> parentFuture) {
    addListener(parentFuture, (res, err) -> {
      if (err != null) {
        dependentFuture.completeExceptionally(err);
      } else {
        dependentFuture.complete(res);
      }
    });
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName,
      boolean restoreAcl, String customSFT) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (exists) {
        future.completeExceptionally(new TableExistsException(tableName));
      } else {
        completeConditionalOnFuture(future,
          internalRestoreSnapshot(snapshotName, tableName, restoreAcl, customSFT));
      }
    });
    return future;
  }

  private CompletableFuture<Void> internalRestoreSnapshot(String snapshotName, TableName tableName,
      boolean restoreAcl, String customSFT) {
    SnapshotProtos.SnapshotDescription snapshot = SnapshotProtos.SnapshotDescription.newBuilder()
      .setName(snapshotName).setTable(tableName.getNameAsString()).build();
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    RestoreSnapshotRequest.Builder builder =
      RestoreSnapshotRequest.newBuilder().setSnapshot(snapshot).setNonceGroup(ng.getNonceGroup())
        .setNonce(ng.newNonce()).setRestoreACL(restoreAcl);
    if(customSFT != null){
      builder.setCustomSFT(customSFT);
    }
    return waitProcedureResult(this.<Long> newMasterCaller().action((controller, stub) -> this
      .<RestoreSnapshotRequest, RestoreSnapshotResponse, Long> call(controller, stub,
        builder.build(),
        (s, c, req, done) -> s.restoreSnapshot(c, req, done), (resp) -> resp.getProcId()))
      .call());
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return getCompletedSnapshots(null);
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern) {
    Preconditions.checkNotNull(pattern,
      "pattern is null. If you don't specify a pattern, use listSnapshots() instead");
    return getCompletedSnapshots(pattern);
  }

  private CompletableFuture<List<SnapshotDescription>> getCompletedSnapshots(Pattern pattern) {
    return this.<List<SnapshotDescription>> newMasterCaller().action((controller, stub) -> this
        .<GetCompletedSnapshotsRequest, GetCompletedSnapshotsResponse, List<SnapshotDescription>>
        call(controller, stub, GetCompletedSnapshotsRequest.newBuilder().build(),
          (s, c, req, done) -> s.getCompletedSnapshots(c, req, done),
          resp -> ProtobufUtil.toSnapshotDescriptionList(resp, pattern)))
        .call();
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern) {
    Preconditions.checkNotNull(tableNamePattern, "tableNamePattern is null."
        + " If you don't specify a tableNamePattern, use listSnapshots() instead");
    return getCompletedSnapshots(tableNamePattern, null);
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    Preconditions.checkNotNull(tableNamePattern, "tableNamePattern is null."
        + " If you don't specify a tableNamePattern, use listSnapshots(Pattern) instead");
    Preconditions.checkNotNull(snapshotNamePattern, "snapshotNamePattern is null."
        + " If you don't specify a snapshotNamePattern, use listTableSnapshots(Pattern) instead");
    return getCompletedSnapshots(tableNamePattern, snapshotNamePattern);
  }

  private CompletableFuture<List<SnapshotDescription>> getCompletedSnapshots(
      Pattern tableNamePattern, Pattern snapshotNamePattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    addListener(listTableNames(tableNamePattern, false), (tableNames, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (tableNames == null || tableNames.size() <= 0) {
        future.complete(Collections.emptyList());
        return;
      }
      addListener(getCompletedSnapshots(snapshotNamePattern), (snapshotDescList, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
          return;
        }
        if (snapshotDescList == null || snapshotDescList.isEmpty()) {
          future.complete(Collections.emptyList());
          return;
        }
        future.complete(snapshotDescList.stream()
          .filter(snap -> (snap != null && tableNames.contains(snap.getTableName())))
          .collect(Collectors.toList()));
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotName) {
    return internalDeleteSnapshot(new SnapshotDescription(snapshotName));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots() {
    return internalDeleteSnapshots(null, null);
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern snapshotNamePattern) {
    Preconditions.checkNotNull(snapshotNamePattern, "snapshotNamePattern is null."
        + " If you don't specify a snapshotNamePattern, use deleteSnapshots() instead");
    return internalDeleteSnapshots(null, snapshotNamePattern);
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern) {
    Preconditions.checkNotNull(tableNamePattern, "tableNamePattern is null."
        + " If you don't specify a tableNamePattern, use deleteSnapshots() instead");
    return internalDeleteSnapshots(tableNamePattern, null);
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    Preconditions.checkNotNull(tableNamePattern, "tableNamePattern is null."
        + " If you don't specify a tableNamePattern, use deleteSnapshots(Pattern) instead");
    Preconditions.checkNotNull(snapshotNamePattern, "snapshotNamePattern is null."
        + " If you don't specify a snapshotNamePattern, use deleteSnapshots(Pattern) instead");
    return internalDeleteSnapshots(tableNamePattern, snapshotNamePattern);
  }

  private CompletableFuture<Void> internalDeleteSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<List<SnapshotDescription>> listSnapshotsFuture;
    if (tableNamePattern == null) {
      listSnapshotsFuture = getCompletedSnapshots(snapshotNamePattern);
    } else {
      listSnapshotsFuture = getCompletedSnapshots(tableNamePattern, snapshotNamePattern);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(listSnapshotsFuture, ((snapshotDescriptions, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (snapshotDescriptions == null || snapshotDescriptions.isEmpty()) {
        future.complete(null);
        return;
      }
      addListener(CompletableFuture.allOf(snapshotDescriptions.stream()
        .map(this::internalDeleteSnapshot).toArray(CompletableFuture[]::new)), (v, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          } else {
            future.complete(v);
          }
        });
    }));
    return future;
  }

  private CompletableFuture<Void> internalDeleteSnapshot(SnapshotDescription snapshot) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteSnapshotRequest, DeleteSnapshotResponse, Void> call(
            controller,
            stub,
            DeleteSnapshotRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.deleteSnapshot(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<Void> execProcedure(String signature, String instance,
      Map<String, String> props) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ProcedureDescription procDesc =
      ProtobufUtil.buildProcedureDescription(signature, instance, props);
    addListener(this.<Long> newMasterCaller()
      .action((controller, stub) -> this.<ExecProcedureRequest, ExecProcedureResponse, Long> call(
        controller, stub, ExecProcedureRequest.newBuilder().setProcedure(procDesc).build(),
        (s, c, req, done) -> s.execProcedure(c, req, done), resp -> resp.getExpectedTimeout()))
      .call(), (expectedTimeout, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        TimerTask pollingTask = new TimerTask() {
          int tries = 0;
          long startTime = EnvironmentEdgeManager.currentTime();
          long endTime = startTime + expectedTimeout;
          long maxPauseTime = expectedTimeout / maxAttempts;

          @Override
          public void run(Timeout timeout) throws Exception {
            if (EnvironmentEdgeManager.currentTime() < endTime) {
              addListener(isProcedureFinished(signature, instance, props), (done, err2) -> {
                if (err2 != null) {
                  future.completeExceptionally(err2);
                  return;
                }
                if (done) {
                  future.complete(null);
                } else {
                  // retry again after pauseTime.
                  long pauseTime =
                    ConnectionUtils.getPauseTime(TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                  pauseTime = Math.min(pauseTime, maxPauseTime);
                  AsyncConnectionImpl.RETRY_TIMER.newTimeout(this, pauseTime,
                    TimeUnit.MICROSECONDS);
                }
              });
            } else {
              future.completeExceptionally(new IOException("Procedure '" + signature + " : " +
                instance + "' wasn't completed in expectedTime:" + expectedTimeout + " ms"));
            }
          }
        };
        // Queue the polling task into RETRY_TIMER to poll procedure state asynchronously.
        AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
      });
    return future;
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithReturn(String signature, String instance,
      Map<String, String> props) {
    ProcedureDescription proDesc =
        ProtobufUtil.buildProcedureDescription(signature, instance, props);
    return this.<byte[]> newMasterCaller()
        .action(
          (controller, stub) -> this.<ExecProcedureRequest, ExecProcedureResponse, byte[]> call(
            controller, stub, ExecProcedureRequest.newBuilder().setProcedure(proDesc).build(),
            (s, c, req, done) -> s.execProcedureWithRet(c, req, done),
            resp -> resp.hasReturnData() ? resp.getReturnData().toByteArray() : null))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(String signature, String instance,
      Map<String, String> props) {
    ProcedureDescription proDesc =
        ProtobufUtil.buildProcedureDescription(signature, instance, props);
    return this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<IsProcedureDoneRequest, IsProcedureDoneResponse, Boolean> call(controller, stub,
              IsProcedureDoneRequest.newBuilder().setProcedure(proDesc).build(),
              (s, c, req, done) -> s.isProcedureDone(c, req, done), resp -> resp.getDone()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> abortProcedure(long procId, boolean mayInterruptIfRunning) {
    return this.<Boolean> newMasterCaller().action(
      (controller, stub) -> this.<AbortProcedureRequest, AbortProcedureResponse, Boolean> call(
        controller, stub, AbortProcedureRequest.newBuilder().setProcId(procId).build(),
        (s, c, req, done) -> s.abortProcedure(c, req, done), resp -> resp.getIsProcedureAborted()))
        .call();
  }

  @Override
  public CompletableFuture<String> getProcedures() {
    return this
        .<String> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetProceduresRequest, GetProceduresResponse, String> call(
                controller, stub, GetProceduresRequest.newBuilder().build(),
                (s, c, req, done) -> s.getProcedures(c, req, done),
                resp -> ProtobufUtil.toProcedureJson(resp.getProcedureList()))).call();
  }

  @Override
  public CompletableFuture<String> getLocks() {
    return this
        .<String> newMasterCaller()
        .action(
          (controller, stub) -> this.<GetLocksRequest, GetLocksResponse, String> call(
            controller, stub, GetLocksRequest.newBuilder().build(),
            (s, c, req, done) -> s.getLocks(c, req, done),
            resp -> ProtobufUtil.toLockJson(resp.getLockList()))).call();
  }

  @Override
  public CompletableFuture<Void> decommissionRegionServers(
      List<ServerName> servers, boolean offload) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this
          .<DecommissionRegionServersRequest, DecommissionRegionServersResponse, Void> call(
            controller, stub,
              RequestConverter.buildDecommissionRegionServersRequest(servers, offload),
            (s, c, req, done) -> s.decommissionRegionServers(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<List<ServerName>> listDecommissionedRegionServers() {
    return this.<List<ServerName>> newMasterCaller()
        .action((controller, stub) -> this
          .<ListDecommissionedRegionServersRequest, ListDecommissionedRegionServersResponse,
            List<ServerName>> call(
              controller, stub, ListDecommissionedRegionServersRequest.newBuilder().build(),
              (s, c, req, done) -> s.listDecommissionedRegionServers(c, req, done),
              resp -> resp.getServerNameList().stream().map(ProtobufUtil::toServerName)
                  .collect(Collectors.toList())))
        .call();
  }

  @Override
  public CompletableFuture<Void> recommissionRegionServer(ServerName server,
      List<byte[]> encodedRegionNames) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) ->
            this.<RecommissionRegionServerRequest, RecommissionRegionServerResponse, Void> call(
                controller, stub, RequestConverter.buildRecommissionRegionServerRequest(
                    server, encodedRegionNames), (s, c, req, done) -> s.recommissionRegionServer(
                        c, req, done), resp -> null)).call();
  }

  /**
   * Get the region location for the passed region name. The region name may be a full region name
   * or encoded region name. If the region does not found, then it'll throw an
   * UnknownRegionException wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName region name or encoded region name
   * @return region location, wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }

    CompletableFuture<Optional<HRegionLocation>> future;
    if (RegionInfo.isEncodedRegionName(regionNameOrEncodedRegionName)) {
      String encodedName = Bytes.toString(regionNameOrEncodedRegionName);
      if (encodedName.length() < RegionInfo.MD5_HEX_LENGTH) {
        // old format encodedName, should be meta region
        future = connection.registry.getMetaRegionLocations()
          .thenApply(locs -> Stream.of(locs.getRegionLocations())
            .filter(loc -> loc.getRegion().getEncodedName().equals(encodedName)).findFirst());
      } else {
        future = ClientMetaTableAccessor.getRegionLocationWithEncodedName(metaTable,
          regionNameOrEncodedRegionName);
      }
    } else {
      // Not all regionNameOrEncodedRegionName here is going to be a valid region name,
      // it needs to throw out IllegalArgumentException in case tableName is passed in.
      RegionInfo regionInfo;
      try {
        regionInfo = CatalogFamilyFormat.parseRegionInfoFromRegionName(
          regionNameOrEncodedRegionName);
      } catch (IOException ioe) {
        return failedFuture(new IllegalArgumentException(ioe.getMessage()));
      }

      if (regionInfo.isMetaRegion()) {
        future = connection.registry.getMetaRegionLocations()
          .thenApply(locs -> Stream.of(locs.getRegionLocations())
            .filter(loc -> loc.getRegion().getReplicaId() == regionInfo.getReplicaId())
            .findFirst());
      } else {
        future =
          ClientMetaTableAccessor.getRegionLocation(metaTable, regionNameOrEncodedRegionName);
      }
    }

    CompletableFuture<HRegionLocation> returnedFuture = new CompletableFuture<>();
    addListener(future, (location, err) -> {
      if (err != null) {
        returnedFuture.completeExceptionally(err);
        return;
      }
      if (!location.isPresent() || location.get().getRegion() == null) {
        returnedFuture.completeExceptionally(
          new UnknownRegionException("Invalid region name or encoded region name: " +
            Bytes.toStringBinary(regionNameOrEncodedRegionName)));
      } else {
        returnedFuture.complete(location.get());
      }
    });
    return returnedFuture;
  }

  /**
   * Get the region info for the passed region name. The region name may be a full region name or
   * encoded region name. If the region does not found, then it'll throw an UnknownRegionException
   * wrapped by a {@link CompletableFuture}
   * @return region info, wrapped by a {@link CompletableFuture}
   */
  private CompletableFuture<RegionInfo> getRegionInfo(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }

    if (Bytes.equals(regionNameOrEncodedRegionName,
      RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName()) ||
      Bytes.equals(regionNameOrEncodedRegionName,
        RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return CompletableFuture.completedFuture(RegionInfoBuilder.FIRST_META_REGIONINFO);
    }

    CompletableFuture<RegionInfo> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionNameOrEncodedRegionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        future.complete(location.getRegion());
      }
    });
    return future;
  }

  private byte[][] getSplitKeys(byte[] startKey, byte[] endKey, int numRegions) {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      return new byte[][] { startKey, endKey };
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    return splitKeys;
  }

  private void verifySplitKeys(byte[][] splitKeys) {
    Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
    // Verify there are no duplicate split keys
    byte[] lastKey = null;
    for (byte[] splitKey : splitKeys) {
      if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
        throw new IllegalArgumentException("Empty split key must not be passed in the split keys.");
      }
      if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
        throw new IllegalArgumentException("All split keys must be unique, " + "found duplicate: "
            + Bytes.toStringBinary(splitKey) + ", " + Bytes.toStringBinary(lastKey));
      }
      lastKey = splitKey;
    }
  }

  private static abstract class ProcedureBiConsumer implements BiConsumer<Void, Throwable> {

    abstract void onFinished();

    abstract void onError(Throwable error);

    @Override
    public void accept(Void v, Throwable error) {
      if (error != null) {
        onError(error);
        return;
      }
      onFinished();
    }
  }

  private static abstract class TableProcedureBiConsumer extends ProcedureBiConsumer {
    protected final TableName tableName;

    TableProcedureBiConsumer(TableName tableName) {
      this.tableName = tableName;
    }

    abstract String getOperationType();

    String getDescription() {
      return "Operation: " + getOperationType() + ", " + "Table Name: "
          + tableName.getNameWithNamespaceInclAsString();
    }

    @Override
    void onFinished() {
      LOG.info(getDescription() + " completed");
    }

    @Override
    void onError(Throwable error) {
      LOG.info(getDescription() + " failed with " + error.getMessage());
    }
  }

  private static abstract class NamespaceProcedureBiConsumer extends ProcedureBiConsumer {
    protected final String namespaceName;

    NamespaceProcedureBiConsumer(String namespaceName) {
      this.namespaceName = namespaceName;
    }

    abstract String getOperationType();

    String getDescription() {
      return "Operation: " + getOperationType() + ", Namespace: " + namespaceName;
    }

    @Override
    void onFinished() {
      LOG.info(getDescription() + " completed");
    }

    @Override
    void onError(Throwable error) {
      LOG.info(getDescription() + " failed with " + error.getMessage());
    }
  }

  private static class CreateTableProcedureBiConsumer extends TableProcedureBiConsumer {

    CreateTableProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "CREATE";
    }
  }

  private static class ModifyTableProcedureBiConsumer extends TableProcedureBiConsumer {

    ModifyTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "MODIFY";
    }
  }

  private static class ModifyTableStoreFileTrackerProcedureBiConsumer
    extends TableProcedureBiConsumer {

    ModifyTableStoreFileTrackerProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_TABLE_STORE_FILE_TRACKER";
    }
  }

  private class DeleteTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteTableProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "DELETE";
    }

    @Override
    void onFinished() {
      connection.getLocator().clearCache(this.tableName);
      super.onFinished();
    }
  }

  private static class TruncateTableProcedureBiConsumer extends TableProcedureBiConsumer {

    TruncateTableProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "TRUNCATE";
    }
  }

  private static class EnableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    EnableTableProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "ENABLE";
    }
  }

  private static class DisableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DisableTableProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "DISABLE";
    }
  }

  private static class AddColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    AddColumnFamilyProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "ADD_COLUMN_FAMILY";
    }
  }

  private static class DeleteColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteColumnFamilyProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "DELETE_COLUMN_FAMILY";
    }
  }

  private static class ModifyColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    ModifyColumnFamilyProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_COLUMN_FAMILY";
    }
  }

  private static class ModifyColumnFamilyStoreFileTrackerProcedureBiConsumer
    extends TableProcedureBiConsumer {

    ModifyColumnFamilyStoreFileTrackerProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_COLUMN_FAMILY_STORE_FILE_TRACKER";
    }
  }

  private static class CreateNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    CreateNamespaceProcedureBiConsumer(String namespaceName) {
      super(namespaceName);
    }

    @Override
    String getOperationType() {
      return "CREATE_NAMESPACE";
    }
  }

  private static class DeleteNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    DeleteNamespaceProcedureBiConsumer(String namespaceName) {
      super(namespaceName);
    }

    @Override
    String getOperationType() {
      return "DELETE_NAMESPACE";
    }
  }

  private static class ModifyNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    ModifyNamespaceProcedureBiConsumer(String namespaceName) {
      super(namespaceName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_NAMESPACE";
    }
  }

  private static class MergeTableRegionProcedureBiConsumer extends TableProcedureBiConsumer {

    MergeTableRegionProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "MERGE_REGIONS";
    }
  }

  private static class SplitTableRegionProcedureBiConsumer extends TableProcedureBiConsumer {

    SplitTableRegionProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "SPLIT_REGION";
    }
  }

  private static class SnapshotProcedureBiConsumer extends TableProcedureBiConsumer {
    SnapshotProcedureBiConsumer(TableName tableName) {
      super(tableName);
    }

    @Override
    String getOperationType() {
      return "SNAPSHOT";
    }
  }


  private static class ReplicationProcedureBiConsumer extends ProcedureBiConsumer {
    private final String peerId;
    private final Supplier<String> getOperation;

    ReplicationProcedureBiConsumer(String peerId, Supplier<String> getOperation) {
      this.peerId = peerId;
      this.getOperation = getOperation;
    }

    String getDescription() {
      return "Operation: " + getOperation.get() + ", peerId: " + peerId;
    }

    @Override
    void onFinished() {
      LOG.info(getDescription() + " completed");
    }

    @Override
    void onError(Throwable error) {
      LOG.info(getDescription() + " failed with " + error.getMessage());
    }
  }

  private CompletableFuture<Void> waitProcedureResult(CompletableFuture<Long> procFuture) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(procFuture, (procId, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      getProcedureResult(procId, future, 0);
    });
    return future;
  }

  private void getProcedureResult(long procId, CompletableFuture<Void> future, int retries) {
    addListener(
      this.<GetProcedureResultResponse> newMasterCaller()
        .action((controller, stub) -> this
          .<GetProcedureResultRequest, GetProcedureResultResponse, GetProcedureResultResponse> call(
            controller, stub, GetProcedureResultRequest.newBuilder().setProcId(procId).build(),
            (s, c, req, done) -> s.getProcedureResult(c, req, done), (resp) -> resp))
        .call(),
      (response, error) -> {
        if (error != null) {
          LOG.warn("failed to get the procedure result procId={}", procId,
            ConnectionUtils.translateException(error));
          retryTimer.newTimeout(t -> getProcedureResult(procId, future, retries + 1),
            ConnectionUtils.getPauseTime(pauseNs, retries), TimeUnit.NANOSECONDS);
          return;
        }
        if (response.getState() == GetProcedureResultResponse.State.RUNNING) {
          retryTimer.newTimeout(t -> getProcedureResult(procId, future, retries + 1),
            ConnectionUtils.getPauseTime(pauseNs, retries), TimeUnit.NANOSECONDS);
          return;
        }
        if (response.hasException()) {
          IOException ioe = ForeignExceptionUtil.toIOException(response.getException());
          future.completeExceptionally(ioe);
        } else {
          future.complete(null);
        }
      });
  }

  private <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  private <T> boolean completeExceptionally(CompletableFuture<T> future, Throwable error) {
    if (error != null) {
      future.completeExceptionally(error);
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics() {
    return getClusterMetrics(EnumSet.allOf(Option.class));
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics(EnumSet<Option> options) {
    return this
        .<ClusterMetrics> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetClusterStatusRequest, GetClusterStatusResponse, ClusterMetrics> call(controller,
                stub, RequestConverter.buildGetClusterStatusRequest(options),
                (s, c, req, done) -> s.getClusterStatus(c, req, done),
                resp -> ClusterMetricsBuilder.toClusterMetrics(resp.getClusterStatus()))).call();
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return this.<Void> newMasterCaller().priority(HIGH_QOS)
      .action((controller, stub) -> this.<ShutdownRequest, ShutdownResponse, Void> call(controller,
        stub, ShutdownRequest.newBuilder().build(), (s, c, req, done) -> s.shutdown(c, req, done),
        resp -> null))
      .call();
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    return this.<Void> newMasterCaller().priority(HIGH_QOS)
      .action((controller, stub) -> this.<StopMasterRequest, StopMasterResponse, Void> call(
        controller, stub, StopMasterRequest.newBuilder().build(),
        (s, c, req, done) -> s.stopMaster(c, req, done), resp -> null))
      .call();
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName serverName) {
    StopServerRequest request = RequestConverter
      .buildStopServerRequest("Called by admin client " + this.connection.toString());
    return this.<Void> newAdminCaller().priority(HIGH_QOS)
      .action((controller, stub) -> this.<StopServerRequest, StopServerResponse, Void> adminCall(
        controller, stub, request, (s, c, req, done) -> s.stopServer(controller, req, done),
        resp -> null))
      .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName serverName) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<UpdateConfigurationRequest, UpdateConfigurationResponse, Void> adminCall(
                controller, stub, UpdateConfigurationRequest.getDefaultInstance(),
                (s, c, req, done) -> s.updateConfiguration(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> updateConfiguration() {
    CompletableFuture<Void> future = new CompletableFuture<Void>();
    addListener(
      getClusterMetrics(EnumSet.of(Option.SERVERS_NAME, Option.MASTER, Option.BACKUP_MASTERS)),
      (status, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          List<CompletableFuture<Void>> futures = new ArrayList<>();
          status.getServersName().forEach(server -> futures.add(updateConfiguration(server)));
          futures.add(updateConfiguration(status.getMasterName()));
          status.getBackupMasterNames().forEach(master -> futures.add(updateConfiguration(master)));
          addListener(
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])),
            (result, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(result);
              }
            });
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(String groupName) {
    CompletableFuture<Void> future = new CompletableFuture<Void>();
    addListener(
      getRSGroup(groupName),
      (rsGroupInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (rsGroupInfo == null) {
          future.completeExceptionally(
            new IllegalArgumentException("Group does not exist: " + groupName));
        } else {
          addListener(getClusterMetrics(EnumSet.of(Option.SERVERS_NAME)), (status, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              List<CompletableFuture<Void>> futures = new ArrayList<>();
              List<ServerName> groupServers = status.getServersName().stream().filter(
                s -> rsGroupInfo.containsServer(s.getAddress())).collect(Collectors.toList());
              groupServers.forEach(server -> futures.add(updateConfiguration(server)));
              addListener(
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])),
                (result, err3) -> {
                  if (err3 != null) {
                    future.completeExceptionally(err3);
                  } else {
                    future.complete(result);
                  }
                });
            }
          });
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName serverName) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<RollWALWriterRequest, RollWALWriterResponse, Void> adminCall(
            controller, stub, RequestConverter.buildRollWALWriterRequest(),
            (s, c, req, done) -> s.rollWALWriter(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> clearCompactionQueues(ServerName serverName, Set<String> queues) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<ClearCompactionQueuesRequest, ClearCompactionQueuesResponse, Void> adminCall(
                controller, stub, RequestConverter.buildClearCompactionQueuesRequest(queues), (s,
                    c, req, done) -> s.clearCompactionQueues(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<SecurityCapability>> getSecurityCapabilities() {
    return this
        .<List<SecurityCapability>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SecurityCapabilitiesRequest, SecurityCapabilitiesResponse, List<SecurityCapability>>
                  call(controller, stub, SecurityCapabilitiesRequest.newBuilder().build(),
                    (s, c, req, done) -> s.getSecurityCapabilities(c, req, done),
                    (resp) -> ProtobufUtil.toSecurityCapabilityList(resp.getCapabilitiesList())))
        .call();
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName) {
    return getRegionMetrics(GetRegionLoadRequest.newBuilder().build(), serverName);
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName,
      TableName tableName) {
    Preconditions.checkNotNull(tableName,
      "tableName is null. If you don't specify a tableName, use getRegionLoads() instead");
    return getRegionMetrics(RequestConverter.buildGetRegionLoadRequest(tableName), serverName);
  }

  private CompletableFuture<List<RegionMetrics>> getRegionMetrics(GetRegionLoadRequest request,
      ServerName serverName) {
    return this.<List<RegionMetrics>> newAdminCaller()
        .action((controller, stub) -> this
            .<GetRegionLoadRequest, GetRegionLoadResponse, List<RegionMetrics>>
              adminCall(controller, stub, request, (s, c, req, done) ->
                s.getRegionLoad(controller, req, done), RegionMetricsBuilder::toRegionMetrics))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsInMaintenanceModeRequest, IsInMaintenanceModeResponse, Boolean> call(controller,
                stub, IsInMaintenanceModeRequest.newBuilder().build(),
                (s, c, req, done) -> s.isMasterInMaintenanceMode(c, req, done),
                resp -> resp.getInMaintenanceMode())).call();
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName tableName,
      CompactType compactType) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();

    switch (compactType) {
      case MOB:
        addListener(connection.registry.getActiveMaster(), (serverName, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          RegionInfo regionInfo = RegionInfo.createMobRegionInfo(tableName);

          addListener(this.<GetRegionInfoResponse> newAdminCaller().serverName(serverName)
            .action((controller, stub) -> this
              .<GetRegionInfoRequest, GetRegionInfoResponse, GetRegionInfoResponse> adminCall(
                controller, stub,
                RequestConverter.buildGetRegionInfoRequest(regionInfo.getRegionName(), true),
                (s, c, req, done) -> s.getRegionInfo(controller, req, done), resp -> resp))
            .call(), (resp2, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                if (resp2.hasCompactionState()) {
                  future.complete(ProtobufUtil.createCompactionState(resp2.getCompactionState()));
                } else {
                  future.complete(CompactionState.NONE);
                }
              }
            });
        });
        break;
      case NORMAL:
        addListener(getTableHRegionLocations(tableName), (locations, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          ConcurrentLinkedQueue<CompactionState> regionStates = new ConcurrentLinkedQueue<>();
          List<CompletableFuture<CompactionState>> futures = new ArrayList<>();
          locations.stream().filter(loc -> loc.getServerName() != null)
            .filter(loc -> loc.getRegion() != null).filter(loc -> !loc.getRegion().isOffline())
            .map(loc -> loc.getRegion().getRegionName()).forEach(region -> {
              futures.add(getCompactionStateForRegion(region).whenComplete((regionState, err2) -> {
                // If any region compaction state is MAJOR_AND_MINOR
                // the table compaction state is MAJOR_AND_MINOR, too.
                if (err2 != null) {
                  future.completeExceptionally(unwrapCompletionException(err2));
                } else if (regionState == CompactionState.MAJOR_AND_MINOR) {
                  future.complete(regionState);
                } else {
                  regionStates.add(regionState);
                }
              }));
            });
          addListener(
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])),
            (ret, err3) -> {
              // If future not completed, check all regions's compaction state
              if (!future.isCompletedExceptionally() && !future.isDone()) {
                CompactionState state = CompactionState.NONE;
                for (CompactionState regionState : regionStates) {
                  switch (regionState) {
                    case MAJOR:
                      if (state == CompactionState.MINOR) {
                        future.complete(CompactionState.MAJOR_AND_MINOR);
                      } else {
                        state = CompactionState.MAJOR;
                      }
                      break;
                    case MINOR:
                      if (state == CompactionState.MAJOR) {
                        future.complete(CompactionState.MAJOR_AND_MINOR);
                      } else {
                        state = CompactionState.MINOR;
                      }
                      break;
                    case NONE:
                    default:
                  }
                }
                if (!future.isDone()) {
                  future.complete(state);
                }
              }
            });
        });
        break;
      default:
        throw new IllegalArgumentException("Unknown compactType: " + compactType);
    }

    return future;
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] regionName) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();
    addListener(getRegionLocation(regionName), (location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      ServerName serverName = location.getServerName();
      if (serverName == null) {
        future
          .completeExceptionally(new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      addListener(
        this.<GetRegionInfoResponse> newAdminCaller()
          .action((controller, stub) -> this
            .<GetRegionInfoRequest, GetRegionInfoResponse, GetRegionInfoResponse> adminCall(
              controller, stub,
              RequestConverter.buildGetRegionInfoRequest(location.getRegion().getRegionName(),
                true),
              (s, c, req, done) -> s.getRegionInfo(controller, req, done), resp -> resp))
          .serverName(serverName).call(),
        (resp2, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            if (resp2.hasCompactionState()) {
              future.complete(ProtobufUtil.createCompactionState(resp2.getCompactionState()));
            } else {
              future.complete(CompactionState.NONE);
            }
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName tableName) {
    MajorCompactionTimestampRequest request =
        MajorCompactionTimestampRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
    return this.<Optional<Long>> newMasterCaller().action((controller, stub) ->
        this.<MajorCompactionTimestampRequest, MajorCompactionTimestampResponse, Optional<Long>>
            call(controller, stub, request, (s, c, req, done) -> s.getLastMajorCompactionTimestamp(
                c, req, done), ProtobufUtil::toOptionalTimestamp)).call();
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(
      byte[] regionName) {
    CompletableFuture<Optional<Long>> future = new CompletableFuture<>();
    // regionName may be a full region name or encoded region name, so getRegionInfo(byte[]) first
    addListener(getRegionInfo(regionName), (region, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      MajorCompactionTimestampForRegionRequest.Builder builder =
        MajorCompactionTimestampForRegionRequest.newBuilder();
      builder.setRegion(
        RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName));
      addListener(this.<Optional<Long>> newMasterCaller().action((controller, stub) -> this
        .<MajorCompactionTimestampForRegionRequest,
        MajorCompactionTimestampResponse, Optional<Long>> call(
          controller, stub, builder.build(),
          (s, c, req, done) -> s.getLastMajorCompactionTimestampForRegion(c, req, done),
          ProtobufUtil::toOptionalTimestamp))
        .call(), (timestamp, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(timestamp);
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Map<ServerName, Boolean>> compactionSwitch(boolean switchState,
      List<String> serverNamesList) {
    CompletableFuture<Map<ServerName, Boolean>> future = new CompletableFuture<>();
    addListener(getRegionServerList(serverNamesList), (serverNames, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      // Accessed by multiple threads.
      Map<ServerName, Boolean> serverStates = new ConcurrentHashMap<>(serverNames.size());
      List<CompletableFuture<Boolean>> futures = new ArrayList<>(serverNames.size());
      serverNames.stream().forEach(serverName -> {
        futures.add(switchCompact(serverName, switchState).whenComplete((serverState, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(unwrapCompletionException(err2));
          } else {
            serverStates.put(serverName, serverState);
          }
        }));
      });
      addListener(
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])),
        (ret, err3) -> {
          if (!future.isCompletedExceptionally()) {
            if (err3 != null) {
              future.completeExceptionally(err3);
            } else {
              future.complete(serverStates);
            }
          }
        });
    });
    return future;
  }

  private CompletableFuture<List<ServerName>> getRegionServerList(List<String> serverNamesList) {
    CompletableFuture<List<ServerName>> future = new CompletableFuture<>();
    if (serverNamesList.isEmpty()) {
      CompletableFuture<ClusterMetrics> clusterMetricsCompletableFuture =
        getClusterMetrics(EnumSet.of(Option.SERVERS_NAME));
      addListener(clusterMetricsCompletableFuture, (clusterMetrics, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          future.complete(clusterMetrics.getServersName());
        }
      });
      return future;
    } else {
      List<ServerName> serverList = new ArrayList<>();
      for (String regionServerName : serverNamesList) {
        ServerName serverName = null;
        try {
          serverName = ServerName.valueOf(regionServerName);
        } catch (Exception e) {
          future.completeExceptionally(
            new IllegalArgumentException(String.format("ServerName format: %s", regionServerName)));
        }
        if (serverName == null) {
          future.completeExceptionally(
            new IllegalArgumentException(String.format("Null ServerName: %s", regionServerName)));
        } else {
          serverList.add(serverName);
        }
      }
      future.complete(serverList);
    }
    return future;
  }

  private CompletableFuture<Boolean> switchCompact(ServerName serverName, boolean onOrOff) {
    return this
        .<Boolean>newAdminCaller()
        .serverName(serverName)
        .action((controller, stub) -> this.<CompactionSwitchRequest, CompactionSwitchResponse,
            Boolean>adminCall(controller, stub,
            CompactionSwitchRequest.newBuilder().setEnabled(onOrOff).build(), (s, c, req, done) ->
            s.compactionSwitch(c, req, done), resp -> resp.getPrevState())).call();
  }

  @Override
  public CompletableFuture<Boolean> balancerSwitch(boolean on, boolean drainRITs) {
    return this.<Boolean> newMasterCaller()
      .action((controller, stub) -> this
        .<SetBalancerRunningRequest, SetBalancerRunningResponse, Boolean> call(controller, stub,
          RequestConverter.buildSetBalancerRunningRequest(on, drainRITs),
          (s, c, req, done) -> s.setBalancerRunning(c, req, done),
          (resp) -> resp.getPrevBalanceValue()))
      .call();
  }

  @Override
  public CompletableFuture<BalanceResponse> balance(BalanceRequest request) {
    return this
        .<BalanceResponse> newMasterCaller()
        .action(
          (controller, stub) -> this.<MasterProtos.BalanceRequest, MasterProtos.BalanceResponse, BalanceResponse> call(controller,
            stub, ProtobufUtil.toBalanceRequest(request),
            (s, c, req, done) -> s.balance(c, req, done), (resp) -> ProtobufUtil.toBalanceResponse(resp))).call();
  }


  @Override
  public CompletableFuture<Boolean> isBalancerEnabled() {
    return this
        .<Boolean> newMasterCaller()
        .action((controller, stub) ->
              this.<IsBalancerEnabledRequest, IsBalancerEnabledResponse, Boolean> call(controller,
            stub, RequestConverter.buildIsBalancerEnabledRequest(), (s, c, req, done)
                  -> s.isBalancerEnabled(c, req, done), (resp) -> resp.getEnabled())).call();
  }

  @Override
  public CompletableFuture<Boolean> normalizerSwitch(boolean on) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetNormalizerRunningRequest, SetNormalizerRunningResponse, Boolean> call(
                controller, stub, RequestConverter.buildSetNormalizerRunningRequest(on), (s, c,
                    req, done) -> s.setNormalizerRunning(c, req, done), (resp) -> resp
                    .getPrevNormalizerValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerEnabled() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsNormalizerEnabledRequest, IsNormalizerEnabledResponse, Boolean> call(controller,
                stub, RequestConverter.buildIsNormalizerEnabledRequest(),
                (s, c, req, done) -> s.isNormalizerEnabled(c, req, done),
                (resp) -> resp.getEnabled())).call();
  }

  @Override
  public CompletableFuture<Boolean> normalize(NormalizeTableFilterParams ntfp) {
    return normalize(RequestConverter.buildNormalizeRequest(ntfp));
  }

  private CompletableFuture<Boolean> normalize(NormalizeRequest request) {
    return this
      .<Boolean> newMasterCaller()
      .action(
        (controller, stub) -> this.call(
          controller, stub, request, MasterService.Interface::normalize,
          NormalizeResponse::getNormalizerRan))
      .call();
  }

  @Override
  public CompletableFuture<Boolean> cleanerChoreSwitch(boolean enabled) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetCleanerChoreRunningRequest, SetCleanerChoreRunningResponse, Boolean> call(
                controller, stub, RequestConverter.buildSetCleanerChoreRunningRequest(enabled), (s,
                    c, req, done) -> s.setCleanerChoreRunning(c, req, done), (resp) -> resp
                    .getPrevValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreEnabled() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsCleanerChoreEnabledRequest, IsCleanerChoreEnabledResponse, Boolean> call(
                controller, stub, RequestConverter.buildIsCleanerChoreEnabledRequest(), (s, c, req,
                    done) -> s.isCleanerChoreEnabled(c, req, done), (resp) -> resp.getValue()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<RunCleanerChoreRequest, RunCleanerChoreResponse, Boolean> call(controller, stub,
                RequestConverter.buildRunCleanerChoreRequest(),
                (s, c, req, done) -> s.runCleanerChore(c, req, done),
                (resp) -> resp.getCleanerChoreRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> catalogJanitorSwitch(boolean enabled) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<EnableCatalogJanitorRequest, EnableCatalogJanitorResponse, Boolean> call(
                controller, stub, RequestConverter.buildEnableCatalogJanitorRequest(enabled), (s,
                    c, req, done) -> s.enableCatalogJanitor(c, req, done), (resp) -> resp
                    .getPrevValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorEnabled() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsCatalogJanitorEnabledRequest, IsCatalogJanitorEnabledResponse, Boolean> call(
                controller, stub, RequestConverter.buildIsCatalogJanitorEnabledRequest(), (s, c,
                    req, done) -> s.isCatalogJanitorEnabled(c, req, done), (resp) -> resp
                    .getValue())).call();
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    return this
        .<Integer> newMasterCaller()
        .action(
          (controller, stub) -> this.<RunCatalogScanRequest, RunCatalogScanResponse, Integer> call(
            controller, stub, RequestConverter.buildCatalogScanRequest(),
            (s, c, req, done) -> s.runCatalogScan(c, req, done), (resp) -> resp.getScanResult()))
        .call();
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable) {
    MasterCoprocessorRpcChannelImpl channel =
        new MasterCoprocessorRpcChannelImpl(this.<Message> newMasterCaller());
    S stub = stubMaker.apply(channel);
    CompletableFuture<R> future = new CompletableFuture<>();
    ClientCoprocessorRpcController controller = new ClientCoprocessorRpcController();
    callable.call(stub, controller, resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getFailed());
      } else {
        future.complete(resp);
      }
    });
    return future;
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, ServerName serverName) {
    RegionServerCoprocessorRpcChannelImpl channel =
        new RegionServerCoprocessorRpcChannelImpl(this.<Message> newServerCaller().serverName(
          serverName));
    S stub = stubMaker.apply(channel);
    CompletableFuture<R> future = new CompletableFuture<>();
    ClientCoprocessorRpcController controller = new ClientCoprocessorRpcController();
    callable.call(stub, controller, resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getFailed());
      } else {
        future.complete(resp);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<List<ServerName>> clearDeadServers(List<ServerName> servers) {
    return this.<List<ServerName>> newMasterCaller()
      .action((controller, stub) -> this
        .<ClearDeadServersRequest, ClearDeadServersResponse, List<ServerName>> call(
          controller, stub, RequestConverter.buildClearDeadServersRequest(servers),
          (s, c, req, done) -> s.clearDeadServers(c, req, done),
          (resp) -> ProtobufUtil.toServerNameList(resp.getServerNameList())))
      .call();
  }

  <T> ServerRequestCallerBuilder<T> newServerCaller() {
    return this.connection.callerFactory.<T> serverRequest()
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
      .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
      .pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForServerOverloaded(pauseNsForServerOverloaded, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt);
  }

  @Override
  public CompletableFuture<Void> enableTableReplication(TableName tableName) {
    if (tableName == null) {
      return failedFuture(new IllegalArgumentException("Table name is null"));
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exist, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (!exist) {
        future.completeExceptionally(new TableNotFoundException(
          "Table '" + tableName.getNameAsString() + "' does not exists."));
        return;
      }
      addListener(getTableSplits(tableName), (splits, err1) -> {
        if (err1 != null) {
          future.completeExceptionally(err1);
        } else {
          addListener(checkAndSyncTableToPeerClusters(tableName, splits), (result, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              addListener(setTableReplication(tableName, true), (result3, err3) -> {
                if (err3 != null) {
                  future.completeExceptionally(err3);
                } else {
                  future.complete(result3);
                }
              });
            }
          });
        }
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> disableTableReplication(TableName tableName) {
    if (tableName == null) {
      return failedFuture(new IllegalArgumentException("Table name is null"));
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exist, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (!exist) {
        future.completeExceptionally(new TableNotFoundException(
          "Table '" + tableName.getNameAsString() + "' does not exists."));
        return;
      }
      addListener(setTableReplication(tableName, false), (result, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(result);
        }
      });
    });
    return future;
  }

  private CompletableFuture<byte[][]> getTableSplits(TableName tableName) {
    CompletableFuture<byte[][]> future = new CompletableFuture<>();
    addListener(
      getRegions(tableName).thenApply(regions -> regions.stream()
        .filter(RegionReplicaUtil::isDefaultReplica).collect(Collectors.toList())),
      (regions, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
          return;
        }
        if (regions.size() == 1) {
          future.complete(null);
        } else {
          byte[][] splits = new byte[regions.size() - 1][];
          for (int i = 1; i < regions.size(); i++) {
            splits[i - 1] = regions.get(i).getStartKey();
          }
          future.complete(splits);
        }
      });
    return future;
  }

  /**
   * Connect to peer and check the table descriptor on peer:
   * <ol>
   * <li>Create the same table on peer when not exist.</li>
   * <li>Throw an exception if the table already has replication enabled on any of the column
   * families.</li>
   * <li>Throw an exception if the table exists on peer cluster but descriptors are not same.</li>
   * </ol>
   * @param tableName name of the table to sync to the peer
   * @param splits table split keys
   */
  private CompletableFuture<Void> checkAndSyncTableToPeerClusters(TableName tableName,
      byte[][] splits) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(listReplicationPeers(), (peers, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (peers == null || peers.size() <= 0) {
        future.completeExceptionally(
          new IllegalArgumentException("Found no peer cluster for replication."));
        return;
      }
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      peers.stream().filter(peer -> peer.getPeerConfig().needToReplicate(tableName))
        .forEach(peer -> {
          futures.add(trySyncTableToPeerCluster(tableName, splits, peer));
        });
      addListener(
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])),
        (result, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(result);
          }
        });
    });
    return future;
  }

  private CompletableFuture<Void> trySyncTableToPeerCluster(TableName tableName, byte[][] splits,
      ReplicationPeerDescription peer) {
    Configuration peerConf = null;
    try {
      peerConf =
        ReplicationPeerConfigUtil.getPeerClusterConfiguration(connection.getConfiguration(), peer);
    } catch (IOException e) {
      return failedFuture(e);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(ConnectionFactory.createAsyncConnection(peerConf), (conn, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      addListener(getDescriptor(tableName), (tableDesc, err1) -> {
        if (err1 != null) {
          future.completeExceptionally(err1);
          return;
        }
        AsyncAdmin peerAdmin = conn.getAdmin();
        addListener(peerAdmin.tableExists(tableName), (exist, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
            return;
          }
          if (!exist) {
            CompletableFuture<Void> createTableFuture = null;
            if (splits == null) {
              createTableFuture = peerAdmin.createTable(tableDesc);
            } else {
              createTableFuture = peerAdmin.createTable(tableDesc, splits);
            }
            addListener(createTableFuture, (result, err3) -> {
              if (err3 != null) {
                future.completeExceptionally(err3);
              } else {
                future.complete(result);
              }
            });
          } else {
            addListener(compareTableWithPeerCluster(tableName, tableDesc, peer, peerAdmin),
              (result, err4) -> {
                if (err4 != null) {
                  future.completeExceptionally(err4);
                } else {
                  future.complete(result);
                }
              });
          }
        });
      });
    });
    return future;
  }

  private CompletableFuture<Void> compareTableWithPeerCluster(TableName tableName,
      TableDescriptor tableDesc, ReplicationPeerDescription peer, AsyncAdmin peerAdmin) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(peerAdmin.getDescriptor(tableName), (peerTableDesc, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (peerTableDesc == null) {
        future.completeExceptionally(
          new IllegalArgumentException("Failed to get table descriptor for table " +
            tableName.getNameAsString() + " from peer cluster " + peer.getPeerId()));
        return;
      }
      if (TableDescriptor.COMPARATOR_IGNORE_REPLICATION.compare(peerTableDesc, tableDesc) != 0) {
        future.completeExceptionally(new IllegalArgumentException(
          "Table " + tableName.getNameAsString() + " exists in peer cluster " + peer.getPeerId() +
            ", but the table descriptors are not same when compared with source cluster." +
            " Thus can not enable the table's replication switch."));
        return;
      }
      future.complete(null);
    });
    return future;
  }

  /**
   * Set the table's replication switch if the table's replication switch is already not set.
   * @param tableName name of the table
   * @param enableRep is replication switch enable or disable
   */
  private CompletableFuture<Void> setTableReplication(TableName tableName, boolean enableRep) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(getDescriptor(tableName), (tableDesc, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (!tableDesc.matchReplicationScope(enableRep)) {
        int scope =
          enableRep ? HConstants.REPLICATION_SCOPE_GLOBAL : HConstants.REPLICATION_SCOPE_LOCAL;
        TableDescriptor newTableDesc =
          TableDescriptorBuilder.newBuilder(tableDesc).setReplicationScope(scope).build();
        addListener(modifyTable(newTableDesc), (result, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(result);
          }
        });
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<CacheEvictionStats> clearBlockCache(TableName tableName) {
    CompletableFuture<CacheEvictionStats> future = new CompletableFuture<>();
    addListener(getTableHRegionLocations(tableName), (locations, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      Map<ServerName, List<RegionInfo>> regionInfoByServerName =
        locations.stream().filter(l -> l.getRegion() != null)
          .filter(l -> !l.getRegion().isOffline()).filter(l -> l.getServerName() != null)
          .collect(Collectors.groupingBy(l -> l.getServerName(),
            Collectors.mapping(l -> l.getRegion(), Collectors.toList())));
      List<CompletableFuture<CacheEvictionStats>> futures = new ArrayList<>();
      CacheEvictionStatsAggregator aggregator = new CacheEvictionStatsAggregator();
      for (Map.Entry<ServerName, List<RegionInfo>> entry : regionInfoByServerName.entrySet()) {
        futures
          .add(clearBlockCache(entry.getKey(), entry.getValue()).whenComplete((stats, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(unwrapCompletionException(err2));
            } else {
              aggregator.append(stats);
            }
          }));
      }
      addListener(CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])),
        (ret, err3) -> {
          if (err3 != null) {
            future.completeExceptionally(unwrapCompletionException(err3));
          } else {
            future.complete(aggregator.sum());
          }
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> cloneTableSchema(TableName tableName, TableName newTableName,
      boolean preserveSplits) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    addListener(tableExists(tableName), (exist, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (!exist) {
        future.completeExceptionally(new TableNotFoundException(tableName));
        return;
      }
      addListener(tableExists(newTableName), (exist1, err1) -> {
        if (err1 != null) {
          future.completeExceptionally(err1);
          return;
        }
        if (exist1) {
          future.completeExceptionally(new TableExistsException(newTableName));
          return;
        }
        addListener(getDescriptor(tableName), (tableDesc, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
            return;
          }
          TableDescriptor newTableDesc = TableDescriptorBuilder.copy(newTableName, tableDesc);
          if (preserveSplits) {
            addListener(getTableSplits(tableName), (splits, err3) -> {
              if (err3 != null) {
                future.completeExceptionally(err3);
              } else {
                addListener(
                  splits != null ? createTable(newTableDesc, splits) : createTable(newTableDesc),
                  (result, err4) -> {
                    if (err4 != null) {
                      future.completeExceptionally(err4);
                    } else {
                      future.complete(result);
                    }
                  });
              }
            });
          } else {
            addListener(createTable(newTableDesc), (result, err5) -> {
              if (err5 != null) {
                future.completeExceptionally(err5);
              } else {
                future.complete(result);
              }
            });
          }
        });
      });
    });
    return future;
  }

  private CompletableFuture<CacheEvictionStats> clearBlockCache(ServerName serverName,
      List<RegionInfo> hris) {
    return this.<CacheEvictionStats> newAdminCaller().action((controller, stub) -> this
      .<ClearRegionBlockCacheRequest, ClearRegionBlockCacheResponse, CacheEvictionStats> adminCall(
        controller, stub, RequestConverter.buildClearRegionBlockCacheRequest(hris),
        (s, c, req, done) -> s.clearRegionBlockCache(controller, req, done),
        resp -> ProtobufUtil.toCacheEvictionStats(resp.getStats())))
      .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Boolean> switchRpcThrottle(boolean enable) {
    CompletableFuture<Boolean> future = this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<SwitchRpcThrottleRequest, SwitchRpcThrottleResponse, Boolean> call(controller, stub,
              SwitchRpcThrottleRequest.newBuilder().setRpcThrottleEnabled(enable).build(),
              (s, c, req, done) -> s.switchRpcThrottle(c, req, done),
              resp -> resp.getPreviousRpcThrottleEnabled()))
        .call();
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isRpcThrottleEnabled() {
    CompletableFuture<Boolean> future = this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<IsRpcThrottleEnabledRequest, IsRpcThrottleEnabledResponse, Boolean> call(controller,
              stub, IsRpcThrottleEnabledRequest.newBuilder().build(),
              (s, c, req, done) -> s.isRpcThrottleEnabled(c, req, done),
              resp -> resp.getRpcThrottleEnabled()))
        .call();
    return future;
  }

  @Override
  public CompletableFuture<Boolean> exceedThrottleQuotaSwitch(boolean enable) {
    CompletableFuture<Boolean> future = this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<SwitchExceedThrottleQuotaRequest, SwitchExceedThrottleQuotaResponse, Boolean> call(
              controller, stub,
              SwitchExceedThrottleQuotaRequest.newBuilder().setExceedThrottleQuotaEnabled(enable)
                  .build(),
              (s, c, req, done) -> s.switchExceedThrottleQuota(c, req, done),
              resp -> resp.getPreviousExceedThrottleQuotaEnabled()))
        .call();
    return future;
  }

  @Override
  public CompletableFuture<Map<TableName, Long>> getSpaceQuotaTableSizes() {
    return this.<Map<TableName, Long>> newMasterCaller().action((controller, stub) -> this
      .<GetSpaceQuotaRegionSizesRequest, GetSpaceQuotaRegionSizesResponse,
      Map<TableName, Long>> call(controller, stub,
        RequestConverter.buildGetSpaceQuotaRegionSizesRequest(),
        (s, c, req, done) -> s.getSpaceQuotaRegionSizes(c, req, done),
        resp -> resp.getSizesList().stream().collect(Collectors
          .toMap(sizes -> ProtobufUtil.toTableName(sizes.getTableName()), RegionSizes::getSize))))
      .call();
  }

  @Override
  public CompletableFuture<Map<TableName, SpaceQuotaSnapshot>> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) {
    return this.<Map<TableName, SpaceQuotaSnapshot>> newAdminCaller()
      .action((controller, stub) -> this
        .<GetSpaceQuotaSnapshotsRequest, GetSpaceQuotaSnapshotsResponse,
        Map<TableName, SpaceQuotaSnapshot>> adminCall(controller, stub,
          RequestConverter.buildGetSpaceQuotaSnapshotsRequest(),
          (s, c, req, done) -> s.getSpaceQuotaSnapshots(controller, req, done),
          resp -> resp.getSnapshotsList().stream()
            .collect(Collectors.toMap(snapshot -> ProtobufUtil.toTableName(snapshot.getTableName()),
              snapshot -> SpaceQuotaSnapshot.toSpaceQuotaSnapshot(snapshot.getSnapshot())))))
      .serverName(serverName).call();
  }

  private CompletableFuture<SpaceQuotaSnapshot> getCurrentSpaceQuotaSnapshot(
      Converter<SpaceQuotaSnapshot, GetQuotaStatesResponse> converter) {
    return this.<SpaceQuotaSnapshot> newMasterCaller()
      .action((controller, stub) -> this
        .<GetQuotaStatesRequest, GetQuotaStatesResponse, SpaceQuotaSnapshot> call(controller, stub,
          RequestConverter.buildGetQuotaStatesRequest(),
          (s, c, req, done) -> s.getQuotaStates(c, req, done), converter))
      .call();
  }

  @Override
  public CompletableFuture<SpaceQuotaSnapshot> getCurrentSpaceQuotaSnapshot(String namespace) {
    return getCurrentSpaceQuotaSnapshot(resp -> resp.getNsSnapshotsList().stream()
      .filter(s -> s.getNamespace().equals(namespace)).findFirst()
      .map(s -> SpaceQuotaSnapshot.toSpaceQuotaSnapshot(s.getSnapshot())).orElse(null));
  }

  @Override
  public CompletableFuture<SpaceQuotaSnapshot> getCurrentSpaceQuotaSnapshot(TableName tableName) {
    HBaseProtos.TableName protoTableName = ProtobufUtil.toProtoTableName(tableName);
    return getCurrentSpaceQuotaSnapshot(resp -> resp.getTableSnapshotsList().stream()
      .filter(s -> s.getTableName().equals(protoTableName)).findFirst()
      .map(s -> SpaceQuotaSnapshot.toSpaceQuotaSnapshot(s.getSnapshot())).orElse(null));
  }

  @Override
  public CompletableFuture<Void> grant(UserPermission userPermission,
      boolean mergeExistingPermissions) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this.<GrantRequest, GrantResponse, Void> call(controller,
          stub, ShadedAccessControlUtil.buildGrantRequest(userPermission, mergeExistingPermissions),
          (s, c, req, done) -> s.grant(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<Void> revoke(UserPermission userPermission) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this.<RevokeRequest, RevokeResponse, Void> call(controller,
          stub, ShadedAccessControlUtil.buildRevokeRequest(userPermission),
          (s, c, req, done) -> s.revoke(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<List<UserPermission>>
      getUserPermissions(GetUserPermissionsRequest getUserPermissionsRequest) {
    return this.<List<UserPermission>> newMasterCaller().action((controller,
        stub) -> this.<AccessControlProtos.GetUserPermissionsRequest, GetUserPermissionsResponse,
            List<UserPermission>> call(controller, stub,
              ShadedAccessControlUtil.buildGetUserPermissionsRequest(getUserPermissionsRequest),
              (s, c, req, done) -> s.getUserPermissions(c, req, done),
              resp -> resp.getUserPermissionList().stream()
                .map(uPerm -> ShadedAccessControlUtil.toUserPermission(uPerm))
                .collect(Collectors.toList())))
        .call();
  }

  @Override
  public CompletableFuture<List<Boolean>> hasUserPermissions(String userName,
      List<Permission> permissions) {
    return this.<List<Boolean>> newMasterCaller()
        .action((controller, stub) -> this
            .<HasUserPermissionsRequest, HasUserPermissionsResponse, List<Boolean>> call(controller,
              stub, ShadedAccessControlUtil.buildHasUserPermissionsRequest(userName, permissions),
              (s, c, req, done) -> s.hasUserPermissions(c, req, done),
              resp -> resp.getHasUserPermissionList()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> snapshotCleanupSwitch(final boolean on,
      final boolean sync) {
    return this.<Boolean>newMasterCaller().action((controller, stub) -> this
        .call(controller, stub, RequestConverter.buildSetSnapshotCleanupRequest(on, sync),
            MasterService.Interface::switchSnapshotCleanup,
            SetSnapshotCleanupResponse::getPrevSnapshotCleanup)).call();
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotCleanupEnabled() {
    return this.<Boolean>newMasterCaller().action((controller, stub) -> this
        .call(controller, stub, RequestConverter.buildIsSnapshotCleanupEnabledRequest(),
            MasterService.Interface::isSnapshotCleanupEnabled,
            IsSnapshotCleanupEnabledResponse::getEnabled)).call();
  }

  @Override
  public CompletableFuture<Void> moveServersToRSGroup(Set<Address> servers, String groupName) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this.
            <MoveServersRequest, MoveServersResponse, Void> call(controller, stub,
                RequestConverter.buildMoveServersRequest(servers, groupName),
              (s, c, req, done) -> s.moveServers(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<Void> addRSGroup(String groupName) {
    return this.<Void> newMasterCaller()
        .action(((controller, stub) -> this.
            <AddRSGroupRequest, AddRSGroupResponse, Void> call(controller, stub,
                AddRSGroupRequest.newBuilder().setRSGroupName(groupName).build(),
              (s, c, req, done) -> s.addRSGroup(c, req, done), resp -> null)))
        .call();
  }

  @Override
  public CompletableFuture<Void> removeRSGroup(String groupName) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this.
            <RemoveRSGroupRequest, RemoveRSGroupResponse, Void> call(controller, stub,
                RemoveRSGroupRequest.newBuilder().setRSGroupName(groupName).build(),
              (s, c, req, done) -> s.removeRSGroup(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<BalanceResponse> balanceRSGroup(String groupName,
    BalanceRequest request) {
    return this.<BalanceResponse>newMasterCaller().action(
        (controller, stub) -> this.<BalanceRSGroupRequest, BalanceRSGroupResponse, BalanceResponse>call(
          controller, stub, ProtobufUtil.createBalanceRSGroupRequest(groupName, request),
          MasterService.Interface::balanceRSGroup, ProtobufUtil::toBalanceResponse))
      .call();
  }

  @Override
  public CompletableFuture<List<RSGroupInfo>> listRSGroups() {
    return this.<List<RSGroupInfo>> newMasterCaller()
        .action((controller, stub) -> this
            .<ListRSGroupInfosRequest, ListRSGroupInfosResponse, List<RSGroupInfo>> call(
                controller, stub, ListRSGroupInfosRequest.getDefaultInstance(),
              (s, c, req, done) -> s.listRSGroupInfos(c, req, done),
              resp -> resp.getRSGroupInfoList().stream()
                  .map(r -> ProtobufUtil.toGroupInfo(r))
                  .collect(Collectors.toList())))
        .call();
  }

  private CompletableFuture<List<LogEntry>> getSlowLogResponses(
      final Map<String, Object> filterParams, final Set<ServerName> serverNames, final int limit,
      final String logType) {
    if (CollectionUtils.isEmpty(serverNames)) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    return CompletableFuture.supplyAsync(() -> serverNames.stream()
      .map((ServerName serverName) ->
        getSlowLogResponseFromServer(serverName, filterParams, limit, logType))
      .map(CompletableFuture::join)
      .flatMap(List::stream)
      .collect(Collectors.toList()));
  }

  private CompletableFuture<List<LogEntry>> getSlowLogResponseFromServer(ServerName serverName,
      Map<String, Object> filterParams, int limit, String logType) {
    return this.<List<LogEntry>>newAdminCaller().action((controller, stub) -> this
      .adminCall(controller, stub,
        RequestConverter.buildSlowLogResponseRequest(filterParams, limit, logType),
        AdminService.Interface::getLogEntries, ProtobufUtil::toSlowLogPayloads))
      .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<Boolean>> clearSlowLogResponses(
      @Nullable Set<ServerName> serverNames) {
    if (CollectionUtils.isEmpty(serverNames)) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    List<CompletableFuture<Boolean>> clearSlowLogResponseList = serverNames.stream()
      .map(this::clearSlowLogsResponses)
      .collect(Collectors.toList());
    return convertToFutureOfList(clearSlowLogResponseList);
  }

  private CompletableFuture<Boolean> clearSlowLogsResponses(final ServerName serverName) {
    return this.<Boolean>newAdminCaller()
      .action(((controller, stub) -> this
        .adminCall(
          controller, stub, RequestConverter.buildClearSlowLogResponseRequest(),
          AdminService.Interface::clearSlowLogsResponses,
          ProtobufUtil::toClearSlowLogPayload))
      ).serverName(serverName).call();
  }

  private static <T> CompletableFuture<List<T>> convertToFutureOfList(
    List<CompletableFuture<T>> futures) {
    CompletableFuture<Void> allDoneFuture =
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    return allDoneFuture.thenApply(v ->
      futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList())
    );
  }

  @Override
  public CompletableFuture<List<TableName>> listTablesInRSGroup(String groupName) {
    return this.<List<TableName>> newMasterCaller()
      .action((controller, stub) -> this
        .<ListTablesInRSGroupRequest, ListTablesInRSGroupResponse, List<TableName>> call(controller,
          stub, ListTablesInRSGroupRequest.newBuilder().setGroupName(groupName).build(),
          (s, c, req, done) -> s.listTablesInRSGroup(c, req, done), resp -> resp.getTableNameList()
            .stream().map(ProtobufUtil::toTableName).collect(Collectors.toList())))
      .call();
  }

  @Override
  public CompletableFuture<Pair<List<String>, List<TableName>>>
    getConfiguredNamespacesAndTablesInRSGroup(String groupName) {
    return this.<Pair<List<String>, List<TableName>>> newMasterCaller()
      .action((controller, stub) -> this
        .<GetConfiguredNamespacesAndTablesInRSGroupRequest,
          GetConfiguredNamespacesAndTablesInRSGroupResponse,
          Pair<List<String>, List<TableName>>> call(controller, stub,
          GetConfiguredNamespacesAndTablesInRSGroupRequest.newBuilder().setGroupName(groupName)
            .build(),
            (s, c, req, done) -> s.getConfiguredNamespacesAndTablesInRSGroup(c, req, done),
            resp -> Pair.newPair(resp.getNamespaceList(), resp.getTableNameList().stream()
              .map(ProtobufUtil::toTableName).collect(Collectors.toList()))))
      .call();
  }

  @Override
  public CompletableFuture<RSGroupInfo> getRSGroup(Address hostPort) {
    return this.<RSGroupInfo> newMasterCaller()
      .action(((controller, stub) -> this
        .<GetRSGroupInfoOfServerRequest, GetRSGroupInfoOfServerResponse, RSGroupInfo> call(
          controller, stub,
          GetRSGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.ServerName.newBuilder().setHostName(hostPort.getHostname())
              .setPort(hostPort.getPort()).build())
            .build(),
          (s, c, req, done) -> s.getRSGroupInfoOfServer(c, req, done),
          resp -> resp.hasRSGroupInfo() ? ProtobufUtil.toGroupInfo(resp.getRSGroupInfo()) : null)))
      .call();
  }

  @Override
  public CompletableFuture<Void> removeServersFromRSGroup(Set<Address> servers) {
    return this.<Void> newMasterCaller()
      .action((controller, stub) -> this.
            <RemoveServersRequest, RemoveServersResponse, Void> call(controller, stub,
                RequestConverter.buildRemoveServersRequest(servers),
              (s, c, req, done) -> s.removeServers(c, req, done), resp -> null))
        .call();
  }

  @Override
  public CompletableFuture<Void> setRSGroup(Set<TableName> tables, String groupName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    for (TableName tableName : tables) {
      addListener(tableExists(tableName), (exist, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        if (!exist) {
          future.completeExceptionally(new TableNotFoundException(tableName));
          return;
        }
      });
    }
    addListener(listTableDescriptors(new ArrayList<>(tables)), ((tableDescriptions, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (tableDescriptions == null || tableDescriptions.isEmpty()) {
        future.complete(null);
        return;
      }
      List<TableDescriptor> newTableDescriptors = new ArrayList<>();
      for (TableDescriptor td : tableDescriptions) {
        newTableDescriptors
            .add(TableDescriptorBuilder.newBuilder(td).setRegionServerGroup(groupName).build());
      }
      addListener(CompletableFuture.allOf(
        newTableDescriptors.stream().map(this::modifyTable).toArray(CompletableFuture[]::new)),
        (v, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          } else {
            future.complete(v);
          }
        });
    }));
    return future;
  }

  @Override
  public CompletableFuture<RSGroupInfo> getRSGroup(TableName table) {
    return this.<RSGroupInfo> newMasterCaller().action(((controller, stub) -> this
      .<GetRSGroupInfoOfTableRequest, GetRSGroupInfoOfTableResponse, RSGroupInfo> call(controller,
        stub,
        GetRSGroupInfoOfTableRequest.newBuilder().setTableName(ProtobufUtil.toProtoTableName(table))
          .build(),
        (s, c, req, done) -> s.getRSGroupInfoOfTable(c, req, done),
        resp -> resp.hasRSGroupInfo() ? ProtobufUtil.toGroupInfo(resp.getRSGroupInfo()) : null)))
      .call();
  }

  @Override
  public CompletableFuture<RSGroupInfo> getRSGroup(String groupName) {
    return this.<RSGroupInfo> newMasterCaller()
      .action(((controller, stub) -> this
        .<GetRSGroupInfoRequest, GetRSGroupInfoResponse, RSGroupInfo> call(controller, stub,
          GetRSGroupInfoRequest.newBuilder().setRSGroupName(groupName).build(),
          (s, c, req, done) -> s.getRSGroupInfo(c, req, done),
          resp -> resp.hasRSGroupInfo() ? ProtobufUtil.toGroupInfo(resp.getRSGroupInfo()) : null)))
      .call();
  }

  @Override
  public CompletableFuture<Void> renameRSGroup(String oldName, String newName) {
    return this.<Void> newMasterCaller()
      .action(
        (
          (controller, stub) -> this.<RenameRSGroupRequest, RenameRSGroupResponse, Void> call(
            controller,
            stub,
            RenameRSGroupRequest.newBuilder().setOldRsgroupName(oldName).setNewRsgroupName(newName)
                                .build(),
            (s, c, req, done) -> s.renameRSGroup(c, req, done),
            resp -> null
          )
        )
      ).call();
  }

  @Override
  public CompletableFuture<Void>
    updateRSGroupConfig(String groupName, Map<String, String> configuration) {
    UpdateRSGroupConfigRequest.Builder request = UpdateRSGroupConfigRequest.newBuilder()
        .setGroupName(groupName);
    if (configuration != null) {
      configuration.entrySet().forEach(e ->
          request.addConfiguration(NameStringPair.newBuilder().setName(e.getKey())
              .setValue(e.getValue()).build()));
    }
    return this.<Void> newMasterCaller()
        .action(((controller, stub) ->
            this.<UpdateRSGroupConfigRequest, UpdateRSGroupConfigResponse, Void> call(
                controller, stub, request.build(),
              (s, c, req, done) -> s.updateRSGroupConfig(c, req, done), resp -> null))
        ).call();
  }

  private CompletableFuture<List<LogEntry>> getBalancerDecisions(final int limit) {
    return this.<List<LogEntry>>newMasterCaller()
      .action((controller, stub) ->
        this.call(controller, stub,
          ProtobufUtil.toBalancerDecisionRequest(limit),
          MasterService.Interface::getLogEntries, ProtobufUtil::toBalancerDecisionResponse))
      .call();
  }

  private CompletableFuture<List<LogEntry>> getBalancerRejections(final int limit) {
    return this.<List<LogEntry>>newMasterCaller()
      .action((controller, stub) ->
        this.call(controller, stub,
          ProtobufUtil.toBalancerRejectionRequest(limit),
          MasterService.Interface::getLogEntries, ProtobufUtil::toBalancerRejectionResponse))
      .call();
  }

  @Override
  public CompletableFuture<List<LogEntry>> getLogEntries(Set<ServerName> serverNames,
      String logType, ServerType serverType, int limit,
      Map<String, Object> filterParams) {
    if (logType == null || serverType == null) {
      throw new IllegalArgumentException("logType and/or serverType cannot be empty");
    }
    switch (logType){
      case "SLOW_LOG":
      case "LARGE_LOG":
        if (ServerType.MASTER.equals(serverType)) {
          throw new IllegalArgumentException("Slow/Large logs are not maintained by HMaster");
        }
        return getSlowLogResponses(filterParams, serverNames, limit, logType);
      case "BALANCER_DECISION":
        if (ServerType.REGION_SERVER.equals(serverType)) {
          throw new IllegalArgumentException(
            "Balancer Decision logs are not maintained by HRegionServer");
        }
        return getBalancerDecisions(limit);
      case "BALANCER_REJECTION":
        if (ServerType.REGION_SERVER.equals(serverType)) {
          throw new IllegalArgumentException(
            "Balancer Rejection logs are not maintained by HRegionServer");
        }
        return getBalancerRejections(limit);
      default:
        return CompletableFuture.completedFuture(Collections.emptyList());
    }
  }
}
