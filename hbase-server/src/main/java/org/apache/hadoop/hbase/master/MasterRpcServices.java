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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.janitor.MetaFixer;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.AccessChecker.InputUser;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.ShadedAccessControlUtil;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.ServiceDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.FixMetaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.FixMetaResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.HbckService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledRequest;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RegionSpecifierAndState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunHbckChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunHbckChoreResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetRegionStateInMetaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetRegionStateInMetaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetTableStateInMetaRequest;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse.NamespaceQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse.TableQuotaSnapshot;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLiveRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLiveRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RemoteProcedureResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ReplicationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;

/**
 * Implements the master RPC services.
 */
@InterfaceAudience.Private
public class MasterRpcServices extends HBaseRpcServicesBase<HMaster>
  implements MasterService.BlockingInterface, RegionServerStatusService.BlockingInterface,
  LockService.BlockingInterface, HbckService.BlockingInterface {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRpcServices.class.getName());
  private static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger."+MasterRpcServices.class.getName());

  /** RPC scheduler to use for the master. */
  public static final String MASTER_RPC_SCHEDULER_FACTORY_CLASS =
    "hbase.master.rpc.scheduler.factory.class";

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   *     the filesystem to use and root directory to use.
   */
  private RegionServerStartupResponse.Builder createConfigurationSubset() {
    RegionServerStartupResponse.Builder resp = addConfig(
      RegionServerStartupResponse.newBuilder(), HConstants.HBASE_DIR);
    resp = addConfig(resp, "fs.defaultFS");
    return addConfig(resp, "hbase.master.info.port");
  }

  private RegionServerStartupResponse.Builder addConfig(
      final RegionServerStartupResponse.Builder resp, final String key) {
    NameStringPair.Builder entry = NameStringPair.newBuilder()
      .setName(key)
      .setValue(server.getConfiguration().get(key));
    resp.addMapEntries(entry.build());
    return resp;
  }

  public MasterRpcServices(HMaster m) throws IOException {
    super(m, m.getProcessName());
  }

  @Override
  protected boolean defaultReservoirEnabled() {
    return false;
  }

  @Override
  protected ServerType getDNSServerType() {
    return DNS.ServerType.MASTER;
  }

  @Override
  protected String getHostname(Configuration conf, String defaultHostname) {
    return conf.get("hbase.master.ipc.address", defaultHostname);
  }

  @Override
  protected String getPortConfigName() {
    return HConstants.MASTER_PORT;
  }

  @Override
  protected int getDefaultPort() {
    return HConstants.DEFAULT_MASTER_PORT;
  }

  @Override
  protected Class<?> getRpcSchedulerFactoryClass(Configuration conf) {
    return conf.getClass(MASTER_RPC_SCHEDULER_FACTORY_CLASS, SimpleRpcSchedulerFactory.class);
  }

  @Override
  protected PriorityFunction createPriority() {
    return new MasterAnnotationReadingPriorityFunction(this);
  }

  /**
   * Checks for the following pre-checks in order:
   * <ol>
   *   <li>Master is initialized</li>
   *   <li>Rpc caller has admin permissions</li>
   * </ol>
   * @param requestName name of rpc request. Used in reporting failures to provide context.
   * @throws ServiceException If any of the above listed pre-check fails.
   */
  private void rpcPreCheck(String requestName) throws ServiceException {
    try {
      server.checkInitialized();
      requirePermission(requestName, Permission.Action.ADMIN);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  enum BalanceSwitchMode {
    SYNC,
    ASYNC
  }

  /**
   * Assigns balancer switch according to BalanceSwitchMode
   * @param b new balancer switch
   * @param mode BalanceSwitchMode
   * @return old balancer switch
   */
  boolean switchBalancer(final boolean b, BalanceSwitchMode mode) throws IOException {
    boolean oldValue = server.loadBalancerTracker.isBalancerOn();
    boolean newValue = b;
    try {
      if (server.cpHost != null) {
        server.cpHost.preBalanceSwitch(newValue);
      }
      try {
        if (mode == BalanceSwitchMode.SYNC) {
          synchronized (server.getLoadBalancer()) {
            server.loadBalancerTracker.setBalancerOn(newValue);
          }
        } else {
          server.loadBalancerTracker.setBalancerOn(newValue);
        }
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
      LOG.info(server.getClientIdAuditPrefix() + " set balanceSwitch=" + newValue);
      if (server.cpHost != null) {
        server.cpHost.postBalanceSwitch(oldValue, newValue);
      }
      server.getLoadBalancer().updateBalancerStatus(newValue);
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  boolean synchronousBalanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.SYNC);
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  @Override
  protected List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<>(5);
    bssi.add(new BlockingServiceAndInterface(
        MasterService.newReflectiveBlockingService(this),
        MasterService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
        RegionServerStatusService.newReflectiveBlockingService(this),
        RegionServerStatusService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(LockService.newReflectiveBlockingService(this),
        LockService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(HbckService.newReflectiveBlockingService(this),
        HbckService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(ClientMetaService.newReflectiveBlockingService(this),
        ClientMetaService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
      AdminService.BlockingInterface.class));
    return bssi;
  }

  void start(ZKWatcher zkWatcher) {
    internalStart(zkWatcher);
  }

  void stop() {
    internalStop();
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetLastFlushedSequenceIdResponse getLastFlushedSequenceId(RpcController controller,
      GetLastFlushedSequenceIdRequest request) throws ServiceException {
    try {
      server.checkServiceStarted();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    byte[] encodedRegionName = request.getRegionName().toByteArray();
    RegionStoreSequenceIds ids = server.getServerManager()
      .getLastFlushedSequenceId(encodedRegionName);
    return ResponseConverter.buildGetLastFlushedSequenceIdResponse(ids);
  }

  @Override
  public RegionServerReportResponse regionServerReport(RpcController controller,
      RegionServerReportRequest request) throws ServiceException {
    try {
      server.checkServiceStarted();
      int versionNumber = 0;
      String version = "0.0.0";
      VersionInfo versionInfo = VersionInfoUtil.getCurrentClientVersionInfo();
      if (versionInfo != null) {
        version = versionInfo.getVersion();
        versionNumber = VersionInfoUtil.getVersionNumber(versionInfo);
      }
      ClusterStatusProtos.ServerLoad sl = request.getLoad();
      ServerName serverName = ProtobufUtil.toServerName(request.getServer());
      ServerMetrics oldLoad = server.getServerManager().getLoad(serverName);
      ServerMetrics newLoad =
        ServerMetricsBuilder.toServerMetrics(serverName, versionNumber, version, sl);
      server.getServerManager().regionServerReport(serverName, newLoad);
      server.getAssignmentManager().reportOnlineRegions(serverName,
        newLoad.getRegionMetrics().keySet());
      if (sl != null && server.metricsMaster != null) {
        // Up our metrics.
        server.metricsMaster.incrementRequests(
          sl.getTotalNumberOfRequests() -
                  (oldLoad != null ? oldLoad.getRequestCount() : 0));
        server.metricsMaster.incrementReadRequests(sl.getReadRequestsCount() -
                (oldLoad != null ? oldLoad.getReadRequestsCount() : 0));
        server.metricsMaster.incrementWriteRequests(sl.getWriteRequestsCount() -
                (oldLoad != null ? oldLoad.getWriteRequestsCount() : 0));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return RegionServerReportResponse.newBuilder().build();
  }

  @Override
  public RegionServerStartupResponse regionServerStartup(RpcController controller,
      RegionServerStartupRequest request) throws ServiceException {
    // Register with server manager
    try {
      server.checkServiceStarted();
      int versionNumber = 0;
      String version = "0.0.0";
      VersionInfo versionInfo = VersionInfoUtil.getCurrentClientVersionInfo();
      if (versionInfo != null) {
        version = versionInfo.getVersion();
        versionNumber = VersionInfoUtil.getVersionNumber(versionInfo);
      }
      InetAddress ia = server.getRemoteInetAddress(request.getPort(), request.getServerStartCode());
      // if regionserver passed hostname to use,
      // then use it instead of doing a reverse DNS lookup
      ServerName rs =
        server.getServerManager().regionServerStartup(request, versionNumber, version, ia);

      // Send back some config info
      RegionServerStartupResponse.Builder resp = createConfigurationSubset();
      NameStringPair.Builder entry = NameStringPair.newBuilder()
        .setName(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER).setValue(rs.getHostname());
      resp.addMapEntries(entry.build());

      return resp.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, ReportRSFatalErrorRequest request) throws ServiceException {
    String errorText = request.getErrorMessage();
    ServerName sn = ProtobufUtil.toServerName(request.getServer());
    String msg = sn + " reported a fatal error:\n" + errorText;
    LOG.warn(msg);
    server.rsFatals.add(msg);
    return ReportRSFatalErrorResponse.newBuilder().build();
  }

  @Override
  public AddColumnResponse addColumn(RpcController controller,
      AddColumnRequest req) throws ServiceException {
    try {
      long procId = server.addColumn(
          ProtobufUtil.toTableName(req.getTableName()),
          ProtobufUtil.toColumnFamilyDescriptor(req.getColumnFamilies()),
          req.getNonceGroup(),
          req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return AddColumnResponse.newBuilder().build();
      } else {
        return AddColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public AssignRegionResponse assignRegion(RpcController controller,
      AssignRegionRequest req) throws ServiceException {
    try {
      server.checkInitialized();

      final RegionSpecifierType type = req.getRegion().getType();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("assignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }

      final byte[] regionName = req.getRegion().getValue().toByteArray();
      final RegionInfo regionInfo = server.getAssignmentManager().getRegionInfo(regionName);
      if (regionInfo == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionName));
      }

      final AssignRegionResponse arr = AssignRegionResponse.newBuilder().build();
      if (server.cpHost != null) {
        server.cpHost.preAssign(regionInfo);
      }
      LOG.info(server.getClientIdAuditPrefix() + " assign " + regionInfo.getRegionNameAsString());
      server.getAssignmentManager().assign(regionInfo);
      if (server.cpHost != null) {
        server.cpHost.postAssign(regionInfo);
      }
      return arr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MasterProtos.BalanceResponse balance(RpcController controller,
    MasterProtos.BalanceRequest request) throws ServiceException {
    try {
      return ProtobufUtil.toBalanceResponse(server.balance(ProtobufUtil.toBalanceRequest(request)));
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(RpcController controller,
     CreateNamespaceRequest request) throws ServiceException {
    try {
      long procId = server.createNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return CreateNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest req)
      throws ServiceException {
    TableDescriptor tableDescriptor = ProtobufUtil.toTableDescriptor(req.getTableSchema());
    byte [][] splitKeys = ProtobufUtil.getSplitKeysArray(req);
    try {
      long procId =
          server.createTable(tableDescriptor, splitKeys, req.getNonceGroup(), req.getNonce());
      LOG.info(server.getClientIdAuditPrefix() + " procedure request for creating table: " +
              req.getTableSchema().getTableName() + " procId is: " + procId);
      return CreateTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller,
      DeleteColumnRequest req) throws ServiceException {
    try {
      long procId = server.deleteColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        req.getColumnName().toByteArray(),
        req.getNonceGroup(),
        req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return DeleteColumnResponse.newBuilder().build();
      } else {
        return DeleteColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    try {
      long procId = server.deleteNamespace(
        request.getNamespaceName(),
        request.getNonceGroup(),
        request.getNonce());
      return DeleteNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Execute Delete Snapshot operation.
   * @return DeleteSnapshotResponse (a protobuf wrapped void) if the snapshot existed and was
   *    deleted properly.
   * @throws ServiceException wrapping SnapshotDoesNotExistException if specified snapshot did not
   *    exist.
   */
  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      server.snapshotManager.checkSnapshotSupport();

      LOG.info(server.getClientIdAuditPrefix() + " delete " + request.getSnapshot());
      server.snapshotManager.deleteSnapshot(request.getSnapshot());
      return DeleteSnapshotResponse.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller,
      DeleteTableRequest request) throws ServiceException {
    try {
      long procId = server.deleteTable(ProtobufUtil.toTableName(
          request.getTableName()), request.getNonceGroup(), request.getNonce());
      return DeleteTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
      throws ServiceException {
    try {
      long procId = server.truncateTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getPreserveSplits(),
        request.getNonceGroup(),
        request.getNonce());
      return TruncateTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller,
      DisableTableRequest request) throws ServiceException {
    try {
      long procId = server.disableTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getNonceGroup(),
        request.getNonce());
      return DisableTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController c,
      EnableCatalogJanitorRequest req) throws ServiceException {
    rpcPreCheck("enableCatalogJanitor");
    return EnableCatalogJanitorResponse.newBuilder().setPrevValue(
      server.catalogJanitorChore.setEnabled(req.getEnable())).build();
  }

  @Override
  public SetCleanerChoreRunningResponse setCleanerChoreRunning(
    RpcController c, SetCleanerChoreRunningRequest req) throws ServiceException {
    rpcPreCheck("setCleanerChoreRunning");

    boolean prevValue =
      server.getLogCleaner().getEnabled() && server.getHFileCleaner().getEnabled();
    server.getLogCleaner().setEnabled(req.getOn());
    server.getHFileCleaner().setEnabled(req.getOn());
    return SetCleanerChoreRunningResponse.newBuilder().setPrevValue(prevValue).build();
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller,
      EnableTableRequest request) throws ServiceException {
    try {
      long procId = server.enableTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getNonceGroup(),
        request.getNonce());
      return EnableTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MergeTableRegionsResponse mergeTableRegions(
      RpcController c, MergeTableRegionsRequest request) throws ServiceException {
    try {
      server.checkInitialized();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    RegionStates regionStates = server.getAssignmentManager().getRegionStates();

    RegionInfo[] regionsToMerge = new RegionInfo[request.getRegionCount()];
    for (int i = 0; i < request.getRegionCount(); i++) {
      final byte[] encodedNameOfRegion = request.getRegion(i).getValue().toByteArray();
      if (request.getRegion(i).getType() != RegionSpecifierType.ENCODED_REGION_NAME) {
        LOG.warn("MergeRegions specifier type: expected: "
          + RegionSpecifierType.ENCODED_REGION_NAME + " actual: region " + i + " ="
          + request.getRegion(i).getType());
      }
      RegionState regionState = regionStates.getRegionState(Bytes.toString(encodedNameOfRegion));
      if (regionState == null) {
        throw new ServiceException(
          new UnknownRegionException(Bytes.toStringBinary(encodedNameOfRegion)));
      }
      regionsToMerge[i] = regionState.getRegion();
    }

    try {
      long procId = server.mergeRegions(
        regionsToMerge,
        request.getForcible(),
        request.getNonceGroup(),
        request.getNonce());
      return MergeTableRegionsResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public SplitTableRegionResponse splitRegion(final RpcController controller,
      final SplitTableRegionRequest request) throws ServiceException {
    try {
      long procId = server.splitRegion(
        ProtobufUtil.toRegionInfo(request.getRegionInfo()),
        request.hasSplitRow() ? request.getSplitRow().toByteArray() : null,
        request.getNonceGroup(),
        request.getNonce());
      return SplitTableRegionResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public ClientProtos.CoprocessorServiceResponse execMasterService(final RpcController controller,
      final ClientProtos.CoprocessorServiceRequest request) throws ServiceException {
    rpcPreCheck("execMasterService");
    try {
      ServerRpcController execController = new ServerRpcController();
      ClientProtos.CoprocessorServiceCall call = request.getCall();
      String serviceName = call.getServiceName();
      String methodName = call.getMethodName();
      if (!server.coprocessorServiceHandlers.containsKey(serviceName)) {
        throw new UnknownProtocolException(null,
          "No registered Master Coprocessor Endpoint found for " + serviceName +
          ". Has it been enabled?");
      }

      Service service = server.coprocessorServiceHandlers.get(serviceName);
      ServiceDescriptor serviceDesc = service.getDescriptorForType();
      MethodDescriptor methodDesc =
          CoprocessorRpcUtils.getMethodDescriptor(methodName, serviceDesc);

      Message execRequest =
          CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());
      final Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, execController, execRequest,
        (message) -> {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        });
      Message execResult = responseBuilder.build();
      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }

      String remoteAddress = RpcServer.getRemoteAddress().map(InetAddress::toString).orElse("");
      User caller = RpcServer.getRequestUser().orElse(null);
      AUDITLOG.info("User {} (remote address: {}) master service request for {}.{}", caller,
        remoteAddress, serviceName, methodName);

      return CoprocessorRpcUtils.getResponse(execResult, HConstants.EMPTY_BYTE_ARRAY);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Triggers an asynchronous attempt to run a distributed procedure.
   * {@inheritDoc}
   */
  @Override
  public ExecProcedureResponse execProcedure(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = server.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException(new DoNotRetryIOException("The procedure is not registered: "
          + desc.getSignature()));
      }
      LOG.info(server.getClientIdAuditPrefix() + " procedure request for: " + desc.getSignature());
      mpm.checkPermissions(desc, getAccessChecker(), RpcServer.getRequestUser().orElse(null));
      mpm.execProcedure(desc);
      // send back the max amount of time the client should wait for the procedure
      // to complete
      long waitTime = SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME;
      return ExecProcedureResponse.newBuilder().setExpectedTimeout(
        waitTime).build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Triggers a synchronous attempt to run a distributed procedure and sets
   * return data in response.
   * {@inheritDoc}
   */
  @Override
  public ExecProcedureResponse execProcedureWithRet(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    rpcPreCheck("execProcedureWithRet");
    try {
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm =
        server.getMasterProcedureManagerHost().getProcedureManager(desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: " + desc.getSignature());
      }
      LOG.info(server.getClientIdAuditPrefix() + " procedure request for: " + desc.getSignature());
      byte[] data = mpm.execProcedureWithRet(desc);
      ExecProcedureResponse.Builder builder = ExecProcedureResponse.newBuilder();
      // set return data if available
      if (data != null) {
        builder.setReturnData(UnsafeByteOperations.unsafeWrap(data));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest req) throws ServiceException {
    GetClusterStatusResponse.Builder response = GetClusterStatusResponse.newBuilder();
    try {
      // We used to check if Master was up at this point but let this call proceed even if
      // Master is initializing... else we shut out stuff like hbck2 tool from making progress
      // since it queries this method to figure cluster version. hbck2 wants to be able to work
      // against Master even if it is 'initializing' so it can do fixup.
      response.setClusterStatus(ClusterMetricsBuilder.toClusterStatus(
        server.getClusterMetrics(ClusterMetricsBuilder.toOptions(req.getOptionsList()))));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  /**
   * List the currently available/stored snapshots. Any in-progress snapshots are ignored
   */
  @Override
  public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
      GetCompletedSnapshotsRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      GetCompletedSnapshotsResponse.Builder builder = GetCompletedSnapshotsResponse.newBuilder();
      List<SnapshotDescription> snapshots = server.snapshotManager.getCompletedSnapshots();

      // convert to protobuf
      for (SnapshotDescription snapshot : snapshots) {
        builder.addSnapshots(snapshot);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      RpcController controller, ListNamespacesRequest request)
      throws ServiceException {
    try {
      return ListNamespacesResponse.newBuilder()
        .addAllNamespaceName(server.listNamespaces())
        .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNamespaceDescriptorResponse getNamespaceDescriptor(
      RpcController controller, GetNamespaceDescriptorRequest request)
      throws ServiceException {
    try {
      return GetNamespaceDescriptorResponse.newBuilder()
        .setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(
            server.getNamespace(request.getNamespaceName())))
        .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get the number of regions of the table that have been updated by the alter.
   *
   * @return Pair indicating the number of regions updated Pair.getFirst is the
   *         regions that are yet to be updated Pair.getSecond is the total number
   *         of regions of the table
   * @throws ServiceException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
      RpcController controller, GetSchemaAlterStatusRequest req) throws ServiceException {
    // TODO: currently, we query using the table name on the client side. this
    // may overlap with other table operations or the table operation may
    // have completed before querying this API. We need to refactor to a
    // transaction system in the future to avoid these ambiguities.
    TableName tableName = ProtobufUtil.toTableName(req.getTableName());

    try {
      server.checkInitialized();
      Pair<Integer,Integer> pair = server.getAssignmentManager().getReopenStatus(tableName);
      GetSchemaAlterStatusResponse.Builder ret = GetSchemaAlterStatusResponse.newBuilder();
      ret.setYetToUpdateRegions(pair.getFirst());
      ret.setTotalRegions(pair.getSecond());
      return ret.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Get list of TableDescriptors for requested tables.
   * @param c Unused (set to null).
   * @param req GetTableDescriptorsRequest that contains:
   *     - tableNames: requested tables, or if empty, all are requested.
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  @Override
  public GetTableDescriptorsResponse getTableDescriptors(RpcController c,
      GetTableDescriptorsRequest req) throws ServiceException {
    try {
      server.checkInitialized();

      final String regex = req.hasRegex() ? req.getRegex() : null;
      final String namespace = req.hasNamespace() ? req.getNamespace() : null;
      List<TableName> tableNameList = null;
      if (req.getTableNamesCount() > 0) {
        tableNameList = new ArrayList<TableName>(req.getTableNamesCount());
        for (HBaseProtos.TableName tableNamePB: req.getTableNamesList()) {
          tableNameList.add(ProtobufUtil.toTableName(tableNamePB));
        }
      }

      List<TableDescriptor> descriptors = server.listTableDescriptors(namespace, regex,
          tableNameList, req.getIncludeSysTables());

      GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
      if (descriptors != null && descriptors.size() > 0) {
        // Add the table descriptors to the response
        for (TableDescriptor htd: descriptors) {
          builder.addTableSchema(ProtobufUtil.toTableSchema(htd));
        }
      }
      return builder.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Get list of userspace table names
   * @param controller Unused (set to null).
   * @param req GetTableNamesRequest
   * @return GetTableNamesResponse
   * @throws ServiceException
   */
  @Override
  public GetTableNamesResponse getTableNames(RpcController controller,
      GetTableNamesRequest req) throws ServiceException {
    try {
      server.checkServiceStarted();

      final String regex = req.hasRegex() ? req.getRegex() : null;
      final String namespace = req.hasNamespace() ? req.getNamespace() : null;
      List<TableName> tableNames = server.listTableNames(namespace, regex,
          req.getIncludeSysTables());

      GetTableNamesResponse.Builder builder = GetTableNamesResponse.newBuilder();
      if (tableNames != null && tableNames.size() > 0) {
        // Add the table names to the response
        for (TableName table: tableNames) {
          builder.addTableNames(ProtobufUtil.toProtoTableName(table));
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTableStateResponse getTableState(RpcController controller,
      GetTableStateRequest request) throws ServiceException {
    try {
      server.checkServiceStarted();
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      TableState ts = server.getTableStateManager().getTableState(tableName);
      GetTableStateResponse.Builder builder = GetTableStateResponse.newBuilder();
      builder.setTableState(ts.convert());
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController c,
      IsCatalogJanitorEnabledRequest req) throws ServiceException {
    return IsCatalogJanitorEnabledResponse.newBuilder().setValue(
      server.isCatalogJanitorEnabled()).build();
  }

  @Override
  public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController c,
                                                             IsCleanerChoreEnabledRequest req)
    throws ServiceException {
    return IsCleanerChoreEnabledResponse.newBuilder().setValue(server.isCleanerChoreEnabled())
                                        .build();
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c,
      IsMasterRunningRequest req) throws ServiceException {
    try {
      server.checkServiceStarted();
      return IsMasterRunningResponse.newBuilder().setIsMasterRunning(
        !server.isStopped()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Checks if the specified procedure is done.
   * @return true if the procedure is done, false if the procedure is in the process of completing
   * @throws ServiceException if invalid procedure or failed procedure with progress failure reason.
   */
  @Override
  public IsProcedureDoneResponse isProcedureDone(RpcController controller,
      IsProcedureDoneRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = server.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
      }
      LOG.debug("Checking to see if procedure from request:"
        + desc.getSignature() + " is done");

      IsProcedureDoneResponse.Builder builder =
        IsProcedureDoneResponse.newBuilder();
      boolean done = mpm.isProcedureDone(desc);
      builder.setDone(done);
      return builder.build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Checks if the specified snapshot is done.
   * @return true if the snapshot is in file system ready to use,
   *     false if the snapshot is in the process of completing
   * @throws ServiceException wrapping UnknownSnapshotException if invalid snapshot, or
   *     a wrapped HBaseSnapshotException with progress failure reason.
   */
  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException {
    LOG.debug("Checking to see if snapshot from request:" +
      ClientSnapshotDescriptionUtils.toString(request.getSnapshot()) + " is done");
    try {
      server.checkInitialized();
      IsSnapshotDoneResponse.Builder builder = IsSnapshotDoneResponse.newBuilder();
      boolean done = server.snapshotManager.isSnapshotDone(request.getSnapshot());
      builder.setDone(done);
      return builder.build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetProcedureResultResponse getProcedureResult(RpcController controller,
      GetProcedureResultRequest request) throws ServiceException {
    LOG.debug("Checking to see if procedure is done pid=" + request.getProcId());
    try {
      server.checkInitialized();
      GetProcedureResultResponse.Builder builder = GetProcedureResultResponse.newBuilder();
      long procId = request.getProcId();
      ProcedureExecutor<?> executor = server.getMasterProcedureExecutor();
      Procedure<?> result = executor.getResultOrProcedure(procId);
      if (result != null) {
        builder.setSubmittedTime(result.getSubmittedTime());
        builder.setLastUpdate(result.getLastUpdate());
        if (executor.isFinished(procId)) {
          builder.setState(GetProcedureResultResponse.State.FINISHED);
          if (result.isFailed()) {
            IOException exception =
                MasterProcedureUtil.unwrapRemoteIOException(result);
            builder.setException(ForeignExceptionUtil.toProtoForeignException(exception));
          }
          byte[] resultData = result.getResult();
          if (resultData != null) {
            builder.setResult(UnsafeByteOperations.unsafeWrap(resultData));
          }
          server.getMasterProcedureExecutor().removeResult(request.getProcId());
        } else {
          builder.setState(GetProcedureResultResponse.State.RUNNING);
        }
      } else {
        builder.setState(GetProcedureResultResponse.State.NOT_FOUND);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AbortProcedureResponse abortProcedure(
      RpcController rpcController, AbortProcedureRequest request) throws ServiceException {
    try {
      AbortProcedureResponse.Builder response = AbortProcedureResponse.newBuilder();
      boolean abortResult =
          server.abortProcedure(request.getProcId(), request.getMayInterruptIfRunning());
      response.setIsProcedureAborted(abortResult);
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController c,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    try {
      ListNamespaceDescriptorsResponse.Builder response =
        ListNamespaceDescriptorsResponse.newBuilder();
      for(NamespaceDescriptor ns: server.getNamespaces()) {
        response.addNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(ns));
      }
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetProceduresResponse getProcedures(
      RpcController rpcController,
      GetProceduresRequest request) throws ServiceException {
    try {
      final GetProceduresResponse.Builder response = GetProceduresResponse.newBuilder();
      for (Procedure<?> p: server.getProcedures()) {
        response.addProcedure(ProcedureUtil.convertToProtoProcedure(p));
      }
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLocksResponse getLocks(
      RpcController controller,
      GetLocksRequest request) throws ServiceException {
    try {
      final GetLocksResponse.Builder builder = GetLocksResponse.newBuilder();

      for (LockedResource lockedResource: server.getLocks()) {
        builder.addLock(ProcedureUtil.convertToProtoLockedResource(lockedResource));
      }

      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(RpcController c,
      ListTableDescriptorsByNamespaceRequest request) throws ServiceException {
    try {
      ListTableDescriptorsByNamespaceResponse.Builder b =
        ListTableDescriptorsByNamespaceResponse.newBuilder();
      for (TableDescriptor htd : server
        .listTableDescriptorsByNamespace(request.getNamespaceName())) {
        b.addTableSchema(ProtobufUtil.toTableSchema(htd));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController c,
      ListTableNamesByNamespaceRequest request) throws ServiceException {
    try {
      ListTableNamesByNamespaceResponse.Builder b =
        ListTableNamesByNamespaceResponse.newBuilder();
      for (TableName tableName: server.listTableNamesByNamespace(request.getNamespaceName())) {
        b.addTableName(ProtobufUtil.toProtoTableName(tableName));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller,
      ModifyColumnRequest req) throws ServiceException {
    try {
      long procId = server.modifyColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        ProtobufUtil.toColumnFamilyDescriptor(req.getColumnFamilies()),
        req.getNonceGroup(),
        req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return ModifyColumnResponse.newBuilder().build();
      } else {
        return ModifyColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ModifyColumnStoreFileTrackerResponse modifyColumnStoreFileTracker(
    RpcController controller, ModifyColumnStoreFileTrackerRequest req)
    throws ServiceException {
    try {
      long procId =
        server.modifyColumnStoreFileTracker(ProtobufUtil.toTableName(req.getTableName()),
          req.getFamily().toByteArray(), req.getDstSft(), req.getNonceGroup(), req.getNonce());
      return ModifyColumnStoreFileTrackerResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    try {
      long procId = server.modifyNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return ModifyNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller,
      ModifyTableRequest req) throws ServiceException {
    try {
      long procId = server.modifyTable(
        ProtobufUtil.toTableName(req.getTableName()),
        ProtobufUtil.toTableDescriptor(req.getTableSchema()),
        req.getNonceGroup(),
        req.getNonce());
      return ModifyTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ModifyTableStoreFileTrackerResponse modifyTableStoreFileTracker(RpcController controller,
    ModifyTableStoreFileTrackerRequest req) throws ServiceException {
    try {
      long procId = server.modifyTableStoreFileTracker(ProtobufUtil.toTableName(req.getTableName()),
        req.getDstSft(), req.getNonceGroup(), req.getNonce());
      return ModifyTableStoreFileTrackerResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MoveRegionResponse moveRegion(RpcController controller,
      MoveRegionRequest req) throws ServiceException {
    final byte [] encodedRegionName = req.getRegion().getValue().toByteArray();
    RegionSpecifierType type = req.getRegion().getType();
    final byte [] destServerName = (req.hasDestServerName())?
      Bytes.toBytes(ProtobufUtil.toServerName(req.getDestServerName()).getServerName()):null;
    MoveRegionResponse mrr = MoveRegionResponse.newBuilder().build();

    if (type != RegionSpecifierType.ENCODED_REGION_NAME) {
      LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.ENCODED_REGION_NAME
        + " actual: " + type);
    }

    try {
      server.checkInitialized();
      server.move(encodedRegionName, destServerName);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return mrr;
  }

  /**
   * Offline specified region from master's in-memory state. It will not attempt to
   * reassign the region as in unassign.
   *
   * This is a special method that should be used by experts or hbck.
   *
   */
  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller,
      OfflineRegionRequest request) throws ServiceException {
    try {
      server.checkInitialized();

      final RegionSpecifierType type = request.getRegion().getType();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }

      final byte[] regionName = request.getRegion().getValue().toByteArray();
      final RegionInfo hri = server.getAssignmentManager().getRegionInfo(regionName);
      if (hri == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionName));
      }

      if (server.cpHost != null) {
        server.cpHost.preRegionOffline(hri);
      }
      LOG.info(server.getClientIdAuditPrefix() + " offline " + hri.getRegionNameAsString());
      server.getAssignmentManager().offlineRegion(hri);
      if (server.cpHost != null) {
        server.cpHost.postRegionOffline(hri);
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return OfflineRegionResponse.newBuilder().build();
  }

  /**
   * Execute Restore/Clone snapshot operation.
   *
   * <p>If the specified table exists a "Restore" is executed, replacing the table
   * schema and directory data with the content of the snapshot.
   * The table must be disabled, or a UnsupportedOperationException will be thrown.
   *
   * <p>If the table doesn't exist a "Clone" is executed, a new table is created
   * using the schema at the time of the snapshot, and the content of the snapshot.
   *
   * <p>The restore/clone operation does not require copying HFiles. Since HFiles
   * are immutable the table can point to and use the same files as the original one.
   */
  @Override
  public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
      RestoreSnapshotRequest request) throws ServiceException {
    try {
      long procId = server.restoreSnapshot(request.getSnapshot(), request.getNonceGroup(),
        request.getNonce(), request.getRestoreACL(), request.getCustomSFT());
      return RestoreSnapshotResponse.newBuilder().setProcId(procId).build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetSnapshotCleanupResponse switchSnapshotCleanup(
      RpcController controller, SetSnapshotCleanupRequest request)
      throws ServiceException {
    try {
      server.checkInitialized();
      final boolean enabled = request.getEnabled();
      final boolean isSynchronous = request.hasSynchronous() && request.getSynchronous();
      final boolean prevSnapshotCleanupRunning = this.switchSnapshotCleanup(enabled, isSynchronous);
      return SetSnapshotCleanupResponse.newBuilder()
          .setPrevSnapshotCleanup(prevSnapshotCleanupRunning).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(
      RpcController controller, IsSnapshotCleanupEnabledRequest request)
      throws ServiceException {
    try {
      server.checkInitialized();
      final boolean isSnapshotCleanupEnabled = server.snapshotCleanupTracker
          .isSnapshotCleanupEnabled();
      return IsSnapshotCleanupEnabledResponse.newBuilder()
          .setEnabled(isSnapshotCleanupEnabled).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Turn on/off snapshot auto-cleanup based on TTL
   *
   * @param enabledNewVal Set to <code>true</code> to enable, <code>false</code> to disable
   * @param synchronous If <code>true</code>, it waits until current snapshot cleanup is completed,
   *   if outstanding
   * @return previous snapshot auto-cleanup mode
   */
  private synchronized boolean switchSnapshotCleanup(final boolean enabledNewVal,
    final boolean synchronous) {
    final boolean oldValue = server.snapshotCleanupTracker.isSnapshotCleanupEnabled();
    server.switchSnapshotCleanup(enabledNewVal, synchronous);
    LOG.info("{} Successfully set snapshot cleanup to {}", server.getClientIdAuditPrefix(),
      enabledNewVal);
    return oldValue;
  }


  @Override
  public RunCatalogScanResponse runCatalogScan(RpcController c,
      RunCatalogScanRequest req) throws ServiceException {
    rpcPreCheck("runCatalogScan");
    try {
      return ResponseConverter.buildRunCatalogScanResponse(
          this.server.catalogJanitorChore.scan());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RunCleanerChoreResponse runCleanerChore(RpcController c, RunCleanerChoreRequest req)
    throws ServiceException {
    rpcPreCheck("runCleanerChore");
    boolean result = server.getHFileCleaner().runCleaner() && server.getLogCleaner().runCleaner();
    return ResponseConverter.buildRunCleanerChoreResponse(result);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(RpcController c,
      SetBalancerRunningRequest req) throws ServiceException {
    try {
      server.checkInitialized();
      boolean prevValue = (req.getSynchronous())?
        synchronousBalanceSwitch(req.getOn()) : server.balanceSwitch(req.getOn());
      return SetBalancerRunningResponse.newBuilder().setPrevBalanceValue(prevValue).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller,
      ShutdownRequest request) throws ServiceException {
    LOG.info(server.getClientIdAuditPrefix() + " shutdown");
    try {
      server.shutdown();
    } catch (IOException e) {
      LOG.error("Exception occurred in HMaster.shutdown()", e);
      throw new ServiceException(e);
    }
    return ShutdownResponse.newBuilder().build();
  }

  /**
   * Triggers an asynchronous attempt to take a snapshot.
   * {@inheritDoc}
   */
  @Override
  public SnapshotResponse snapshot(RpcController controller,
      SnapshotRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      server.snapshotManager.checkSnapshotSupport();

      LOG.info(server.getClientIdAuditPrefix() + " snapshot request for:" +
        ClientSnapshotDescriptionUtils.toString(request.getSnapshot()));
      // get the snapshot information
      SnapshotDescription snapshot = SnapshotDescriptionUtils.validate(
        request.getSnapshot(), server.getConfiguration());
      // send back the max amount of time the client should wait for the snapshot to complete
      long waitTime = SnapshotDescriptionUtils.getMaxMasterTimeout(server.getConfiguration(),
        snapshot.getType(), SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);

      SnapshotResponse.Builder builder = SnapshotResponse.newBuilder().setExpectedTimeout(waitTime);

      // If there is nonce group and nonce in the snapshot request, then the client can
      // handle snapshot procedure procId. And if enable the snapshot procedure, we
      // will do the snapshot work with proc-v2, otherwise we will fall back to zk proc.
      if (request.hasNonceGroup() && request.hasNonce() &&
          server.snapshotManager.snapshotProcedureEnabled()) {
        long nonceGroup = request.getNonceGroup();
        long nonce = request.getNonce();
        long procId = server.snapshotManager.takeSnapshot(snapshot, nonceGroup, nonce);
        return builder.setProcId(procId).build();
      } else {
        server.snapshotManager.takeSnapshot(snapshot);
        return builder.build();
      }
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller,
      StopMasterRequest request) throws ServiceException {
    LOG.info(server.getClientIdAuditPrefix() + " stop");
    try {
      server.stopMaster();
    } catch (IOException e) {
      LOG.error("Exception occurred while stopping master", e);
      throw new ServiceException(e);
    }
    return StopMasterResponse.newBuilder().build();
  }

  @Override
  public IsInMaintenanceModeResponse isMasterInMaintenanceMode(
      final RpcController controller,
      final IsInMaintenanceModeRequest request) throws ServiceException {
    IsInMaintenanceModeResponse.Builder response = IsInMaintenanceModeResponse.newBuilder();
    response.setInMaintenanceMode(server.isInMaintenanceMode());
    return response.build();
  }

  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller,
      UnassignRegionRequest req) throws ServiceException {
    try {
      final byte [] regionName = req.getRegion().getValue().toByteArray();
      RegionSpecifierType type = req.getRegion().getType();
      UnassignRegionResponse urr = UnassignRegionResponse.newBuilder().build();

      server.checkInitialized();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("unassignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }
      RegionStateNode rsn =
        server.getAssignmentManager().getRegionStates().getRegionStateNodeFromName(regionName);
      if (rsn == null) {
        throw new UnknownRegionException(Bytes.toString(regionName));
      }

      RegionInfo hri = rsn.getRegionInfo();
      if (server.cpHost != null) {
        server.cpHost.preUnassign(hri);
      }
      LOG.debug(server.getClientIdAuditPrefix() + " unassign " + hri.getRegionNameAsString()
          + " in current location if it is online");
      server.getAssignmentManager().unassign(hri);
      if (server.cpHost != null) {
        server.cpHost.postUnassign(hri);
      }

      return urr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ReportRegionStateTransitionResponse reportRegionStateTransition(RpcController c,
      ReportRegionStateTransitionRequest req) throws ServiceException {
    try {
      server.checkServiceStarted();
      return server.getAssignmentManager().reportRegionStateTransition(req);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public SetQuotaResponse setQuota(RpcController c, SetQuotaRequest req)
      throws ServiceException {
    try {
      server.checkInitialized();
      return server.getMasterQuotaManager().setQuota(req);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller,
      MajorCompactionTimestampRequest request) throws ServiceException {
    MajorCompactionTimestampResponse.Builder response =
        MajorCompactionTimestampResponse.newBuilder();
    try {
      server.checkInitialized();
      response.setCompactionTimestamp(server.getLastMajorCompactionTimestamp(ProtobufUtil
          .toTableName(request.getTableName())));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(
      RpcController controller, MajorCompactionTimestampForRegionRequest request)
      throws ServiceException {
    MajorCompactionTimestampResponse.Builder response =
        MajorCompactionTimestampResponse.newBuilder();
    try {
      server.checkInitialized();
      response.setCompactionTimestamp(server.getLastMajorCompactionTimestampForRegion(request
          .getRegion().getValue().toByteArray()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
      IsBalancerEnabledRequest request) throws ServiceException {
    IsBalancerEnabledResponse.Builder response = IsBalancerEnabledResponse.newBuilder();
    response.setEnabled(server.isBalancerOn());
    return response.build();
  }

  @Override
  public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller,
    SetSplitOrMergeEnabledRequest request) throws ServiceException {
    SetSplitOrMergeEnabledResponse.Builder response = SetSplitOrMergeEnabledResponse.newBuilder();
    try {
      server.checkInitialized();
      boolean newValue = request.getEnabled();
      for (MasterProtos.MasterSwitchType masterSwitchType: request.getSwitchTypesList()) {
        MasterSwitchType switchType = convert(masterSwitchType);
        boolean oldValue = server.isSplitOrMergeEnabled(switchType);
        response.addPrevValue(oldValue);
        if (server.cpHost != null) {
          server.cpHost.preSetSplitOrMergeEnabled(newValue, switchType);
        }
        server.getSplitOrMergeTracker().setSplitOrMergeEnabled(newValue, switchType);
        if (server.cpHost != null) {
          server.cpHost.postSetSplitOrMergeEnabled(newValue, switchType);
        }
      }
    } catch (IOException | KeeperException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
    IsSplitOrMergeEnabledRequest request) throws ServiceException {
    IsSplitOrMergeEnabledResponse.Builder response = IsSplitOrMergeEnabledResponse.newBuilder();
    response.setEnabled(server.isSplitOrMergeEnabled(convert(request.getSwitchType())));
    return response.build();
  }

  @Override
  public NormalizeResponse normalize(RpcController controller,
      NormalizeRequest request) throws ServiceException {
    rpcPreCheck("normalize");
    try {
      final NormalizeTableFilterParams ntfp = new NormalizeTableFilterParams.Builder()
        .tableNames(ProtobufUtil.toTableNameList(request.getTableNamesList()))
        .regex(request.hasRegex() ? request.getRegex() : null)
        .namespace(request.hasNamespace() ? request.getNamespace() : null)
        .build();
      return NormalizeResponse.newBuilder()
        // all API requests are considered priority requests.
        .setNormalizerRan(server.normalizeRegions(ntfp, true))
        .build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller,
      SetNormalizerRunningRequest request) throws ServiceException {
    rpcPreCheck("setNormalizerRunning");

    // Sets normalizer on/off flag in ZK.
    // TODO: this method is totally broken in terms of atomicity of actions and values read.
    //  1. The contract has this RPC returning the previous value. There isn't a ZKUtil method
    //     that lets us retrieve the previous value as part of setting a new value, so we simply
    //     perform a read before issuing the update. Thus we have a data race opportunity, between
    //     when the `prevValue` is read and whatever is actually overwritten.
    //  2. Down in `setNormalizerOn`, the call to `createAndWatch` inside of the catch clause can
    //     itself fail in the event that the znode already exists. Thus, another data race, between
    //     when the initial `setData` call is notified of the absence of the target znode and the
    //     subsequent `createAndWatch`, with another client creating said node.
    //  That said, there's supposed to be only one active master and thus there's supposed to be
    //  only one process with the authority to modify the value.
    final boolean prevValue = server.getRegionNormalizerManager().isNormalizerOn();
    final boolean newValue = request.getOn();
    server.getRegionNormalizerManager().setNormalizerOn(newValue);
    LOG.info("{} set normalizerSwitch={}", server.getClientIdAuditPrefix(), newValue);
    return SetNormalizerRunningResponse.newBuilder().setPrevNormalizerValue(prevValue).build();
  }

  @Override
  public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
    IsNormalizerEnabledRequest request) {
    IsNormalizerEnabledResponse.Builder response = IsNormalizerEnabledResponse.newBuilder();
    response.setEnabled(server.isNormalizerOn());
    return response.build();
  }

  /**
   * Returns the security capabilities in effect on the cluster
   */
  @Override
  public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
      SecurityCapabilitiesRequest request) throws ServiceException {
    SecurityCapabilitiesResponse.Builder response = SecurityCapabilitiesResponse.newBuilder();
    try {
      server.checkInitialized();
      Set<SecurityCapabilitiesResponse.Capability> capabilities = new HashSet<>();
      // Authentication
      if (User.isHBaseSecurityEnabled(server.getConfiguration())) {
        capabilities.add(SecurityCapabilitiesResponse.Capability.SECURE_AUTHENTICATION);
      } else {
        capabilities.add(SecurityCapabilitiesResponse.Capability.SIMPLE_AUTHENTICATION);
      }
      // A coprocessor that implements AccessControlService can provide AUTHORIZATION and
      // CELL_AUTHORIZATION
      if (server.cpHost != null && hasAccessControlServiceCoprocessor(server.cpHost)) {
        if (AccessChecker.isAuthorizationSupported(server.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.AUTHORIZATION);
        }
        if (AccessController.isCellAuthorizationSupported(server.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.CELL_AUTHORIZATION);
        }
      }
      // A coprocessor that implements VisibilityLabelsService can provide CELL_VISIBILITY.
      if (server.cpHost != null && hasVisibilityLabelsServiceCoprocessor(server.cpHost)) {
        if (VisibilityController.isCellAuthorizationSupported(server.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.CELL_VISIBILITY);
        }
      }
      response.addAllCapabilities(capabilities);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  /**
   * Determines if there is a MasterCoprocessor deployed which implements
   * {@link AccessControlService.Interface}.
   */
  boolean hasAccessControlServiceCoprocessor(MasterCoprocessorHost cpHost) {
    return checkCoprocessorWithService(cpHost.findCoprocessors(MasterCoprocessor.class),
      AccessControlService.Interface.class);
  }

  /**
   * Determines if there is a MasterCoprocessor deployed which implements
   * {@link VisibilityLabelsService.Interface}.
   */
  boolean hasVisibilityLabelsServiceCoprocessor(MasterCoprocessorHost cpHost) {
    return checkCoprocessorWithService(cpHost.findCoprocessors(MasterCoprocessor.class),
      VisibilityLabelsService.Interface.class);
  }

  /**
   * Determines if there is a coprocessor implementation in the provided argument which extends
   * or implements the provided {@code service}.
   */
  boolean checkCoprocessorWithService(
      List<MasterCoprocessor> coprocessorsToCheck, Class<?> service) {
    if (coprocessorsToCheck == null || coprocessorsToCheck.isEmpty()) {
      return false;
    }
    for (MasterCoprocessor cp : coprocessorsToCheck) {
      if (service.isAssignableFrom(cp.getClass())) {
        return true;
      }
    }
    return false;
  }

  private MasterSwitchType convert(MasterProtos.MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return MasterSwitchType.SPLIT;
      case MERGE:
        return MasterSwitchType.MERGE;
      default:
        break;
    }
    return null;
  }

  @Override
  public AddReplicationPeerResponse addReplicationPeer(RpcController controller,
      AddReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = server.addReplicationPeer(request.getPeerId(),
        ReplicationPeerConfigUtil.convert(request.getPeerConfig()),
        request.getPeerState().getState().equals(ReplicationState.State.ENABLED));
      return AddReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
      RemoveReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = server.removeReplicationPeer(request.getPeerId());
      return RemoveReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
      EnableReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = server.enableReplicationPeer(request.getPeerId());
      return EnableReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
      DisableReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = server.disableReplicationPeer(request.getPeerId());
      return DisableReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
      GetReplicationPeerConfigRequest request) throws ServiceException {
    GetReplicationPeerConfigResponse.Builder response = GetReplicationPeerConfigResponse
        .newBuilder();
    try {
      String peerId = request.getPeerId();
      ReplicationPeerConfig peerConfig = server.getReplicationPeerConfig(peerId);
      response.setPeerId(peerId);
      response.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller,
      UpdateReplicationPeerConfigRequest request) throws ServiceException {
    try {
      long procId = server.updateReplicationPeerConfig(request.getPeerId(),
        ReplicationPeerConfigUtil.convert(request.getPeerConfig()));
      return UpdateReplicationPeerConfigResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TransitReplicationPeerSyncReplicationStateResponse
      transitReplicationPeerSyncReplicationState(RpcController controller,
          TransitReplicationPeerSyncReplicationStateRequest request) throws ServiceException {
    try {
      long procId = server.transitReplicationPeerSyncReplicationState(request.getPeerId(),
        ReplicationPeerConfigUtil.toSyncReplicationState(request.getSyncReplicationState()));
      return TransitReplicationPeerSyncReplicationStateResponse.newBuilder().setProcId(procId)
          .build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
      ListReplicationPeersRequest request) throws ServiceException {
    ListReplicationPeersResponse.Builder response = ListReplicationPeersResponse.newBuilder();
    try {
      List<ReplicationPeerDescription> peers = server
          .listReplicationPeers(request.hasRegex() ? request.getRegex() : null);
      for (ReplicationPeerDescription peer : peers) {
        response.addPeerDesc(ReplicationPeerConfigUtil.toProtoReplicationPeerDescription(peer));
      }
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(
      RpcController controller, ListDecommissionedRegionServersRequest request)
      throws ServiceException {
    ListDecommissionedRegionServersResponse.Builder response =
        ListDecommissionedRegionServersResponse.newBuilder();
    try {
      server.checkInitialized();
      if (server.cpHost != null) {
        server.cpHost.preListDecommissionedRegionServers();
      }
      List<ServerName> servers = server.listDecommissionedRegionServers();
      response.addAllServerName((servers.stream().map(server -> ProtobufUtil.toServerName(server)))
          .collect(Collectors.toList()));
      if (server.cpHost != null) {
        server.cpHost.postListDecommissionedRegionServers();
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return response.build();
  }

  @Override
  public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller,
      DecommissionRegionServersRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      List<ServerName> servers = request.getServerNameList().stream()
          .map(pbServer -> ProtobufUtil.toServerName(pbServer)).collect(Collectors.toList());
      boolean offload = request.getOffload();
      if (server.cpHost != null) {
        server.cpHost.preDecommissionRegionServers(servers, offload);
      }
      server.decommissionRegionServers(servers, offload);
      if (server.cpHost != null) {
        server.cpHost.postDecommissionRegionServers(servers, offload);
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return DecommissionRegionServersResponse.newBuilder().build();
  }

  @Override
  public RecommissionRegionServerResponse recommissionRegionServer(RpcController controller,
      RecommissionRegionServerRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      ServerName sn = ProtobufUtil.toServerName(request.getServerName());
      List<byte[]> encodedRegionNames = request.getRegionList().stream()
          .map(regionSpecifier -> regionSpecifier.getValue().toByteArray())
          .collect(Collectors.toList());
      if (server.cpHost != null) {
        server.cpHost.preRecommissionRegionServer(sn, encodedRegionNames);
      }
      server.recommissionRegionServer(sn, encodedRegionNames);
      if (server.cpHost != null) {
        server.cpHost.postRecommissionRegionServer(sn, encodedRegionNames);
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return RecommissionRegionServerResponse.newBuilder().build();
  }

  @Override
  public LockResponse requestLock(RpcController controller, final LockRequest request)
      throws ServiceException {
    try {
      if (request.getDescription().isEmpty()) {
        throw new IllegalArgumentException("Empty description");
      }
      NonceProcedureRunnable npr;
      LockType type = LockType.valueOf(request.getLockType().name());
      if (request.getRegionInfoCount() > 0) {
        final RegionInfo[] regionInfos = new RegionInfo[request.getRegionInfoCount()];
        for (int i = 0; i < request.getRegionInfoCount(); ++i) {
          regionInfos[i] = ProtobufUtil.toRegionInfo(request.getRegionInfo(i));
        }
        npr = new NonceProcedureRunnable(server, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(server.getLockManager().remoteLocks().requestRegionsLock(regionInfos,
                request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else if (request.hasTableName()) {
        final TableName tableName = ProtobufUtil.toTableName(request.getTableName());
        npr = new NonceProcedureRunnable(server, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(server.getLockManager().remoteLocks().requestTableLock(tableName, type,
                request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else if (request.hasNamespace()) {
        npr = new NonceProcedureRunnable(server, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(server.getLockManager().remoteLocks().requestNamespaceLock(
                request.getNamespace(), type, request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else {
        throw new IllegalArgumentException("one of table/namespace/region should be specified");
      }
      long procId = MasterProcedureUtil.submitProcedure(npr);
      return LockResponse.newBuilder().setProcId(procId).build();
    } catch (IllegalArgumentException e) {
      LOG.warn("Exception when queuing lock", e);
      throw new ServiceException(new DoNotRetryIOException(e));
    } catch (IOException e) {
      LOG.warn("Exception when queuing lock", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @return LOCKED, if procedure is found and it has the lock; else UNLOCKED.
   * @throws ServiceException if given proc id is found but it is not a LockProcedure.
   */
  @Override
  public LockHeartbeatResponse lockHeartbeat(RpcController controller, LockHeartbeatRequest request)
      throws ServiceException {
    try {
      if (server.getLockManager().remoteLocks().lockHeartbeat(request.getProcId(),
          request.getKeepAlive())) {
        return LockHeartbeatResponse.newBuilder().setTimeoutMs(
            server.getConfiguration().getInt(LockProcedure.REMOTE_LOCKS_TIMEOUT_MS_CONF,
                LockProcedure.DEFAULT_REMOTE_LOCKS_TIMEOUT_MS))
            .setLockStatus(LockHeartbeatResponse.LockStatus.LOCKED).build();
      } else {
        return LockHeartbeatResponse.newBuilder()
            .setLockStatus(LockHeartbeatResponse.LockStatus.UNLOCKED).build();
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegionSpaceUseReportResponse reportRegionSpaceUse(RpcController controller,
      RegionSpaceUseReportRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      if (!QuotaUtil.isQuotaEnabled(server.getConfiguration())) {
        return RegionSpaceUseReportResponse.newBuilder().build();
      }
      MasterQuotaManager quotaManager = this.server.getMasterQuotaManager();
      if (quotaManager != null) {
        final long now = EnvironmentEdgeManager.currentTime();
        for (RegionSpaceUse report : request.getSpaceUseList()) {
          quotaManager.addRegionSize(ProtobufUtil.toRegionInfo(report.getRegionInfo()),
            report.getRegionSize(), now);
        }
      } else {
        LOG.debug("Received region space usage report but HMaster is not ready to process it, "
            + "skipping");
      }
      return RegionSpaceUseReportResponse.newBuilder().build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(
      RpcController controller, GetSpaceQuotaRegionSizesRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      MasterQuotaManager quotaManager = this.server.getMasterQuotaManager();
      GetSpaceQuotaRegionSizesResponse.Builder builder =
          GetSpaceQuotaRegionSizesResponse.newBuilder();
      if (quotaManager != null) {
        Map<RegionInfo,Long> regionSizes = quotaManager.snapshotRegionSizes();
        Map<TableName,Long> regionSizesByTable = new HashMap<>();
        // Translate hregioninfo+long -> tablename+long
        for (Entry<RegionInfo,Long> entry : regionSizes.entrySet()) {
          final TableName tableName = entry.getKey().getTable();
          Long prevSize = regionSizesByTable.get(tableName);
          if (prevSize == null) {
            prevSize = 0L;
          }
          regionSizesByTable.put(tableName, prevSize + entry.getValue());
        }
        // Serialize them into the protobuf
        for (Entry<TableName,Long> tableSize : regionSizesByTable.entrySet()) {
          builder.addSizes(RegionSizes.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName(tableSize.getKey()))
              .setSize(tableSize.getValue()).build());
        }
        return builder.build();
      } else {
        LOG.debug("Received space quota region size report but HMaster is not ready to process it,"
            + "skipping");
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetQuotaStatesResponse getQuotaStates(
      RpcController controller, GetQuotaStatesRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      QuotaObserverChore quotaChore = this.server.getQuotaObserverChore();
      GetQuotaStatesResponse.Builder builder = GetQuotaStatesResponse.newBuilder();
      if (quotaChore != null) {
        // The "current" view of all tables with quotas
        Map<TableName, SpaceQuotaSnapshot> tableSnapshots = quotaChore.getTableQuotaSnapshots();
        for (Entry<TableName, SpaceQuotaSnapshot> entry : tableSnapshots.entrySet()) {
          builder.addTableSnapshots(
              TableQuotaSnapshot.newBuilder()
                  .setTableName(ProtobufUtil.toProtoTableName(entry.getKey()))
                  .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(entry.getValue())).build());
        }
        // The "current" view of all namespaces with quotas
        Map<String, SpaceQuotaSnapshot> nsSnapshots = quotaChore.getNamespaceQuotaSnapshots();
        for (Entry<String, SpaceQuotaSnapshot> entry : nsSnapshots.entrySet()) {
          builder.addNsSnapshots(
              NamespaceQuotaSnapshot.newBuilder()
                  .setNamespace(entry.getKey())
                  .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(entry.getValue())).build());
        }
        return builder.build();
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClearDeadServersResponse clearDeadServers(RpcController controller,
      ClearDeadServersRequest request) throws ServiceException {
    LOG.debug(server.getClientIdAuditPrefix() + " clear dead region servers.");
    ClearDeadServersResponse.Builder response = ClearDeadServersResponse.newBuilder();
    try {
      server.checkInitialized();
      if (server.cpHost != null) {
        server.cpHost.preClearDeadServers();
      }

      if (server.getServerManager().areDeadServersInProgress()) {
        LOG.debug("Some dead server is still under processing, won't clear the dead server list");
        response.addAllServerName(request.getServerNameList());
      } else {
        DeadServer deadServer = server.getServerManager().getDeadServers();
        Set<Address> clearedServers = new HashSet<>();
        for (HBaseProtos.ServerName pbServer : request.getServerNameList()) {
          ServerName serverName = ProtobufUtil.toServerName(pbServer);
          final boolean deadInProcess = server.getProcedures().stream().anyMatch(
            p -> (p instanceof ServerCrashProcedure)
              && ((ServerCrashProcedure) p).getServerName().equals(serverName));
          if (deadInProcess) {
            throw new ServiceException(
              String.format("Dead server '%s' is not 'dead' in fact...", serverName));
          }

          if (!deadServer.removeDeadServer(serverName)) {
            response.addServerName(pbServer);
          } else {
            clearedServers.add(serverName.getAddress());
          }
        }
        server.getRSGroupInfoManager().removeServers(clearedServers);
        LOG.info("Remove decommissioned servers {} from RSGroup done", clearedServers);
      }

      if (server.cpHost != null) {
        server.cpHost.postClearDeadServers(
            ProtobufUtil.toServerNameList(request.getServerNameList()),
            ProtobufUtil.toServerNameList(response.getServerNameList()));
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }
    return response.build();
  }

  @Override
  public ReportProcedureDoneResponse reportProcedureDone(RpcController controller,
      ReportProcedureDoneRequest request) throws ServiceException {
    // Check Masters is up and ready for duty before progressing. Remote side will keep trying.
    try {
      this.server.checkServiceStarted();
    } catch (ServerNotRunningYetException snrye) {
      throw new ServiceException(snrye);
    }
    request.getResultList().forEach(result -> {
      if (result.getStatus() == RemoteProcedureResult.Status.SUCCESS) {
        server.remoteProcedureCompleted(result.getProcId());
      } else {
        server.remoteProcedureFailed(result.getProcId(),
          RemoteProcedureException.fromProto(result.getError()));
      }
    });
    return ReportProcedureDoneResponse.getDefaultInstance();
  }

  @Override
  public FileArchiveNotificationResponse reportFileArchival(RpcController controller,
      FileArchiveNotificationRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      if (!QuotaUtil.isQuotaEnabled(server.getConfiguration())) {
        return FileArchiveNotificationResponse.newBuilder().build();
      }
      server.getMasterQuotaManager().processFileArchivals(request, server.getConnection(),
          server.getConfiguration(), server.getFileSystem());
      return FileArchiveNotificationResponse.newBuilder().build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  // HBCK Services

  @Override
  public RunHbckChoreResponse runHbckChore(RpcController c, RunHbckChoreRequest req)
      throws ServiceException {
    rpcPreCheck("runHbckChore");
    LOG.info("{} request HBCK chore to run", server.getClientIdAuditPrefix());
    HbckChore hbckChore = server.getHbckChore();
    boolean ran = hbckChore.runChore();
    return RunHbckChoreResponse.newBuilder().setRan(ran).build();
  }

  /**
   * Update state of the table in meta only. This is required by hbck in some situations to cleanup
   * stuck assign/ unassign regions procedures for the table.
   *
   * @return previous state of the table
   */
  @Override
  public GetTableStateResponse setTableStateInMeta(RpcController controller,
      SetTableStateInMetaRequest request) throws ServiceException {
    rpcPreCheck("setTableStateInMeta");
    TableName tn = ProtobufUtil.toTableName(request.getTableName());
    try {
      TableState prevState = this.server.getTableStateManager().getTableState(tn);
      TableState newState = TableState.convert(tn, request.getTableState());
      LOG.info("{} set table={} state from {} to {}", server.getClientIdAuditPrefix(),
          tn, prevState.getState(), newState.getState());
      this.server.getTableStateManager().setTableState(tn, newState.getState());
      return GetTableStateResponse.newBuilder().setTableState(prevState.convert()).build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Update state of the region in meta only. This is required by hbck in some situations to cleanup
   * stuck assign/ unassign regions procedures for the table.
   *
   * @return previous states of the regions
   */
  @Override
  public SetRegionStateInMetaResponse setRegionStateInMeta(RpcController controller,
    SetRegionStateInMetaRequest request) throws ServiceException {
    rpcPreCheck("setRegionStateInMeta");
    SetRegionStateInMetaResponse.Builder builder = SetRegionStateInMetaResponse.newBuilder();
    try {
      for (RegionSpecifierAndState s : request.getStatesList()) {
        RegionSpecifier spec = s.getRegionSpecifier();
        String encodedName;
        if (spec.getType() == RegionSpecifierType.ENCODED_REGION_NAME) {
          encodedName = spec.getValue().toStringUtf8();
        } else {
          // TODO: actually, a full region name can save a lot on meta scan, improve later.
          encodedName = RegionInfo.encodeRegionName(spec.getValue().toByteArray());
        }
        RegionInfo info = this.server.getAssignmentManager().loadRegionFromMeta(encodedName);
        LOG.trace("region info loaded from meta table: {}", info);
        RegionState prevState =
          this.server.getAssignmentManager().getRegionStates().getRegionState(info);
        RegionState.State newState = RegionState.State.convert(s.getState());
        LOG.info("{} set region={} state from {} to {}", server.getClientIdAuditPrefix(), info,
          prevState.getState(), newState);
        Put metaPut = MetaTableAccessor.makePutFromRegionInfo(info,
          EnvironmentEdgeManager.currentTime());
        metaPut.addColumn(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER,
          Bytes.toBytes(newState.name()));
        List<Put> putList = new ArrayList<>();
        putList.add(metaPut);
        MetaTableAccessor.putsToMetaTable(this.server.getConnection(), putList);
        // Loads from meta again to refresh AM cache with the new region state
        this.server.getAssignmentManager().loadRegionFromMeta(encodedName);
        builder.addStates(RegionSpecifierAndState.newBuilder().setRegionSpecifier(spec)
          .setState(prevState.getState().convert()));
      }
    } catch (Exception e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  /**
   * Get RegionInfo from Master using content of RegionSpecifier as key.
   * @return RegionInfo found by decoding <code>rs</code> or null if none found
   */
  private RegionInfo getRegionInfo(HBaseProtos.RegionSpecifier rs) throws UnknownRegionException {
    RegionInfo ri = null;
    switch(rs.getType()) {
      case REGION_NAME:
        final byte[] regionName = rs.getValue().toByteArray();
        ri = this.server.getAssignmentManager().getRegionInfo(regionName);
        break;
      case ENCODED_REGION_NAME:
        String encodedRegionName = Bytes.toString(rs.getValue().toByteArray());
        RegionState regionState = this.server.getAssignmentManager().getRegionStates().
            getRegionState(encodedRegionName);
        ri = regionState == null ?
          this.server.getAssignmentManager().loadRegionFromMeta(encodedRegionName) :
            regionState.getRegion();
        break;
      default:
        break;
    }
    return ri;
  }

  /**
   * @throws ServiceException If no MasterProcedureExecutor
   */
  private void checkMasterProcedureExecutor() throws ServiceException {
    if (this.server.getMasterProcedureExecutor() == null) {
      throw new ServiceException("Master's ProcedureExecutor not initialized; retry later");
    }
  }

  /**
   * A 'raw' version of assign that does bulk and can skirt Master state checks if override
   * is set; i.e. assigns can be forced during Master startup or if RegionState is unclean.
   * Used by HBCK2.
   */
  @Override
  public MasterProtos.AssignsResponse assigns(RpcController controller,
      MasterProtos.AssignsRequest request) throws ServiceException {
    checkMasterProcedureExecutor();
    MasterProtos.AssignsResponse.Builder responseBuilder =
      MasterProtos.AssignsResponse.newBuilder();
    try {
      boolean override = request.getOverride();
      LOG.info("{} assigns, override={}", server.getClientIdAuditPrefix(), override);
      for (HBaseProtos.RegionSpecifier rs: request.getRegionList()) {
        long pid = Procedure.NO_PROC_ID;
        RegionInfo ri = getRegionInfo(rs);
        if (ri == null) {
          LOG.info("Unknown={}", rs);
        } else {
          Procedure p = this.server.getAssignmentManager().createOneAssignProcedure(ri, override);
          if (p != null) {
            pid = this.server.getMasterProcedureExecutor().submitProcedure(p);
          }
        }
        responseBuilder.addPid(pid);
      }
      return responseBuilder.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * A 'raw' version of unassign that does bulk and can skirt Master state checks if override
   * is set; i.e. unassigns can be forced during Master startup or if RegionState is unclean.
   * Used by HBCK2.
   */
  @Override
  public MasterProtos.UnassignsResponse unassigns(RpcController controller,
      MasterProtos.UnassignsRequest request) throws ServiceException {
    checkMasterProcedureExecutor();
    MasterProtos.UnassignsResponse.Builder responseBuilder =
        MasterProtos.UnassignsResponse.newBuilder();
    try {
      boolean override = request.getOverride();
      LOG.info("{} unassigns, override={}", server.getClientIdAuditPrefix(), override);
      for (HBaseProtos.RegionSpecifier rs: request.getRegionList()) {
        long pid = Procedure.NO_PROC_ID;
        RegionInfo ri = getRegionInfo(rs);
        if (ri == null) {
          LOG.info("Unknown={}", rs);
        } else {
          Procedure p = this.server.getAssignmentManager().createOneUnassignProcedure(ri, override);
          if (p != null) {
            pid = this.server.getMasterProcedureExecutor().submitProcedure(p);
          }
        }
        responseBuilder.addPid(pid);
      }
      return responseBuilder.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Bypass specified procedure to completion. Procedure is marked completed but no actual work
   * is done from the current state/ step onwards. Parents of the procedure are also marked for
   * bypass.
   *
   * NOTE: this is a dangerous operation and may be used to unstuck buggy procedures. This may
   * leave system in inconherent state. This may need to be followed by some cleanup steps/
   * actions by operator.
   *
   * @return BypassProcedureToCompletionResponse indicating success or failure
   */
  @Override
  public MasterProtos.BypassProcedureResponse bypassProcedure(RpcController controller,
      MasterProtos.BypassProcedureRequest request) throws ServiceException {
    try {
      LOG.info("{} bypass procedures={}, waitTime={}, override={}, recursive={}",
          server.getClientIdAuditPrefix(), request.getProcIdList(), request.getWaitTime(),
          request.getOverride(), request.getRecursive());
      List<Boolean> ret =
          server.getMasterProcedureExecutor().bypassProcedure(request.getProcIdList(),
          request.getWaitTime(), request.getOverride(), request.getRecursive());
      return MasterProtos.BypassProcedureResponse.newBuilder().addAllBypassed(ret).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MasterProtos.ScheduleServerCrashProcedureResponse scheduleServerCrashProcedure(
      RpcController controller, MasterProtos.ScheduleServerCrashProcedureRequest request)
      throws ServiceException {
    List<Long> pids = new ArrayList<>();
    for (HBaseProtos.ServerName sn: request.getServerNameList()) {
      ServerName serverName = ProtobufUtil.toServerName(sn);
      LOG.info("{} schedule ServerCrashProcedure for {}",
          this.server.getClientIdAuditPrefix(), serverName);
      if (shouldSubmitSCP(serverName)) {
        pids.add(this.server.getServerManager().expireServer(serverName, true));
      } else {
        pids.add(Procedure.NO_PROC_ID);
      }
    }
    return MasterProtos.ScheduleServerCrashProcedureResponse.newBuilder().addAllPid(pids).build();
  }

  @Override
  public MasterProtos.ScheduleSCPsForUnknownServersResponse scheduleSCPsForUnknownServers(
      RpcController controller, MasterProtos.ScheduleSCPsForUnknownServersRequest request)
      throws ServiceException {
    List<Long> pids = new ArrayList<>();
    final Set<ServerName> serverNames =
      server.getAssignmentManager().getRegionStates().getRegionStates().stream()
        .map(RegionState::getServerName).collect(Collectors.toSet());

    final Set<ServerName> unknownServerNames = serverNames.stream()
      .filter(sn -> server.getServerManager().isServerUnknown(sn)).collect(Collectors.toSet());

    for (ServerName sn: unknownServerNames) {
      LOG.info("{} schedule ServerCrashProcedure for unknown {}",
        this.server.getClientIdAuditPrefix(), sn);
      if (shouldSubmitSCP(sn)) {
        pids.add(this.server.getServerManager().expireServer(sn, true));
      } else {
        pids.add(Procedure.NO_PROC_ID);
      }
    }
    return MasterProtos.ScheduleSCPsForUnknownServersResponse.newBuilder().addAllPid(pids).build();
  }

  @Override
  public FixMetaResponse fixMeta(RpcController controller, FixMetaRequest request)
      throws ServiceException {
    rpcPreCheck("fixMeta");
    try {
      MetaFixer mf = new MetaFixer(this.server);
      mf.fix();
      return FixMetaResponse.newBuilder().build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public SwitchRpcThrottleResponse switchRpcThrottle(RpcController controller,
      SwitchRpcThrottleRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      return server.getMasterQuotaManager().switchRpcThrottle(request);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MasterProtos.IsRpcThrottleEnabledResponse isRpcThrottleEnabled(RpcController controller,
      MasterProtos.IsRpcThrottleEnabledRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      return server.getMasterQuotaManager().isRpcThrottleEnabled(request);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SwitchExceedThrottleQuotaResponse switchExceedThrottleQuota(RpcController controller,
      SwitchExceedThrottleQuotaRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      return server.getMasterQuotaManager().switchExceedThrottleQuota(request);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GrantResponse grant(RpcController controller, GrantRequest request)
      throws ServiceException {
    try {
      server.checkInitialized();
      if (server.cpHost != null && hasAccessControlServiceCoprocessor(server.cpHost)) {
        final UserPermission perm =
            ShadedAccessControlUtil.toUserPermission(request.getUserPermission());
        boolean mergeExistingPermissions = request.getMergeExistingPermissions();
        server.cpHost.preGrant(perm, mergeExistingPermissions);
        try (Table table = server.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          PermissionStorage.addUserPermission(getConfiguration(), perm, table,
            mergeExistingPermissions);
        }
        server.cpHost.postGrant(perm, mergeExistingPermissions);
        User caller = RpcServer.getRequestUser().orElse(null);
        if (AUDITLOG.isTraceEnabled()) {
          // audit log should store permission changes in addition to auth results
          String remoteAddress = RpcServer.getRemoteAddress().map(InetAddress::toString).orElse("");
          AUDITLOG.trace("User {} (remote address: {}) granted permission {}", caller,
            remoteAddress, perm);
        }
        return GrantResponse.getDefaultInstance();
      } else {
        throw new DoNotRetryIOException(
            new UnsupportedOperationException(AccessController.class.getName() + " is not loaded"));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RevokeResponse revoke(RpcController controller, RevokeRequest request)
      throws ServiceException {
    try {
      server.checkInitialized();
      if (server.cpHost != null && hasAccessControlServiceCoprocessor(server.cpHost)) {
        final UserPermission userPermission =
            ShadedAccessControlUtil.toUserPermission(request.getUserPermission());
        server.cpHost.preRevoke(userPermission);
        try (Table table = server.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          PermissionStorage.removeUserPermission(server.getConfiguration(), userPermission, table);
        }
        server.cpHost.postRevoke(userPermission);
        User caller = RpcServer.getRequestUser().orElse(null);
        if (AUDITLOG.isTraceEnabled()) {
          // audit log should record all permission changes
          String remoteAddress = RpcServer.getRemoteAddress().map(InetAddress::toString).orElse("");
          AUDITLOG.trace("User {} (remote address: {}) revoked permission {}", caller,
            remoteAddress, userPermission);
        }
        return RevokeResponse.getDefaultInstance();
      } else {
        throw new DoNotRetryIOException(
            new UnsupportedOperationException(AccessController.class.getName() + " is not loaded"));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public GetUserPermissionsResponse getUserPermissions(RpcController controller,
      GetUserPermissionsRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      if (server.cpHost != null && hasAccessControlServiceCoprocessor(server.cpHost)) {
        final String userName = request.hasUserName() ? request.getUserName().toStringUtf8() : null;
        String namespace =
            request.hasNamespaceName() ? request.getNamespaceName().toStringUtf8() : null;
        TableName table =
            request.hasTableName() ? ProtobufUtil.toTableName(request.getTableName()) : null;
        byte[] cf = request.hasColumnFamily() ? request.getColumnFamily().toByteArray() : null;
        byte[] cq =
            request.hasColumnQualifier() ? request.getColumnQualifier().toByteArray() : null;
        Type permissionType = request.hasType() ? request.getType() : null;
        server.getMasterCoprocessorHost().preGetUserPermissions(userName, namespace, table, cf, cq);

        List<UserPermission> perms = null;
        if (permissionType == Type.Table) {
          boolean filter = (cf != null || userName != null) ? true : false;
          perms = PermissionStorage.getUserTablePermissions(server.getConfiguration(), table, cf,
            cq, userName, filter);
        } else if (permissionType == Type.Namespace) {
          perms = PermissionStorage.getUserNamespacePermissions(server.getConfiguration(),
            namespace, userName, userName != null ? true : false);
        } else {
          perms = PermissionStorage.getUserPermissions(server.getConfiguration(), null, null, null,
            userName, userName != null ? true : false);
          // Skip super users when filter user is specified
          if (userName == null) {
            // Adding superusers explicitly to the result set as PermissionStorage do not store
            // them. Also using acl as table name to be inline with the results of global admin and
            // will help in avoiding any leakage of information about being superusers.
            for (String user : Superusers.getSuperUsers()) {
              perms.add(new UserPermission(user,
                  Permission.newBuilder().withActions(Action.values()).build()));
            }
          }
        }

        server.getMasterCoprocessorHost().postGetUserPermissions(userName, namespace, table, cf,
          cq);
        AccessControlProtos.GetUserPermissionsResponse response =
            ShadedAccessControlUtil.buildGetUserPermissionsResponse(perms);
        return response;
      } else {
        throw new DoNotRetryIOException(
            new UnsupportedOperationException(AccessController.class.getName() + " is not loaded"));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public HasUserPermissionsResponse hasUserPermissions(RpcController controller,
      HasUserPermissionsRequest request) throws ServiceException {
    try {
      server.checkInitialized();
      if (server.cpHost != null && hasAccessControlServiceCoprocessor(server.cpHost)) {
        User caller = RpcServer.getRequestUser().orElse(null);
        String userName =
            request.hasUserName() ? request.getUserName().toStringUtf8() : caller.getShortName();
        List<Permission> permissions = new ArrayList<>();
        for (int i = 0; i < request.getPermissionCount(); i++) {
          permissions.add(ShadedAccessControlUtil.toPermission(request.getPermission(i)));
        }
        server.getMasterCoprocessorHost().preHasUserPermissions(userName, permissions);
        if (!caller.getShortName().equals(userName)) {
          List<String> groups = AccessChecker.getUserGroups(userName);
          caller = new InputUser(userName, groups.toArray(new String[groups.size()]));
        }
        List<Boolean> hasUserPermissions = new ArrayList<>();
        if (getAccessChecker() != null) {
          for (Permission permission : permissions) {
            boolean hasUserPermission =
                getAccessChecker().hasUserPermission(caller, "hasUserPermissions", permission);
            hasUserPermissions.add(hasUserPermission);
          }
        } else {
          for (int i = 0; i < permissions.size(); i++) {
            hasUserPermissions.add(true);
          }
        }
        server.getMasterCoprocessorHost().postHasUserPermissions(userName, permissions);
        HasUserPermissionsResponse.Builder builder =
            HasUserPermissionsResponse.newBuilder().addAllHasUserPermission(hasUserPermissions);
        return builder.build();
      } else {
        throw new DoNotRetryIOException(
            new UnsupportedOperationException(AccessController.class.getName() + " is not loaded"));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private boolean shouldSubmitSCP(ServerName serverName) {
    // check if there is already a SCP of this server running
    List<Procedure<MasterProcedureEnv>> procedures =
        server.getMasterProcedureExecutor().getProcedures();
    for (Procedure<MasterProcedureEnv> procedure : procedures) {
      if (procedure instanceof ServerCrashProcedure) {
        if (serverName.compareTo(((ServerCrashProcedure) procedure).getServerName()) == 0
            && !procedure.isFinished()) {
          LOG.info("there is already a SCP of this server {} running, pid {}", serverName,
            procedure.getProcId());
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public GetRSGroupInfoResponse getRSGroupInfo(RpcController controller,
    GetRSGroupInfoRequest request) throws ServiceException {
    String groupName = request.getRSGroupName();
    LOG.info(
      server.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, group=" + groupName);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preGetRSGroupInfo(groupName);
      }
      RSGroupInfo rsGroupInfo = server.getRSGroupInfoManager().getRSGroup(groupName);
      GetRSGroupInfoResponse resp;
      if (rsGroupInfo != null) {
        resp = GetRSGroupInfoResponse.newBuilder()
          .setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(rsGroupInfo)).build();
      } else {
        resp = GetRSGroupInfoResponse.getDefaultInstance();
      }
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postGetRSGroupInfo(groupName);
      }
      return resp;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetRSGroupInfoOfTableResponse getRSGroupInfoOfTable(RpcController controller,
    GetRSGroupInfoOfTableRequest request) throws ServiceException {
    TableName tableName = ProtobufUtil.toTableName(request.getTableName());
    LOG.info(
      server.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, table=" + tableName);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preGetRSGroupInfoOfTable(tableName);
      }
      GetRSGroupInfoOfTableResponse resp;
      TableDescriptor td = server.getTableDescriptors().get(tableName);
      if (td == null) {
        resp = GetRSGroupInfoOfTableResponse.getDefaultInstance();
      } else {
        RSGroupInfo rsGroupInfo =
            RSGroupUtil.getRSGroupInfo(server, server.getRSGroupInfoManager(), tableName)
                .orElse(server.getRSGroupInfoManager().getRSGroup(RSGroupInfo.DEFAULT_GROUP));
        resp = GetRSGroupInfoOfTableResponse.newBuilder()
          .setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(rsGroupInfo)).build();
      }
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postGetRSGroupInfoOfTable(tableName);
      }
      return resp;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetRSGroupInfoOfServerResponse getRSGroupInfoOfServer(RpcController controller,
    GetRSGroupInfoOfServerRequest request) throws ServiceException {
    Address hp =
      Address.fromParts(request.getServer().getHostName(), request.getServer().getPort());
    LOG.info(server.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, server=" + hp);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preGetRSGroupInfoOfServer(hp);
      }
      RSGroupInfo rsGroupInfo = server.getRSGroupInfoManager().getRSGroupOfServer(hp);
      GetRSGroupInfoOfServerResponse resp;
      if (rsGroupInfo != null) {
        resp = GetRSGroupInfoOfServerResponse.newBuilder()
          .setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(rsGroupInfo)).build();
      } else {
        resp = GetRSGroupInfoOfServerResponse.getDefaultInstance();
      }
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postGetRSGroupInfoOfServer(hp);
      }
      return resp;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MoveServersResponse moveServers(RpcController controller, MoveServersRequest request)
      throws ServiceException {
    Set<Address> hostPorts = Sets.newHashSet();
    MoveServersResponse.Builder builder = MoveServersResponse.newBuilder();
    for (HBaseProtos.ServerName el : request.getServersList()) {
      hostPorts.add(Address.fromParts(el.getHostName(), el.getPort()));
    }
    LOG.info(server.getClientIdAuditPrefix() + " move servers " + hostPorts + " to rsgroup " +
        request.getTargetGroup());
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preMoveServers(hostPorts, request.getTargetGroup());
      }
      server.getRSGroupInfoManager().moveServers(hostPorts, request.getTargetGroup());
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postMoveServers(hostPorts, request.getTargetGroup());
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public AddRSGroupResponse addRSGroup(RpcController controller, AddRSGroupRequest request)
      throws ServiceException {
    AddRSGroupResponse.Builder builder = AddRSGroupResponse.newBuilder();
    LOG.info(server.getClientIdAuditPrefix() + " add rsgroup " + request.getRSGroupName());
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preAddRSGroup(request.getRSGroupName());
      }
      server.getRSGroupInfoManager().addRSGroup(new RSGroupInfo(request.getRSGroupName()));
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postAddRSGroup(request.getRSGroupName());
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public RemoveRSGroupResponse removeRSGroup(RpcController controller, RemoveRSGroupRequest request)
      throws ServiceException {
    RemoveRSGroupResponse.Builder builder = RemoveRSGroupResponse.newBuilder();
    LOG.info(server.getClientIdAuditPrefix() + " remove rsgroup " + request.getRSGroupName());
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preRemoveRSGroup(request.getRSGroupName());
      }
      server.getRSGroupInfoManager().removeRSGroup(request.getRSGroupName());
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postRemoveRSGroup(request.getRSGroupName());
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public BalanceRSGroupResponse balanceRSGroup(RpcController controller,
      BalanceRSGroupRequest request) throws ServiceException {
    BalanceRequest balanceRequest = ProtobufUtil.toBalanceRequest(request);

    BalanceRSGroupResponse.Builder builder = BalanceRSGroupResponse.newBuilder()
      .setBalanceRan(false);

    LOG.info(
      server.getClientIdAuditPrefix() + " balance rsgroup, group=" + request.getRSGroupName());
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost()
          .preBalanceRSGroup(request.getRSGroupName(), balanceRequest);
      }
      BalanceResponse response =
        server.getRSGroupInfoManager().balanceRSGroup(request.getRSGroupName(), balanceRequest);
      ProtobufUtil.populateBalanceRSGroupResponse(builder, response);
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost()
          .postBalanceRSGroup(request.getRSGroupName(), balanceRequest, response);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public ListRSGroupInfosResponse listRSGroupInfos(RpcController controller,
      ListRSGroupInfosRequest request) throws ServiceException {
    ListRSGroupInfosResponse.Builder builder = ListRSGroupInfosResponse.newBuilder();
    LOG.info(server.getClientIdAuditPrefix() + " list rsgroup");
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preListRSGroups();
      }
      List<RSGroupInfo> rsGroupInfos = server.getRSGroupInfoManager().listRSGroups().stream()
          .map(RSGroupInfo::new).collect(Collectors.toList());
      Map<String, RSGroupInfo> name2Info = new HashMap<>();
      List<TableDescriptor> needToFill =
          new ArrayList<>(server.getTableDescriptors().getAll().values());
      for (RSGroupInfo rsGroupInfo : rsGroupInfos) {
        name2Info.put(rsGroupInfo.getName(), rsGroupInfo);
        for (TableDescriptor td : server.getTableDescriptors().getAll().values()) {
          if (rsGroupInfo.containsTable(td.getTableName())){
            needToFill.remove(td);
          }
        }
      }
      for (TableDescriptor td : needToFill) {
        String groupName = td.getRegionServerGroup().orElse(RSGroupInfo.DEFAULT_GROUP);
        RSGroupInfo rsGroupInfo = name2Info.get(groupName);
        if (rsGroupInfo != null) {
          rsGroupInfo.addTable(td.getTableName());
        }
      }
      for (RSGroupInfo rsGroupInfo : rsGroupInfos) {
        // TODO: this can be done at once outside this loop, do not need to scan all every time.
        builder.addRSGroupInfo(ProtobufUtil.toProtoGroupInfo(rsGroupInfo));
      }
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postListRSGroups();
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public RemoveServersResponse removeServers(RpcController controller,
      RemoveServersRequest request) throws ServiceException {
    RemoveServersResponse.Builder builder = RemoveServersResponse.newBuilder();
    Set<Address> servers = Sets.newHashSet();
    for (HBaseProtos.ServerName el : request.getServersList()) {
      servers.add(Address.fromParts(el.getHostName(), el.getPort()));
    }
    LOG.info(server.getClientIdAuditPrefix() + " remove decommissioned servers from rsgroup: " +
        servers);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preRemoveServers(servers);
      }
      server.getRSGroupInfoManager().removeServers(servers);
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postRemoveServers(servers);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public ListTablesInRSGroupResponse listTablesInRSGroup(RpcController controller,
    ListTablesInRSGroupRequest request) throws ServiceException {
    ListTablesInRSGroupResponse.Builder builder = ListTablesInRSGroupResponse.newBuilder();
    String groupName = request.getGroupName();
    LOG.info(server.getClientIdAuditPrefix() + " list tables in rsgroup " + groupName);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preListTablesInRSGroup(groupName);
      }
      RSGroupUtil.listTablesInRSGroup(server, groupName).stream()
        .map(ProtobufUtil::toProtoTableName).forEach(builder::addTableName);
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postListTablesInRSGroup(groupName);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public GetConfiguredNamespacesAndTablesInRSGroupResponse
    getConfiguredNamespacesAndTablesInRSGroup(RpcController controller,
      GetConfiguredNamespacesAndTablesInRSGroupRequest request) throws ServiceException {
    GetConfiguredNamespacesAndTablesInRSGroupResponse.Builder builder =
      GetConfiguredNamespacesAndTablesInRSGroupResponse.newBuilder();
    String groupName = request.getGroupName();
    LOG.info(server.getClientIdAuditPrefix() + " get configured namespaces and tables in rsgroup " +
      groupName);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preGetConfiguredNamespacesAndTablesInRSGroup(groupName);
      }
      for (NamespaceDescriptor nd : server.getClusterSchema().getNamespaces()) {
        if (groupName.equals(nd.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP))) {
          builder.addNamespace(nd.getName());
        }
      }
      for (TableDescriptor td : server.getTableDescriptors().getAll().values()) {
        if (td.getRegionServerGroup().map(g -> g.equals(groupName)).orElse(false)) {
          builder.addTableName(ProtobufUtil.toProtoTableName(td.getTableName()));
        }
      }
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postGetConfiguredNamespacesAndTablesInRSGroup(groupName);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public RenameRSGroupResponse renameRSGroup(RpcController controller,
      RenameRSGroupRequest request) throws ServiceException {
    RenameRSGroupResponse.Builder builder = RenameRSGroupResponse.newBuilder();
    String oldRSGroup = request.getOldRsgroupName();
    String newRSGroup = request.getNewRsgroupName();
    LOG.info("{} rename rsgroup from {} to {} ",
      server.getClientIdAuditPrefix(), oldRSGroup, newRSGroup);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preRenameRSGroup(oldRSGroup, newRSGroup);
      }
      server.getRSGroupInfoManager().renameRSGroup(oldRSGroup, newRSGroup);
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postRenameRSGroup(oldRSGroup, newRSGroup);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public UpdateRSGroupConfigResponse updateRSGroupConfig(RpcController controller,
                                                         UpdateRSGroupConfigRequest request)
      throws ServiceException {
    UpdateRSGroupConfigResponse.Builder builder = UpdateRSGroupConfigResponse.newBuilder();
    String groupName = request.getGroupName();
    Map<String, String> configuration = new HashMap<>();
    request.getConfigurationList().forEach(p -> configuration.put(p.getName(), p.getValue()));
    LOG.info("{} update rsgroup {} configuration {}", server.getClientIdAuditPrefix(), groupName,
        configuration);
    try {
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().preUpdateRSGroupConfig(groupName, configuration);
      }
      server.getRSGroupInfoManager().updateRSGroupConfig(groupName, configuration);
      if (server.getMasterCoprocessorHost() != null) {
        server.getMasterCoprocessorHost().postUpdateRSGroupConfig(groupName, configuration);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public HBaseProtos.LogEntry getLogEntries(RpcController controller,
      HBaseProtos.LogRequest request) throws ServiceException {
    try {
      final String logClassName = request.getLogClassName();
      Class<?> logClass = Class.forName(logClassName)
        .asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("BalancerDecisionsRequest")) {
        MasterProtos.BalancerDecisionsRequest balancerDecisionsRequest =
          (MasterProtos.BalancerDecisionsRequest) method
            .invoke(null, request.getLogMessage());
        MasterProtos.BalancerDecisionsResponse balancerDecisionsResponse =
          getBalancerDecisions(balancerDecisionsRequest);
        return HBaseProtos.LogEntry.newBuilder()
          .setLogClassName(balancerDecisionsResponse.getClass().getName())
          .setLogMessage(balancerDecisionsResponse.toByteString())
          .build();
      }else if (logClassName.contains("BalancerRejectionsRequest")){
        MasterProtos.BalancerRejectionsRequest balancerRejectionsRequest =
          (MasterProtos.BalancerRejectionsRequest) method
            .invoke(null, request.getLogMessage());
        MasterProtos.BalancerRejectionsResponse balancerRejectionsResponse =
          getBalancerRejections(balancerRejectionsRequest);
        return HBaseProtos.LogEntry.newBuilder()
          .setLogClassName(balancerRejectionsResponse.getClass().getName())
          .setLogMessage(balancerRejectionsResponse.toByteString())
          .build();
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
        | InvocationTargetException e) {
      LOG.error("Error while retrieving log entries.", e);
      throw new ServiceException(e);
    }
    throw new ServiceException("Invalid request params");
  }

  private MasterProtos.BalancerDecisionsResponse
    getBalancerDecisions(MasterProtos.BalancerDecisionsRequest request) {
    final NamedQueueRecorder namedQueueRecorder = this.server.getNamedQueueRecorder();
    if (namedQueueRecorder == null) {
      return MasterProtos.BalancerDecisionsResponse.newBuilder()
        .addAllBalancerDecision(Collections.emptyList()).build();
    }
    final NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(BalancerDecisionDetails.BALANCER_DECISION_EVENT);
    namedQueueGetRequest.setBalancerDecisionsRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    List<RecentLogs.BalancerDecision> balancerDecisions = namedQueueGetResponse != null ?
      namedQueueGetResponse.getBalancerDecisions() :
      Collections.emptyList();
    return MasterProtos.BalancerDecisionsResponse.newBuilder()
      .addAllBalancerDecision(balancerDecisions).build();
  }

  private MasterProtos.BalancerRejectionsResponse getBalancerRejections(
    MasterProtos.BalancerRejectionsRequest request) {
    final NamedQueueRecorder namedQueueRecorder = this.server.getNamedQueueRecorder();
    if (namedQueueRecorder == null) {
      return MasterProtos.BalancerRejectionsResponse.newBuilder()
        .addAllBalancerRejection(Collections.emptyList()).build();
    }
    final NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(BalancerRejectionDetails.BALANCER_REJECTION_EVENT);
    namedQueueGetRequest.setBalancerRejectionsRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    List<RecentLogs.BalancerRejection> balancerRejections = namedQueueGetResponse != null ?
      namedQueueGetResponse.getBalancerRejections() :
      Collections.emptyList();
    return MasterProtos.BalancerRejectionsResponse.newBuilder()
      .addAllBalancerRejection(balancerRejections).build();
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
    final GetRegionInfoRequest request) throws ServiceException {
    RegionInfo ri = null;
    try {
      ri = getRegionInfo(request.getRegion());
    } catch(UnknownRegionException ure) {
      throw new ServiceException(ure);
    }
    GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
    if (ri != null) {
      builder.setRegionInfo(ProtobufUtil.toRegionInfo(ri));
    } else {
      // Is it a MOB name? These work differently.
      byte [] regionName = request.getRegion().getValue().toByteArray();
      TableName tableName = RegionInfo.getTable(regionName);
      if (MobUtils.isMobRegionName(tableName, regionName)) {
        // a dummy region info contains the compaction state.
        RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tableName);
        builder.setRegionInfo(ProtobufUtil.toRegionInfo(mobRegionInfo));
        if (request.hasCompactionState() && request.getCompactionState()) {
          builder.setCompactionState(server.getMobCompactionState(tableName));
        }
      } else {
        // If unknown RegionInfo and not a MOB region, it is unknown.
        throw new ServiceException(new UnknownRegionException(Bytes.toString(regionName)));
      }
    }
    return builder.build();
  }

  @Override
  public GetStoreFileResponse getStoreFile(RpcController controller, GetStoreFileRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public GetOnlineRegionResponse getOnlineRegion(RpcController controller,
    GetOnlineRegionRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public OpenRegionResponse openRegion(RpcController controller, OpenRegionRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public WarmupRegionResponse warmupRegion(RpcController controller, WarmupRegionRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public CloseRegionResponse closeRegion(RpcController controller, CloseRegionRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public FlushRegionResponse flushRegion(RpcController controller, FlushRegionRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public CompactionSwitchResponse compactionSwitch(RpcController controller,
    CompactionSwitchRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public CompactRegionResponse compactRegion(RpcController controller, CompactRegionRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public ReplicateWALEntryResponse replicateWALEntry(RpcController controller,
    ReplicateWALEntryRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public ReplicateWALEntryResponse replay(RpcController controller,
    ReplicateWALEntryRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public RollWALWriterResponse rollWALWriter(RpcController controller, RollWALWriterRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public GetServerInfoResponse getServerInfo(RpcController controller, GetServerInfoRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public StopServerResponse stopServer(RpcController controller, StopServerRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
    UpdateFavoredNodesRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public GetRegionLoadResponse getRegionLoad(RpcController controller, GetRegionLoadRequest request)
    throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller,
    ClearCompactionQueuesRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
    ClearRegionBlockCacheRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController controller,
    GetSpaceQuotaSnapshotsRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public ExecuteProceduresResponse executeProcedures(RpcController controller,
    ExecuteProceduresRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }

  @Override
  public GetLiveRegionServersResponse getLiveRegionServers(RpcController controller,
    GetLiveRegionServersRequest request) throws ServiceException {
    List<ServerName> regionServers = new ArrayList<>(server.getLiveRegionServers());
    Collections.shuffle(regionServers, ThreadLocalRandom.current());
    GetLiveRegionServersResponse.Builder builder =
      GetLiveRegionServersResponse.newBuilder().setTotal(regionServers.size());
    regionServers.stream().limit(request.getCount()).map(ProtobufUtil::toServerName)
      .forEach(builder::addServer);
    return builder.build();
  }

  @Override
  public ReplicateWALEntryResponse replicateToReplica(RpcController controller,
    ReplicateWALEntryRequest request) throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("Unsupported method on master"));
  }
}
