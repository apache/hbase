/**
 *
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

package org.apache.hadoop.hbase.client.locking;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.NonceGenerator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockType;

/**
 * Helper class to create "master locks" for namespaces, tables and regions.
 * DEV-NOTE: At the moment this class is used only by the RS for MOB,
 *           to prevent other MOB compaction to conflict.
 *           The RS has already the stub of the LockService, so we have only one constructor that
 *           takes the LockService stub. If in the future we are going to use this in other places
 *           we should add a constructor that from conf or connection, creates the stub.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class LockServiceClient {
  private final LockService.BlockingInterface stub;
  private final Configuration conf;
  private final NonceGenerator ng;

  public LockServiceClient(final Configuration conf, final LockService.BlockingInterface stub,
      final NonceGenerator ng) {
    this.conf = conf;
    this.stub = stub;
    this.ng = ng;
  }

  /**
   * Create a new EntityLock object to acquire an exclusive or shared lock on a table.
   * Internally, the table namespace will also be locked in shared mode.
   */
  public EntityLock tableLock(final TableName tableName, final boolean exclusive,
      final String description, final Abortable abort) {
    LockRequest lockRequest = buildLockRequest(exclusive ? LockType.EXCLUSIVE : LockType.SHARED,
        tableName.getNameAsString(), null, null, description, ng.getNonceGroup(), ng.newNonce());
    return new EntityLock(conf, stub, lockRequest, abort);
  }

  /**
   * LocCreate a new EntityLock object to acquire exclusive lock on a namespace.
   * Clients can not acquire shared locks on namespace.
   */
  public EntityLock namespaceLock(String namespace, String description, Abortable abort) {
    LockRequest lockRequest = buildLockRequest(LockType.EXCLUSIVE,
        namespace, null, null, description, ng.getNonceGroup(), ng.newNonce());
    return new EntityLock(conf, stub, lockRequest, abort);
  }

  /**
   * Create a new EntityLock object to acquire exclusive lock on multiple regions of same tables.
   * Internally, the table and its namespace will also be locked in shared mode.
   */
  public EntityLock regionLock(List<RegionInfo> regionInfos, String description, Abortable abort) {
    LockRequest lockRequest = buildLockRequest(LockType.EXCLUSIVE,
        null, null, regionInfos, description, ng.getNonceGroup(), ng.newNonce());
    return new EntityLock(conf, stub, lockRequest, abort);
  }

  @InterfaceAudience.Private
  public static LockRequest buildLockRequest(final LockType type,
      final String namespace, final TableName tableName, final List<RegionInfo> regionInfos,
      final String description, final long nonceGroup, final long nonce) {
    final LockRequest.Builder builder = LockRequest.newBuilder()
      .setLockType(type)
      .setNonceGroup(nonceGroup)
      .setNonce(nonce);
    if (regionInfos != null) {
      for (RegionInfo hri: regionInfos) {
        builder.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    } else if (namespace != null) {
      builder.setNamespace(namespace);
    } else if (tableName != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    return builder.setDescription(description).build();
  }
}
