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
package org.apache.hadoop.hbase.security.access;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclRemoteStateData;

/**
 * Applies one ACL change set pushed by the master to the local {@link AuthManager} cache, via
 * {@link AclRefresher} (serial queue, timestamp watermark).
 */
@InterfaceAudience.Private
public class RefreshAclRemoteCallable extends BaseRSProcedureCallable {

  private List<AuthManager.EntryUpdate> updates;
  private boolean full;
  private long ts;

  @Override
  protected byte[] doCall() {
    AclRefresher refresher = rs.getAclRefresher();
    if (full) {
      refresher.submitFullSnapshot(ts, toSnapshot(updates));
    } else {
      refresher.submitUpdates(ts, updates);
    }
    return null;
  }

  @Override
  protected void initParameter(byte[] parameter) throws InvalidProtocolBufferException {
    RefreshAclRemoteStateData data = RefreshAclRemoteStateData.parseFrom(parameter);
    full = data.getFull();
    ts = data.getTs();
    updates = new ArrayList<>(data.getUpdatesCount());
    for (AclEntryUpdate update : data.getUpdatesList()) {
      byte[] entry = update.getEntry().toByteArray();
      updates.add(update.getDeleted()
        ? AuthManager.EntryUpdate.deleted(entry)
        : AuthManager.EntryUpdate.replace(entry,
          AccessControlUtil.toUserPermission(update.getPerms())));
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.M_RS_REFRESH_ACL;
  }

  private static Map<byte[], ListMultimap<String, UserPermission>>
    toSnapshot(List<AuthManager.EntryUpdate> entryUpdates) {
    Map<byte[], ListMultimap<String, UserPermission>> snapshot =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (AuthManager.EntryUpdate update : entryUpdates) {
      if (!update.isDeleted()) {
        snapshot.put(update.getEntry(), update.getPermissions());
      }
    }
    return snapshot;
  }
}
