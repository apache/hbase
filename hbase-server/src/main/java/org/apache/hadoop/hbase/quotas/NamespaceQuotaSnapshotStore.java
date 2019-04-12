/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * {@link QuotaSnapshotStore} implementation for namespaces.
 */
@InterfaceAudience.Private
public class NamespaceQuotaSnapshotStore implements QuotaSnapshotStore<String> {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReadLock rlock = lock.readLock();
  private final WriteLock wlock = lock.writeLock();

  private final Connection conn;
  private final QuotaObserverChore chore;
  private Map<RegionInfo,Long> regionUsage;

  public NamespaceQuotaSnapshotStore(Connection conn, QuotaObserverChore chore, Map<RegionInfo,Long> regionUsage) {
    this.conn = Objects.requireNonNull(conn);
    this.chore = Objects.requireNonNull(chore);
    this.regionUsage = Objects.requireNonNull(regionUsage);
  }

  @Override
  public SpaceQuota getSpaceQuota(String namespace) throws IOException {
    Quotas quotas = getQuotaForNamespace(namespace);
    if (quotas != null && quotas.hasSpace()) {
      return quotas.getSpace();
    }
    return null;
  }

  /**
   * Fetches the namespace quota. Visible for mocking/testing.
   */
  Quotas getQuotaForNamespace(String namespace) throws IOException {
    return QuotaTableUtil.getNamespaceQuota(conn, namespace);
  }

  @Override
  public SpaceQuotaSnapshot getCurrentState(String namespace) {
    // Defer the "current state" to the chore
    return this.chore.getNamespaceQuotaSnapshot(namespace);
  }

  @Override
  public SpaceQuotaSnapshot getTargetState(
      String subject, SpaceQuota spaceQuota) throws IOException {
    rlock.lock();
    try {
      final long sizeLimitInBytes = spaceQuota.getSoftLimit();
      long sum = 0L;
      for (Entry<RegionInfo,Long> entry : filterBySubject(subject)) {
        sum += entry.getValue();
      }
      // Add in the size for any snapshots against this table
      sum += QuotaTableUtil.getNamespaceSnapshotSize(conn, subject);
      // Observance is defined as the size of the table being less than the limit
      SpaceQuotaStatus status = sum <= sizeLimitInBytes ? SpaceQuotaStatus.notInViolation()
          : new SpaceQuotaStatus(ProtobufUtil.toViolationPolicy(spaceQuota.getViolationPolicy()));
      return new SpaceQuotaSnapshot(status, sum, sizeLimitInBytes);
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public Iterable<Entry<RegionInfo, Long>> filterBySubject(String namespace) {
    rlock.lock();
    try {
      return regionUsage.entrySet().stream()
        .filter(entry -> namespace.equals(entry.getKey().getTable().getNamespaceAsString()))
        .collect(Collectors.toList());
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public void setCurrentState(String namespace, SpaceQuotaSnapshot snapshot) {
    // Defer the "current state" to the chore
    this.chore.setNamespaceQuotaSnapshot(namespace, snapshot);
  }

  @Override
  public void setRegionUsage(Map<RegionInfo,Long> regionUsage) {
    wlock.lock();
    try {
      this.regionUsage = Objects.requireNonNull(regionUsage);
    } finally {
      wlock.unlock();
    }
  }
}
