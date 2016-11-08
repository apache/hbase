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
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * {@link QuotaViolationStore} implementation for namespaces.
 */
@InterfaceAudience.Private
public class NamespaceQuotaViolationStore implements QuotaViolationStore<String> {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReadLock rlock = lock.readLock();
  private final WriteLock wlock = lock.writeLock();

  private final Connection conn;
  private final QuotaObserverChore chore;
  private Map<HRegionInfo,Long> regionUsage;

  public NamespaceQuotaViolationStore(Connection conn, QuotaObserverChore chore, Map<HRegionInfo,Long> regionUsage) {
    this.conn = Objects.requireNonNull(conn);
    this.chore = Objects.requireNonNull(chore);
    this.regionUsage = Objects.requireNonNull(regionUsage);
  }

  @Override
  public SpaceQuota getSpaceQuota(String namespace) throws IOException {
    Quotas quotas = getQuotaForNamespace(namespace);
    if (null != quotas && quotas.hasSpace()) {
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
  public ViolationState getCurrentState(String namespace) {
    // Defer the "current state" to the chore
    return this.chore.getNamespaceQuotaViolation(namespace);
  }

  @Override
  public ViolationState getTargetState(String subject, SpaceQuota spaceQuota) {
    rlock.lock();
    try {
      final long sizeLimitInBytes = spaceQuota.getSoftLimit();
      long sum = 0L;
      for (Entry<HRegionInfo,Long> entry : filterBySubject(subject)) {
        sum += entry.getValue();
        if (sum > sizeLimitInBytes) {
          // Short-circuit early
          return ViolationState.IN_VIOLATION;
        }
      }
      // Observance is defined as the size of the table being less than the limit
      return sum <= sizeLimitInBytes ? ViolationState.IN_OBSERVANCE : ViolationState.IN_VIOLATION;
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public Iterable<Entry<HRegionInfo,Long>> filterBySubject(String namespace) {
    rlock.lock();
    try {
      return Iterables.filter(regionUsage.entrySet(), new Predicate<Entry<HRegionInfo,Long>>() {
        @Override
        public boolean apply(Entry<HRegionInfo,Long> input) {
          return namespace.equals(input.getKey().getTable().getNamespaceAsString());
        }
      });
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public void setCurrentState(String namespace, ViolationState state) {
    // Defer the "current state" to the chore
    this.chore.setNamespaceQuotaViolation(namespace, state);
  }

  @Override
  public void setRegionUsage(Map<HRegionInfo,Long> regionUsage) {
    wlock.lock();
    try {
      this.regionUsage = Objects.requireNonNull(regionUsage);
    } finally {
      wlock.unlock();
    }
  }
}
