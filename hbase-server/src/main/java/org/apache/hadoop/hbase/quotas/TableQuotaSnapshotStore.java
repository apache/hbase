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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Predicate;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Iterables;

/**
 * {@link QuotaSnapshotStore} for tables.
 */
@InterfaceAudience.Private
public class TableQuotaSnapshotStore implements QuotaSnapshotStore<TableName> {
  private static final Log LOG = LogFactory.getLog(TableQuotaSnapshotStore.class);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReadLock rlock = lock.readLock();
  private final WriteLock wlock = lock.writeLock();

  private final Connection conn;
  private final QuotaObserverChore chore;
  private Map<HRegionInfo,Long> regionUsage;

  public TableQuotaSnapshotStore(Connection conn, QuotaObserverChore chore, Map<HRegionInfo,Long> regionUsage) {
    this.conn = Objects.requireNonNull(conn);
    this.chore = Objects.requireNonNull(chore);
    this.regionUsage = Objects.requireNonNull(regionUsage);
  }

  @Override
  public SpaceQuota getSpaceQuota(TableName subject) throws IOException {
    Quotas quotas = getQuotaForTable(subject);
    if (quotas != null && quotas.hasSpace()) {
      return quotas.getSpace();
    }
    return null;
  }
  /**
   * Fetches the table quota. Visible for mocking/testing.
   */
  Quotas getQuotaForTable(TableName table) throws IOException {
    return QuotaTableUtil.getTableQuota(conn, table);
  }

  @Override
  public SpaceQuotaSnapshot getCurrentState(TableName table) {
    // Defer the "current state" to the chore
    return chore.getTableQuotaSnapshot(table);
  }

  @Override
  public SpaceQuotaSnapshot getTargetState(
      TableName table, SpaceQuota spaceQuota) throws IOException {
    rlock.lock();
    try {
      final long sizeLimitInBytes = spaceQuota.getSoftLimit();
      long sum = 0L;
      for (Entry<HRegionInfo,Long> entry : filterBySubject(table)) {
        sum += entry.getValue();
      }
      // Add in the size for any snapshots against this table
      sum += getSnapshotSizesForTable(table);
      // Observance is defined as the size of the table being less than the limit
      SpaceQuotaStatus status = sum <= sizeLimitInBytes ? SpaceQuotaStatus.notInViolation()
          : new SpaceQuotaStatus(ProtobufUtil.toViolationPolicy(spaceQuota.getViolationPolicy()));
      return new SpaceQuotaSnapshot(status, sum, sizeLimitInBytes);
    } finally {
      rlock.unlock();
    }
  }

  /**
   * Fetches any serialized snapshot sizes from the quota table for the {@code tn} provided. Any
   * malformed records are skipped with a warning printed out.
   */
  long getSnapshotSizesForTable(TableName tn) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      Scan s = QuotaTableUtil.createScanForSpaceSnapshotSizes(tn);
      ResultScanner rs = quotaTable.getScanner(s);
      try {
        long size = 0L;
        // Should just be a single row (for our table)
        for (Result result : rs) {
          // May have multiple columns, one for each snapshot
          CellScanner cs = result.cellScanner();
          while (cs.advance()) {
            Cell current = cs.current();
            try {
              long snapshotSize = QuotaTableUtil.parseSnapshotSize(current);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Saw snapshot size of " + snapshotSize + " for " + current);
              }
              size += snapshotSize;
            } catch (InvalidProtocolBufferException e) {
              LOG.warn("Failed to parse snapshot size from cell: " + current);
            }
          }
        }
        return size;
      } finally {
        if (null != rs) {
          rs.close();
        }
      }
    }
  }

  @Override
  public Iterable<Entry<HRegionInfo,Long>> filterBySubject(TableName table) {
    rlock.lock();
    try {
      return Iterables.filter(regionUsage.entrySet(), new Predicate<Entry<HRegionInfo,Long>>() {
        @Override
        public boolean apply(Entry<HRegionInfo,Long> input) {
          return table.equals(input.getKey().getTable());
        }
      });
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public void setCurrentState(TableName table, SpaceQuotaSnapshot snapshot) {
    // Defer the "current state" to the chore
    this.chore.setTableQuotaSnapshot(table, snapshot);
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
