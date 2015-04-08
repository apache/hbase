/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * In-Memory state of the user quotas
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class UserQuotaState extends QuotaState {
  private Map<String, QuotaLimiter> namespaceLimiters = null;
  private Map<TableName, QuotaLimiter> tableLimiters = null;
  private boolean bypassGlobals = false;

  public UserQuotaState() {
    super();
  }

  public UserQuotaState(final long updateTs) {
    super(updateTs);
  }

  @Override
  public synchronized String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("UserQuotaState(ts=" + getLastUpdate());
    if (bypassGlobals) builder.append(" bypass-globals");

    if (isBypass()) {
      builder.append(" bypass");
    } else {
      if (getGlobalLimiterWithoutUpdatingLastQuery() != NoopQuotaLimiter.get()) {
        builder.append(" global-limiter");
      }

      if (tableLimiters != null && !tableLimiters.isEmpty()) {
        builder.append(" [");
        for (TableName table : tableLimiters.keySet()) {
          builder.append(" " + table);
        }
        builder.append(" ]");
      }

      if (namespaceLimiters != null && !namespaceLimiters.isEmpty()) {
        builder.append(" [");
        for (String ns : namespaceLimiters.keySet()) {
          builder.append(" " + ns);
        }
        builder.append(" ]");
      }
    }
    builder.append(')');
    return builder.toString();
  }

  /**
   * @return true if there is no quota information associated to this object
   */
  @Override
  public synchronized boolean isBypass() {
    return !bypassGlobals && getGlobalLimiterWithoutUpdatingLastQuery() == NoopQuotaLimiter.get()
        && (tableLimiters == null || tableLimiters.isEmpty())
        && (namespaceLimiters == null || namespaceLimiters.isEmpty());
  }

  public synchronized boolean hasBypassGlobals() {
    return bypassGlobals;
  }

  @Override
  public synchronized void setQuotas(final Quotas quotas) {
    super.setQuotas(quotas);
    bypassGlobals = quotas.getBypassGlobals();
  }

  /**
   * Add the quota information of the specified table. (This operation is part of the QuotaState
   * setup)
   */
  public synchronized void setQuotas(final TableName table, Quotas quotas) {
    tableLimiters = setLimiter(tableLimiters, table, quotas);
  }

  /**
   * Add the quota information of the specified namespace. (This operation is part of the QuotaState
   * setup)
   */
  public synchronized void setQuotas(final String namespace, Quotas quotas) {
    namespaceLimiters = setLimiter(namespaceLimiters, namespace, quotas);
  }

  private <K> Map<K, QuotaLimiter> setLimiter(Map<K, QuotaLimiter> limiters, final K key,
      final Quotas quotas) {
    if (limiters == null) {
      limiters = new HashMap<K, QuotaLimiter>();
    }

    QuotaLimiter limiter =
        quotas.hasThrottle() ? QuotaLimiterFactory.fromThrottle(quotas.getThrottle()) : null;
    if (limiter != null && !limiter.isBypass()) {
      limiters.put(key, limiter);
    } else {
      limiters.remove(key);
    }
    return limiters;
  }

  /**
   * Perform an update of the quota state based on the other quota state object. (This operation is
   * executed by the QuotaCache)
   */
  @Override
  public synchronized void update(final QuotaState other) {
    super.update(other);

    if (other instanceof UserQuotaState) {
      UserQuotaState uOther = (UserQuotaState) other;
      tableLimiters = updateLimiters(tableLimiters, uOther.tableLimiters);
      namespaceLimiters = updateLimiters(namespaceLimiters, uOther.namespaceLimiters);
      bypassGlobals = uOther.bypassGlobals;
    } else {
      tableLimiters = null;
      namespaceLimiters = null;
      bypassGlobals = false;
    }
  }

  private static <K> Map<K, QuotaLimiter> updateLimiters(final Map<K, QuotaLimiter> map,
      final Map<K, QuotaLimiter> otherMap) {
    if (map == null) {
      return otherMap;
    }

    if (otherMap != null) {
      // To Remove
      Set<K> toRemove = new HashSet<K>(map.keySet());
      toRemove.removeAll(otherMap.keySet());
      map.keySet().removeAll(toRemove);

      // To Update/Add
      for (final Map.Entry<K, QuotaLimiter> entry : otherMap.entrySet()) {
        QuotaLimiter limiter = map.get(entry.getKey());
        if (limiter == null) {
          limiter = entry.getValue();
        } else {
          limiter = QuotaLimiterFactory.update(limiter, entry.getValue());
        }
        map.put(entry.getKey(), limiter);
      }
      return map;
    }
    return null;
  }

  /**
   * Return the limiter for the specified table associated with this quota. If the table does not
   * have its own quota limiter the global one will be returned. In case there is no quota limiter
   * associated with this object a noop limiter will be returned.
   * @return the quota limiter for the specified table
   */
  public synchronized QuotaLimiter getTableLimiter(final TableName table) {
    setLastQuery(EnvironmentEdgeManager.currentTime());
    if (tableLimiters != null) {
      QuotaLimiter limiter = tableLimiters.get(table);
      if (limiter != null) return limiter;
    }
    if (namespaceLimiters != null) {
      QuotaLimiter limiter = namespaceLimiters.get(table.getNamespaceAsString());
      if (limiter != null) return limiter;
    }
    return getGlobalLimiterWithoutUpdatingLastQuery();
  }
}
