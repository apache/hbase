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

import java.util.Objects;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

/**
 * A point-in-time view of a space quota on a table.
 */
@InterfaceAudience.Private
public class SpaceQuotaSnapshot {
  private static final SpaceQuotaSnapshot NO_SUCH_SNAPSHOT = new SpaceQuotaSnapshot(
      SpaceQuotaStatus.notInViolation(), 0, Long.MAX_VALUE);
  private final SpaceQuotaStatus quotaStatus;
  private final long usage;
  private final long limit;

  /**
   * Encapsulates the state of a quota on a table. The quota may or may not be in violation.
   * If it is in violation, there will be a non-null violation policy.
   */
  @InterfaceAudience.Private
  public static class SpaceQuotaStatus {
    private static final SpaceQuotaStatus NOT_IN_VIOLATION = new SpaceQuotaStatus(null, false);
    final SpaceViolationPolicy policy;
    final boolean inViolation;

    public SpaceQuotaStatus(SpaceViolationPolicy policy) {
      this.policy = Objects.requireNonNull(policy);
      this.inViolation = true;
    }

    private SpaceQuotaStatus(SpaceViolationPolicy policy, boolean inViolation) {
      this.policy = policy;
      this.inViolation = inViolation;
    }

    /**
     * The violation policy which may be null. Is guaranteed to be non-null if
     * {@link #isInViolation()} is <code>true</code>, and <code>false</code>
     * otherwise.
     */
    public SpaceViolationPolicy getPolicy() {
      return policy;
    }

    /**
     * <code>true</code> if the quota is being violated, <code>false</code> otherwise.
     */
    public boolean isInViolation() {
      return inViolation;
    }

    /**
     * Returns a singleton referring to a quota which is not in violation.
     */
    public static SpaceQuotaStatus notInViolation() {
      return NOT_IN_VIOLATION;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(policy == null ? 0 : policy.hashCode())
          .append(inViolation).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SpaceQuotaStatus) {
        SpaceQuotaStatus other = (SpaceQuotaStatus) o;
        return Objects.equals(policy, other.policy) && inViolation == other.inViolation;
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append("[policy=").append(policy);
      sb.append(", inViolation=").append(inViolation).append("]");
      return sb.toString();
    }

    public static QuotaProtos.SpaceQuotaStatus toProto(SpaceQuotaStatus status) {
      QuotaProtos.SpaceQuotaStatus.Builder builder = QuotaProtos.SpaceQuotaStatus.newBuilder();
      builder.setInViolation(status.inViolation);
      if (status.isInViolation()) {
        builder.setPolicy(ProtobufUtil.toProtoViolationPolicy(status.getPolicy()));
      }
      return builder.build();
    }

    public static SpaceQuotaStatus toStatus(QuotaProtos.SpaceQuotaStatus proto) {
      if (proto.getInViolation()) {
        return new SpaceQuotaStatus(ProtobufUtil.toViolationPolicy(proto.getPolicy()));
      } else {
        return NOT_IN_VIOLATION;
      }
    }
  }

  public SpaceQuotaSnapshot(SpaceQuotaStatus quotaStatus, long usage, long limit) {
    this.quotaStatus = Objects.requireNonNull(quotaStatus);
    this.usage = usage;
    this.limit = limit;
  }

  /**
   * Returns the status of the quota.
   */
  public SpaceQuotaStatus getQuotaStatus() {
    return quotaStatus;
  }

  /**
   * Returns the current usage, in bytes, of the target (e.g. table, namespace).
   */
  public long getUsage() {
    return usage;
  }

  /**
   * Returns the limit, in bytes, of the target (e.g. table, namespace).
   */
  public long getLimit() {
    return limit;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(quotaStatus.hashCode())
        .append(usage)
        .append(limit)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SpaceQuotaSnapshot) {
      SpaceQuotaSnapshot other = (SpaceQuotaSnapshot) o;
      return quotaStatus.equals(other.quotaStatus) && usage == other.usage && limit == other.limit;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("SpaceQuotaSnapshot[policy=").append(quotaStatus).append(", use=");
    sb.append(usage).append("bytes/").append(limit).append("bytes]");
    return sb.toString();
  }

  // ProtobufUtil is in hbase-client, and this doesn't need to be public.
  public static SpaceQuotaSnapshot toSpaceQuotaSnapshot(QuotaProtos.SpaceQuotaSnapshot proto) {
    return new SpaceQuotaSnapshot(SpaceQuotaStatus.toStatus(proto.getStatus()),
        proto.getUsage(), proto.getLimit());
  }

  public static QuotaProtos.SpaceQuotaSnapshot toProtoSnapshot(SpaceQuotaSnapshot snapshot) {
    return QuotaProtos.SpaceQuotaSnapshot.newBuilder()
        .setStatus(SpaceQuotaStatus.toProto(snapshot.getQuotaStatus()))
        .setUsage(snapshot.getUsage()).setLimit(snapshot.getLimit()).build();
  }

  /**
   * Returns a singleton that corresponds to no snapshot information.
   */
  public static SpaceQuotaSnapshot getNoSuchSnapshot() {
    return NO_SUCH_SNAPSHOT;
  }
}
