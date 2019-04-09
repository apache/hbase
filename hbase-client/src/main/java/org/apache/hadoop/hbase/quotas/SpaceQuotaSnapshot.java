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
import java.util.Optional;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.util.StringUtils;

/**
 * A point-in-time view of a space quota on a table.
 */
@InterfaceAudience.Private
public class SpaceQuotaSnapshot implements SpaceQuotaSnapshotView {
  private static final SpaceQuotaSnapshot NO_SUCH_SNAPSHOT = new SpaceQuotaSnapshot(
      SpaceQuotaStatus.notInViolation(), 0, Long.MAX_VALUE);
  private final SpaceQuotaStatus quotaStatus;
  private final long usage;
  private final long limit;

  /**
   * Encapsulates the state of a quota on a table. The quota may or may not be in violation.
   * If the quota is not in violation, the violation may be null. If the quota is in violation,
   * there is guaranteed to be a non-null violation policy.
   */
  @InterfaceAudience.Private
  public static class SpaceQuotaStatus implements SpaceQuotaStatusView {
    private static final SpaceQuotaStatus NOT_IN_VIOLATION = new SpaceQuotaStatus(null, false);
    final Optional<SpaceViolationPolicy> policy;
    final boolean inViolation;

    /**
     * Constructs a {@code SpaceQuotaSnapshot} which is in violation of the provided {@code policy}.
     * <p/>
     * Use {@link #notInViolation()} to obtain an instance of this class for the cases when the
     * quota is not in violation.
     * @param policy The non-null policy being violated.
     */
    public SpaceQuotaStatus(SpaceViolationPolicy policy) {
      // If the caller is instantiating a status, the policy must be non-null
      this(Objects.requireNonNull(policy), true);
    }

    private SpaceQuotaStatus(SpaceViolationPolicy policy, boolean inViolation) {
      this.policy = Optional.ofNullable(policy);
      this.inViolation = inViolation;
    }

    /**
     * Returns the violation policy, which may be null. It is guaranteed to be non-null if
     * {@link #isInViolation()} is {@code true}, but may be null otherwise.
     */
    @Override
    public Optional<SpaceViolationPolicy> getPolicy() {
      return policy;
    }

    /**
     * @return {@code true} if the quota is being violated, {@code false} otherwise.
     */
    @Override
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
        builder.setViolationPolicy(ProtobufUtil.toProtoViolationPolicy(status.getPolicy().get()));
      }
      return builder.build();
    }

    public static SpaceQuotaStatus toStatus(QuotaProtos.SpaceQuotaStatus proto) {
      if (proto.getInViolation()) {
        return new SpaceQuotaStatus(ProtobufUtil.toViolationPolicy(proto.getViolationPolicy()));
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
  @Override
  public SpaceQuotaStatus getQuotaStatus() {
    return quotaStatus;
  }

  /**
   * Returns the current usage, in bytes, of the target (e.g. table, namespace).
   */
  @Override
  public long getUsage() {
    return usage;
  }

  /**
   * Returns the limit, in bytes, of the target (e.g. table, namespace).
   */
  @Override
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
    sb.append(StringUtils.byteDesc(usage)).append("/");
    sb.append(StringUtils.byteDesc(limit)).append("]");
    return sb.toString();
  }

  // ProtobufUtil is in hbase-client, and this doesn't need to be public.
  public static SpaceQuotaSnapshot toSpaceQuotaSnapshot(QuotaProtos.SpaceQuotaSnapshot proto) {
    return new SpaceQuotaSnapshot(SpaceQuotaStatus.toStatus(proto.getQuotaStatus()),
        proto.getQuotaUsage(), proto.getQuotaLimit());
  }

  public static QuotaProtos.SpaceQuotaSnapshot toProtoSnapshot(SpaceQuotaSnapshot snapshot) {
    return QuotaProtos.SpaceQuotaSnapshot.newBuilder()
        .setQuotaStatus(SpaceQuotaStatus.toProto(snapshot.getQuotaStatus()))
        .setQuotaUsage(snapshot.getUsage()).setQuotaLimit(snapshot.getLimit()).build();
  }

  /**
   * Returns a singleton that corresponds to no snapshot information.
   */
  public static SpaceQuotaSnapshot getNoSuchSnapshot() {
    return NO_SUCH_SNAPSHOT;
  }
}
