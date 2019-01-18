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

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest.Builder;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceLimitRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * A {@link QuotaSettings} implementation for configuring filesystem-use quotas.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class SpaceLimitSettings extends QuotaSettings {

  private final SpaceLimitRequest proto;

  SpaceLimitSettings(TableName tableName, long sizeLimit, SpaceViolationPolicy violationPolicy) {
    super(null, Objects.requireNonNull(tableName), null, null);
    validateSizeLimit(sizeLimit);
    proto = buildProtoAddQuota(sizeLimit, Objects.requireNonNull(violationPolicy));
  }

  /**
   * Constructs a {@code SpaceLimitSettings} to remove a space quota on the given {@code tableName}.
   */
  SpaceLimitSettings(TableName tableName) {
    super(null, Objects.requireNonNull(tableName), null, null);
    proto = buildProtoRemoveQuota();
  }

  SpaceLimitSettings(String namespace, long sizeLimit, SpaceViolationPolicy violationPolicy) {
    super(null, null, Objects.requireNonNull(namespace), null);
    validateSizeLimit(sizeLimit);
    proto = buildProtoAddQuota(sizeLimit, Objects.requireNonNull(violationPolicy));
  }

  /**
   * Constructs a {@code SpaceLimitSettings} to remove a space quota on the given {@code namespace}.
   */
  SpaceLimitSettings(String namespace) {
    super(null, null, Objects.requireNonNull(namespace), null);
    proto = buildProtoRemoveQuota();
  }

  SpaceLimitSettings(TableName tableName, String namespace, SpaceLimitRequest req) {
    super(null, tableName, namespace, null);
    proto = req;
  }

  /**
   * Build a {@link SpaceLimitRequest} protobuf object from the given {@link SpaceQuota}.
   *
   * @param protoQuota The preconstructed SpaceQuota protobuf
   * @return A protobuf request to change a space limit quota
   */
  private SpaceLimitRequest buildProtoFromQuota(SpaceQuota protoQuota) {
    return SpaceLimitRequest.newBuilder().setQuota(protoQuota).build();
  }

  /**
   * Builds a {@link SpaceQuota} protobuf object given the arguments.
   *
   * @param sizeLimit The size limit of the quota.
   * @param violationPolicy The action to take when the quota is exceeded.
   * @return The protobuf SpaceQuota representation.
   */
  private SpaceLimitRequest buildProtoAddQuota(
      long sizeLimit, SpaceViolationPolicy violationPolicy) {
    return buildProtoFromQuota(SpaceQuota.newBuilder()
            .setSoftLimit(sizeLimit)
            .setViolationPolicy(ProtobufUtil.toProtoViolationPolicy(violationPolicy))
            .build());
  }

  /**
   * Builds a {@link SpaceQuota} protobuf object to remove a quota.
   *
   * @return The protobuf SpaceQuota representation.
   */
  private SpaceLimitRequest buildProtoRemoveQuota() {
    return SpaceLimitRequest.newBuilder().setQuota(
        SpaceQuota.newBuilder()
            .setRemove(true)
            .build())
        .build();
  }

  /**
   * Returns a copy of the internal state of <code>this</code>
   */
  SpaceLimitRequest getProto() {
    return proto.toBuilder().build();
  }

  @Override
  public QuotaType getQuotaType() {
    return QuotaType.SPACE;
  }

  @Override
  protected void setupSetQuotaRequest(Builder builder) {
    // TableName/Namespace are serialized in QuotaSettings
    builder.setSpaceLimit(proto);
  }

  /**
   * Constructs a {@link SpaceLimitSettings} from the provided protobuf message and tablename.
   *
   * @param tableName The target tablename for the limit.
   * @param proto The protobuf representation.
   * @return A QuotaSettings.
   */
  static SpaceLimitSettings fromSpaceQuota(
      final TableName tableName, final QuotaProtos.SpaceQuota proto) {
    validateProtoArguments(proto);
    return new SpaceLimitSettings(tableName, proto.getSoftLimit(),
        ProtobufUtil.toViolationPolicy(proto.getViolationPolicy()));
  }

  /**
   * Constructs a {@link SpaceLimitSettings} from the provided protobuf message and namespace.
   *
   * @param namespace The target namespace for the limit.
   * @param proto The protobuf representation.
   * @return A QuotaSettings.
   */
  static SpaceLimitSettings fromSpaceQuota(
      final String namespace, final QuotaProtos.SpaceQuota proto) {
    validateProtoArguments(proto);
    return new SpaceLimitSettings(namespace, proto.getSoftLimit(),
        ProtobufUtil.toViolationPolicy(proto.getViolationPolicy()));
  }

  /**
   * Validates that the provided protobuf SpaceQuota has the necessary information to construct
   * a {@link SpaceLimitSettings}.
   *
   * @param proto The protobuf message to validate.
   */
  static void validateProtoArguments(final QuotaProtos.SpaceQuota proto) {
    if (!Objects.requireNonNull(proto).hasSoftLimit()) {
      throw new IllegalArgumentException("Cannot handle SpaceQuota without a soft limit");
    }
    if (!proto.hasViolationPolicy()) {
      throw new IllegalArgumentException("Cannot handle SpaceQuota without a violation policy");
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTableName(), getNamespace(), proto);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpaceLimitSettings)) {
      return false;
    }
    // o is non-null and an instance of SpaceLimitSettings
    SpaceLimitSettings other = (SpaceLimitSettings) o;
    return Objects.equals(getTableName(), other.getTableName()) &&
        Objects.equals(getNamespace(), other.getNamespace()) &&
        Objects.equals(proto, other.proto);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TYPE => SPACE");
    if (getTableName() != null) {
      sb.append(", TABLE => ").append(getTableName());
    }
    if (getNamespace() != null) {
      sb.append(", NAMESPACE => ").append(getNamespace());
    }
    if (proto.getQuota().getRemove()) {
      sb.append(", REMOVE => ").append(proto.getQuota().getRemove());
    } else {
      sb.append(", LIMIT => ").append(sizeToString(proto.getQuota().getSoftLimit()));
      sb.append(", VIOLATION_POLICY => ").append(proto.getQuota().getViolationPolicy());
    }
    return sb.toString();
  }

  @Override
  protected QuotaSettings merge(QuotaSettings newSettings) {
    if (newSettings instanceof SpaceLimitSettings) {
      SpaceLimitSettings settingsToMerge = (SpaceLimitSettings) newSettings;

      // The message contained the expect SpaceQuota object
      if (settingsToMerge.proto.hasQuota()) {
        SpaceQuota quotaToMerge = settingsToMerge.proto.getQuota();
        if (quotaToMerge.getRemove()) {
          return settingsToMerge;
        } else {
          // Validate that the two settings are for the same target.
          // SpaceQuotas either apply to a table or a namespace (no user spacequota).
          if (!Objects.equals(getTableName(), settingsToMerge.getTableName())
              && !Objects.equals(getNamespace(), settingsToMerge.getNamespace())) {
            throw new IllegalArgumentException("Cannot merge " + newSettings + " into " + this);
          }
          // Create a builder from the old settings
          SpaceQuota.Builder mergedBuilder = this.proto.getQuota().toBuilder();
          // Build a new SpaceQuotas object from merging in the new settings
          return new SpaceLimitSettings(
              getTableName(), getNamespace(),
              buildProtoFromQuota(mergedBuilder.mergeFrom(quotaToMerge).build()));
        }
      }
      // else, we don't know what to do, so return the original object
    }
    return this;
  }

  // Helper function to validate sizeLimit
  private void validateSizeLimit(long sizeLimit) {
    if (sizeLimit < 0L) {
      throw new IllegalArgumentException("Size limit must be a non-negative value.");
    }
  }
}
