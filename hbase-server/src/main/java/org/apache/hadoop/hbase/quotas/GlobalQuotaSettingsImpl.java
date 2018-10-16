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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory.QuotaGlobalsSettingsBypass;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link GlobalQuotaSettings} to hide the Protobuf messages we use internally.
 */
@InterfaceAudience.Private
public class GlobalQuotaSettingsImpl extends GlobalQuotaSettings {

  private final QuotaProtos.Throttle throttleProto;
  private final Boolean bypassGlobals;
  private final QuotaProtos.SpaceQuota spaceProto;

  protected GlobalQuotaSettingsImpl(
      String username, TableName tableName, String namespace, QuotaProtos.Quotas quotas) {
    this(username, tableName, namespace,
        (quotas != null && quotas.hasThrottle() ? quotas.getThrottle() : null),
        (quotas != null && quotas.hasBypassGlobals() ? quotas.getBypassGlobals() : null),
        (quotas != null && quotas.hasSpace() ? quotas.getSpace() : null));
  }

  protected GlobalQuotaSettingsImpl(
      String userName, TableName tableName, String namespace, QuotaProtos.Throttle throttleProto,
      Boolean bypassGlobals, QuotaProtos.SpaceQuota spaceProto) {
    super(userName, tableName, namespace);
    this.throttleProto = throttleProto;
    this.bypassGlobals = bypassGlobals;
    this.spaceProto = spaceProto;
  }

  @Override
  public List<QuotaSettings> getQuotaSettings() {
    // Very similar to QuotaSettingsFactory
    List<QuotaSettings> settings = new ArrayList<>();
    if (throttleProto != null) {
      settings.addAll(QuotaSettingsFactory.fromThrottle(
          getUserName(), getTableName(), getNamespace(), throttleProto));
    }
    if (bypassGlobals != null && bypassGlobals.booleanValue()) {
      settings.add(new QuotaGlobalsSettingsBypass(
          getUserName(), getTableName(), getNamespace(), true));
    }
    if (spaceProto != null) {
      settings.add(QuotaSettingsFactory.fromSpace(getTableName(), getNamespace(), spaceProto));
    }
    return settings;
  }

  protected QuotaProtos.Throttle getThrottleProto() {
    return this.throttleProto;
  }

  protected Boolean getBypassGlobals() {
    return this.bypassGlobals;
  }

  protected QuotaProtos.SpaceQuota getSpaceProto() {
    return this.spaceProto;
  }

  /**
   * Constructs a new {@link Quotas} message from {@code this}.
   */
  protected Quotas toQuotas() {
    QuotaProtos.Quotas.Builder builder = QuotaProtos.Quotas.newBuilder();
    if (getThrottleProto() != null) {
      builder.setThrottle(getThrottleProto());
    }
    if (getBypassGlobals() != null) {
      builder.setBypassGlobals(getBypassGlobals());
    }
    if (getSpaceProto() != null) {
      builder.setSpace(getSpaceProto());
    }
    return builder.build();
  }

  @Override
  protected GlobalQuotaSettingsImpl merge(QuotaSettings other) throws IOException {
    // Validate the quota subject
    validateQuotaTarget(other);

    // Propagate the Throttle
    QuotaProtos.Throttle.Builder throttleBuilder =
        throttleProto == null ? null : throttleProto.toBuilder();

    if (other instanceof ThrottleSettings) {
      ThrottleSettings otherThrottle = (ThrottleSettings) other;
      if (!otherThrottle.proto.hasType() || !otherThrottle.proto.hasTimedQuota()) {
        // To prevent the "empty" row in QuotaTableUtil.QUOTA_TABLE_NAME
        throttleBuilder = null;
      } else {
        QuotaProtos.ThrottleRequest otherProto = otherThrottle.proto;
        validateTimedQuota(otherProto.getTimedQuota());
        if (throttleBuilder == null) {
          throttleBuilder = QuotaProtos.Throttle.newBuilder();
        }

        switch (otherProto.getType()) {
          case REQUEST_NUMBER:
            throttleBuilder.setReqNum(otherProto.getTimedQuota());
            break;
          case REQUEST_SIZE:
            throttleBuilder.setReqSize(otherProto.getTimedQuota());
            break;
          case WRITE_NUMBER:
            throttleBuilder.setWriteNum(otherProto.getTimedQuota());
            break;
          case WRITE_SIZE:
            throttleBuilder.setWriteSize(otherProto.getTimedQuota());
            break;
          case READ_NUMBER:
            throttleBuilder.setReadNum(otherProto.getTimedQuota());
            break;
          case READ_SIZE:
            throttleBuilder.setReadSize(otherProto.getTimedQuota());
            break;
        }
      }
    }

    // Propagate the space quota portion
    QuotaProtos.SpaceQuota.Builder spaceBuilder = (spaceProto == null
        ? null : spaceProto.toBuilder());
    if (other instanceof SpaceLimitSettings) {
      if (spaceBuilder == null) {
        spaceBuilder = QuotaProtos.SpaceQuota.newBuilder();
      }
      SpaceLimitSettings settingsToMerge = (SpaceLimitSettings) other;

      QuotaProtos.SpaceLimitRequest spaceRequest = settingsToMerge.getProto();

      // The message contained the expect SpaceQuota object
      if (spaceRequest.hasQuota()) {
        SpaceQuota quotaToMerge = spaceRequest.getQuota();
        // Validate that the two settings are for the same target.
        // SpaceQuotas either apply to a table or a namespace (no user spacequota).
        if (!Objects.equals(getTableName(), settingsToMerge.getTableName())
            && !Objects.equals(getNamespace(), settingsToMerge.getNamespace())) {
          throw new IllegalArgumentException(
              "Cannot merge " + settingsToMerge + " into " + this);
        }

        if (quotaToMerge.getRemove()) {
          // Update the builder to propagate the removal
          spaceBuilder.setRemove(true).clearSoftLimit().clearViolationPolicy();
        } else {
          // Add the new settings to the existing settings
          spaceBuilder.mergeFrom(quotaToMerge);
        }
      }
    }

    Boolean bypassGlobals = this.bypassGlobals;
    if (other instanceof QuotaGlobalsSettingsBypass) {
      bypassGlobals = ((QuotaGlobalsSettingsBypass) other).getBypass();
    }

    if (throttleBuilder == null &&
        (spaceBuilder == null || (spaceBuilder.hasRemove() && spaceBuilder.getRemove()))
        && bypassGlobals == null) {
      return null;
    }

    return new GlobalQuotaSettingsImpl(
        getUserName(), getTableName(), getNamespace(),
        (throttleBuilder == null ? null : throttleBuilder.build()), bypassGlobals,
        (spaceBuilder == null ? null : spaceBuilder.build()));
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("GlobalQuota: ");
    if (throttleProto != null) {
      Map<ThrottleType,TimedQuota> throttleQuotas = buildThrottleQuotas(throttleProto);
      builder.append(" { TYPE => THROTTLE ");
      for (Entry<ThrottleType,TimedQuota> entry : throttleQuotas.entrySet()) {
        final ThrottleType type = entry.getKey();
        final TimedQuota timedQuota = entry.getValue();
        builder.append("{THROTTLE_TYPE => ").append(type.name()).append(", LIMIT => ");
        if (timedQuota.hasSoftLimit()) {
          switch (type) {
            case REQUEST_NUMBER:
            case WRITE_NUMBER:
            case READ_NUMBER:
              builder.append(String.format("%dreq", timedQuota.getSoftLimit()));
              break;
            case REQUEST_SIZE:
            case WRITE_SIZE:
            case READ_SIZE:
              builder.append(sizeToString(timedQuota.getSoftLimit()));
              break;
          }
        } else if (timedQuota.hasShare()) {
          builder.append(String.format("%.2f%%", timedQuota.getShare()));
        }
        builder.append('/');
        builder.append(timeToString(ProtobufUtil.toTimeUnit(timedQuota.getTimeUnit())));
        if (timedQuota.hasScope()) {
          builder.append(", SCOPE => ");
          builder.append(timedQuota.getScope().toString());
        }
      }
      builder.append( "} } ");
    } else {
      builder.append(" {} ");
    }
    if (bypassGlobals != null) {
      builder.append(" { GLOBAL_BYPASS => " + bypassGlobals + " } ");
    }
    if (spaceProto != null) {
      builder.append(" { TYPE => SPACE");
      if (getTableName() != null) {
        builder.append(", TABLE => ").append(getTableName());
      }
      if (getNamespace() != null) {
        builder.append(", NAMESPACE => ").append(getNamespace());
      }
      if (spaceProto.getRemove()) {
        builder.append(", REMOVE => ").append(spaceProto.getRemove());
      } else {
        builder.append(", LIMIT => ").append(spaceProto.getSoftLimit());
        builder.append(", VIOLATION_POLICY => ").append(spaceProto.getViolationPolicy());
      }
      builder.append(" } ");
    }
    return builder.toString();
  }

  private Map<ThrottleType,TimedQuota> buildThrottleQuotas(Throttle proto) {
    HashMap<ThrottleType,TimedQuota> quotas = new HashMap<>();
    if (proto.hasReadNum()) {
      quotas.put(ThrottleType.READ_NUMBER, proto.getReadNum());
    }
    if (proto.hasReadSize()) {
      quotas.put(ThrottleType.READ_SIZE, proto.getReadSize());
    }
    if (proto.hasReqNum()) {
      quotas.put(ThrottleType.REQUEST_NUMBER, proto.getReqNum());
    }
    if (proto.hasReqSize()) {
      quotas.put(ThrottleType.REQUEST_SIZE, proto.getReqSize());
    }
    if (proto.hasWriteNum()) {
      quotas.put(ThrottleType.WRITE_NUMBER, proto.getWriteNum());
    }
    if (proto.hasWriteSize()) {
      quotas.put(ThrottleType.WRITE_SIZE, proto.getWriteSize());
    }
    return quotas;
  }

  private void clearThrottleBuilder(QuotaProtos.Throttle.Builder builder) {
    builder.clearReadNum();
    builder.clearReadSize();
    builder.clearReqNum();
    builder.clearReqSize();
    builder.clearWriteNum();
    builder.clearWriteSize();
  }
}
