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
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

/**
 * Implementation of {@link GlobalQuotaSettings} to hide the Protobuf messages we use internally.
 */
@InterfaceAudience.Private
public class GlobalQuotaSettingsImpl extends GlobalQuotaSettings {

  private final QuotaProtos.Throttle throttleProto;
  private final Boolean bypassGlobals;
  private final QuotaProtos.SpaceQuota spaceProto;

  protected GlobalQuotaSettingsImpl(String username, TableName tableName, String namespace,
      String regionServer, QuotaProtos.Quotas quotas) {
    this(username, tableName, namespace, regionServer,
        (quotas != null && quotas.hasThrottle() ? quotas.getThrottle() : null),
        (quotas != null && quotas.hasBypassGlobals() ? quotas.getBypassGlobals() : null),
        (quotas != null && quotas.hasSpace() ? quotas.getSpace() : null));
  }

  protected GlobalQuotaSettingsImpl(String userName, TableName tableName, String namespace,
      String regionServer, QuotaProtos.Throttle throttleProto, Boolean bypassGlobals,
      QuotaProtos.SpaceQuota spaceProto) {
    super(userName, tableName, namespace, regionServer);
    this.throttleProto = throttleProto;
    this.bypassGlobals = bypassGlobals;
    this.spaceProto = spaceProto;
  }

  @Override
  public List<QuotaSettings> getQuotaSettings() {
    // Very similar to QuotaSettingsFactory
    List<QuotaSettings> settings = new ArrayList<>();
    if (throttleProto != null) {
      settings.addAll(QuotaSettingsFactory.fromThrottle(getUserName(), getTableName(),
        getNamespace(), getRegionServer(), throttleProto));
    }
    if (bypassGlobals != null && bypassGlobals.booleanValue()) {
      settings.add(new QuotaGlobalsSettingsBypass(getUserName(), getTableName(), getNamespace(),
          getRegionServer(), true));
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

  private boolean hasThrottle(QuotaProtos.ThrottleType quotaType,
      QuotaProtos.Throttle.Builder throttleBuilder) {
    boolean hasThrottle = false;
    switch (quotaType) {
      case REQUEST_NUMBER:
        if (throttleBuilder.hasReqNum()) {
          hasThrottle = true;
        }
        break;
      case REQUEST_SIZE:
        if (throttleBuilder.hasReqSize()) {
          hasThrottle = true;
        }
        break;
      case WRITE_NUMBER:
        if (throttleBuilder.hasWriteNum()) {
          hasThrottle = true;
        }
        break;
      case WRITE_SIZE:
        if (throttleBuilder.hasWriteSize()) {
          hasThrottle = true;
        }
        break;
      case READ_NUMBER:
        if (throttleBuilder.hasReadNum()) {
          hasThrottle = true;
        }
        break;
      case READ_SIZE:
        if (throttleBuilder.hasReadSize()) {
          hasThrottle = true;
        }
        break;
      case REQUEST_CAPACITY_UNIT:
        if (throttleBuilder.hasReqCapacityUnit()) {
          hasThrottle = true;
        }
        break;
      case READ_CAPACITY_UNIT:
        if (throttleBuilder.hasReadCapacityUnit()) {
          hasThrottle = true;
        }
        break;
      case WRITE_CAPACITY_UNIT:
        if (throttleBuilder.hasWriteCapacityUnit()) {
          hasThrottle = true;
        }
        break;
      default:
    }
    return hasThrottle;
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
        // It means it's a remove request
        // To prevent the "empty" row in QuotaTableUtil.QUOTA_TABLE_NAME

        QuotaProtos.ThrottleRequest otherProto = otherThrottle.proto;
        if (throttleBuilder != null && !otherThrottle.proto.hasTimedQuota() && otherThrottle.proto
            .hasType()) {
          switch (otherProto.getType()) {
            case REQUEST_NUMBER:
              throttleBuilder.clearReqNum();
              break;
            case REQUEST_SIZE:
              throttleBuilder.clearReqSize();
              break;
            case WRITE_NUMBER:
              throttleBuilder.clearWriteNum();
              break;
            case WRITE_SIZE:
              throttleBuilder.clearWriteSize();
              break;
            case READ_NUMBER:
              throttleBuilder.clearReadNum();
              break;
            case READ_SIZE:
              throttleBuilder.clearReadSize();
              break;
            case REQUEST_CAPACITY_UNIT:
              throttleBuilder.clearReqCapacityUnit();
              break;
            case READ_CAPACITY_UNIT:
              throttleBuilder.clearReadCapacityUnit();
              break;
            case WRITE_CAPACITY_UNIT:
              throttleBuilder.clearWriteCapacityUnit();
              break;
            default:
          }
          boolean hasThrottle = false;
          for (QuotaProtos.ThrottleType quotaType : QuotaProtos.ThrottleType.values()) {
            hasThrottle = hasThrottle(quotaType, throttleBuilder);
            if (hasThrottle) {
              break;
            }
          }
          if (!hasThrottle) {
            throttleBuilder = null;
          }
        } else {
          throttleBuilder = null;
        }

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
          case REQUEST_CAPACITY_UNIT:
            throttleBuilder.setReqCapacityUnit(otherProto.getTimedQuota());
            break;
          case READ_CAPACITY_UNIT:
            throttleBuilder.setReadCapacityUnit(otherProto.getTimedQuota());
            break;
          case WRITE_CAPACITY_UNIT:
            throttleBuilder.setWriteCapacityUnit(otherProto.getTimedQuota());
            break;
          default:
        }
      }
    }

    // Propagate the space quota portion
    QuotaProtos.SpaceQuota.Builder spaceBuilder =
        (spaceProto == null ? null : spaceProto.toBuilder());
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
        if (!Objects.equals(getTableName(), settingsToMerge.getTableName()) && !Objects
            .equals(getNamespace(), settingsToMerge.getNamespace())) {
          throw new IllegalArgumentException("Cannot merge " + settingsToMerge + " into " + this);
        }

        if (quotaToMerge.getRemove()) {
          // It means it's a remove request
          // Update the builder to propagate the removal
          spaceBuilder.setRemove(true).clearSoftLimit().clearViolationPolicy();
        } else {
          // Add the new settings to the existing settings
          spaceBuilder.mergeFrom(quotaToMerge);
        }
      }
    }

    boolean removeSpaceBuilder =
        (spaceBuilder == null) || (spaceBuilder.hasRemove() && spaceBuilder.getRemove());

    Boolean bypassGlobals = this.bypassGlobals;
    if (other instanceof QuotaGlobalsSettingsBypass) {
      bypassGlobals = ((QuotaGlobalsSettingsBypass) other).getBypass();
    }

    if (throttleBuilder == null && removeSpaceBuilder && bypassGlobals == null) {
      return null;
    }

    return new GlobalQuotaSettingsImpl(getUserName(), getTableName(), getNamespace(),
        getRegionServer(), (throttleBuilder == null ? null : throttleBuilder.build()),
        bypassGlobals, (removeSpaceBuilder ? null : spaceBuilder.build()));
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
            case REQUEST_CAPACITY_UNIT:
            case READ_CAPACITY_UNIT:
            case WRITE_CAPACITY_UNIT:
              builder.append(String.format("%dCU", timedQuota.getSoftLimit()));
            default:
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
        builder.append(", LIMIT => ").append(sizeToString(spaceProto.getSoftLimit()));
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
    if (proto.hasReqCapacityUnit()) {
      quotas.put(ThrottleType.REQUEST_CAPACITY_UNIT, proto.getReqCapacityUnit());
    }
    if (proto.hasReadCapacityUnit()) {
      quotas.put(ThrottleType.READ_CAPACITY_UNIT, proto.getReqCapacityUnit());
    }
    if (proto.hasWriteCapacityUnit()) {
      quotas.put(ThrottleType.WRITE_CAPACITY_UNIT, proto.getWriteCapacityUnit());
    }
    return quotas;
  }
}
