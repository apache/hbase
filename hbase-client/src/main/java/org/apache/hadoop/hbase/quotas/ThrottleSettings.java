/**
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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ThrottleSettings extends QuotaSettings {
  final QuotaProtos.ThrottleRequest proto;

  ThrottleSettings(final String userName, final TableName tableName, final String namespace,
      final String regionServer, final QuotaProtos.ThrottleRequest proto) {
    super(userName, tableName, namespace, regionServer);
    this.proto = proto;
  }

  public ThrottleType getThrottleType() {
    return ProtobufUtil.toThrottleType(proto.getType());
  }

  public long getSoftLimit() {
    return proto.hasTimedQuota() ? proto.getTimedQuota().getSoftLimit() : -1;
  }

  /**
   * Returns a copy of the internal state of <code>this</code>
   */
  QuotaProtos.ThrottleRequest getProto() {
    return proto.toBuilder().build();
  }

  public TimeUnit getTimeUnit() {
    return proto.hasTimedQuota() ?
      ProtobufUtil.toTimeUnit(proto.getTimedQuota().getTimeUnit()) : null;
  }

  public QuotaScope getQuotaScope() {
    return proto.hasTimedQuota() ? ProtobufUtil.toQuotaScope(proto.getTimedQuota().getScope())
        : null;
  }

  @Override
  public QuotaType getQuotaType() {
    return QuotaType.THROTTLE;
  }

  @Override
  protected void setupSetQuotaRequest(SetQuotaRequest.Builder builder) {
    builder.setThrottle(proto);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TYPE => THROTTLE");
    if (proto.hasType()) {
      builder.append(", THROTTLE_TYPE => ");
      builder.append(proto.getType().toString());
    }
    if (proto.hasTimedQuota()) {
      QuotaProtos.TimedQuota timedQuota = proto.getTimedQuota();
      builder.append(", LIMIT => ");
      if (timedQuota.hasSoftLimit()) {
        switch (getThrottleType()) {
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
            break;
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
    } else {
      builder.append(", LIMIT => NONE");
    }
    return builder.toString();
  }

  @Override
  protected ThrottleSettings merge(QuotaSettings other) throws IOException {
    if (other instanceof ThrottleSettings) {
      ThrottleSettings otherThrottle = (ThrottleSettings) other;

      // Make sure this and the other target the same "subject"
      validateQuotaTarget(other);

      QuotaProtos.ThrottleRequest.Builder builder = proto.toBuilder();
      if (!otherThrottle.proto.hasType()) {
        return null;
      }

      QuotaProtos.ThrottleRequest otherProto = otherThrottle.proto;
      if (otherProto.hasTimedQuota()) {
        if (otherProto.hasTimedQuota()) {
          validateTimedQuota(otherProto.getTimedQuota());
        }

        if (!proto.getType().equals(otherProto.getType())) {
          throw new IllegalArgumentException(
              "Cannot merge a ThrottleRequest for " + proto.getType() + " with " +
                  otherProto.getType());
        }
        QuotaProtos.TimedQuota.Builder timedQuotaBuilder = proto.getTimedQuota().toBuilder();
        timedQuotaBuilder.mergeFrom(otherProto.getTimedQuota());

        QuotaProtos.ThrottleRequest mergedReq = builder.setTimedQuota(
            timedQuotaBuilder.build()).build();
        return new ThrottleSettings(getUserName(), getTableName(), getNamespace(),
            getRegionServer(), mergedReq);
      }
    }
    return this;
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
    }
  }

  static ThrottleSettings fromTimedQuota(final String userName, final TableName tableName,
      final String namespace, final String regionServer, ThrottleType type,
      QuotaProtos.TimedQuota timedQuota) {
    QuotaProtos.ThrottleRequest.Builder builder = QuotaProtos.ThrottleRequest.newBuilder();
    builder.setType(ProtobufUtil.toProtoThrottleType(type));
    builder.setTimedQuota(timedQuota);
    return new ThrottleSettings(userName, tableName, namespace, regionServer, builder.build());
  }
}
