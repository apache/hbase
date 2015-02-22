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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;

@InterfaceAudience.Private
@InterfaceStability.Evolving
class ThrottleSettings extends QuotaSettings {
  private final QuotaProtos.ThrottleRequest proto;

  ThrottleSettings(final String userName, final TableName tableName,
      final String namespace, final QuotaProtos.ThrottleRequest proto) {
    super(userName, tableName, namespace);
    this.proto = proto;
  }

  public ThrottleType getThrottleType() {
    return ProtobufUtil.toThrottleType(proto.getType());
  }

  public long getSoftLimit() {
    return proto.hasTimedQuota() ? proto.getTimedQuota().getSoftLimit() : -1;
  }

  public TimeUnit getTimeUnit() {
    return proto.hasTimedQuota() ?
      ProtobufUtil.toTimeUnit(proto.getTimedQuota().getTimeUnit()) : null;
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
            builder.append(String.format("%dreq", timedQuota.getSoftLimit()));
            break;
          case REQUEST_SIZE:
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
    } else {
      builder.append(", LIMIT => NONE");
    }
    return builder.toString();
  }

  static ThrottleSettings fromTimedQuota(final String userName,
      final TableName tableName, final String namespace,
      ThrottleType type, QuotaProtos.TimedQuota timedQuota) {
    QuotaProtos.ThrottleRequest.Builder builder = QuotaProtos.ThrottleRequest.newBuilder();
    builder.setType(ProtobufUtil.toProtoThrottleType(type));
    builder.setTimedQuota(timedQuota);
    return new ThrottleSettings(userName, tableName, namespace, builder.build());
  }
}
