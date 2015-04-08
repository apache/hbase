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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class QuotaSettings {
  private final String userName;
  private final String namespace;
  private final TableName tableName;

  protected QuotaSettings(final String userName, final TableName tableName, 
      final String namespace) {
    this.userName = userName;
    this.namespace = namespace;
    this.tableName = tableName;
  }

  public abstract QuotaType getQuotaType();

  public String getUserName() {
    return userName;
  }

  public TableName getTableName() {
    return tableName;
  }

  public String getNamespace() {
    return namespace;
  }

  /**
   * Convert a QuotaSettings to a protocol buffer SetQuotaRequest. This is used internally by the
   * Admin client to serialize the quota settings and send them to the master.
   */
  public static SetQuotaRequest buildSetQuotaRequestProto(final QuotaSettings settings) {
    SetQuotaRequest.Builder builder = SetQuotaRequest.newBuilder();
    if (settings.getUserName() != null) {
      builder.setUserName(settings.getUserName());
    }
    if (settings.getTableName() != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(settings.getTableName()));
    }
    if (settings.getNamespace() != null) {
      builder.setNamespace(settings.getNamespace());
    }
    settings.setupSetQuotaRequest(builder);
    return builder.build();
  }

  /**
   * Called by toSetQuotaRequestProto() the subclass should implement this method to set the
   * specific SetQuotaRequest properties.
   */
  protected abstract void setupSetQuotaRequest(SetQuotaRequest.Builder builder);

  protected String ownerToString() {
    StringBuilder builder = new StringBuilder();
    if (userName != null) {
      builder.append("USER => '");
      builder.append(userName);
      builder.append("', ");
    }
    if (tableName != null) {
      builder.append("TABLE => '");
      builder.append(tableName.toString());
      builder.append("', ");
    }
    if (namespace != null) {
      builder.append("NAMESPACE => '");
      builder.append(namespace);
      builder.append("', ");
    }
    return builder.toString();
  }

  protected static String sizeToString(final long size) {
    if (size >= (1L << 50)) return String.format("%dP", size / (1L << 50));
    if (size >= (1L << 40)) return String.format("%dT", size / (1L << 40));
    if (size >= (1L << 30)) return String.format("%dG", size / (1L << 30));
    if (size >= (1L << 20)) return String.format("%dM", size / (1L << 20));
    if (size >= (1L << 10)) return String.format("%dK", size / (1L << 10));
    return String.format("%dB", size);
  }

  protected static String timeToString(final TimeUnit timeUnit) {
    switch (timeUnit) {
    case NANOSECONDS:
      return "nsec";
    case MICROSECONDS:
      return "usec";
    case MILLISECONDS:
      return "msec";
    case SECONDS:
      return "sec";
    case MINUTES:
      return "min";
    case HOURS:
      return "hour";
    case DAYS:
      return "day";
    default:
      throw new RuntimeException("Invalid TimeUnit " + timeUnit);
    }
  }
}
