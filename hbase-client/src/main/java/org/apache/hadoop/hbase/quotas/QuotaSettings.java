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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory.QuotaGlobalsSettingsBypass;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;

@InterfaceAudience.Public
public abstract class QuotaSettings {
  private final String userName;
  private final String namespace;
  private final TableName tableName;
  private final String regionServer;

  protected QuotaSettings(final String userName, final TableName tableName, final String namespace,
      final String regionServer) {
    this.userName = userName;
    this.namespace = namespace;
    this.tableName = tableName;
    this.regionServer = regionServer;
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

  public String getRegionServer() {
    return regionServer;
  }

  /**
   * Converts the protocol buffer request into a QuotaSetting POJO. Arbitrarily
   * enforces that the request only contain one "limit", despite the message
   * allowing multiple. The public API does not allow such use of the message.
   *
   * @param request The protocol buffer request.
   * @return A {@link QuotaSettings} POJO.
   */
  @InterfaceAudience.Private
  public static QuotaSettings buildFromProto(SetQuotaRequest request) {
    String username = null;
    if (request.hasUserName()) {
      username = request.getUserName();
    }
    TableName tableName = null;
    if (request.hasTableName()) {
      tableName = ProtobufUtil.toTableName(request.getTableName());
    }
    String namespace = null;
    if (request.hasNamespace()) {
      namespace = request.getNamespace();
    }
    String regionServer = null;
    if (request.hasRegionServer()) {
      regionServer = request.getRegionServer();
    }
    if (request.hasBypassGlobals()) {
      // Make sure we don't have either of the two below limits also included
      if (request.hasSpaceLimit() || request.hasThrottle()) {
        throw new IllegalStateException(
            "SetQuotaRequest has multiple limits: " + TextFormat.shortDebugString(request));
      }
      return new QuotaGlobalsSettingsBypass(
          username, tableName, namespace, regionServer, request.getBypassGlobals());
    } else if (request.hasSpaceLimit()) {
      // Make sure we don't have the below limit as well
      if (request.hasThrottle()) {
        throw new IllegalStateException(
            "SetQuotaRequests has multiple limits: " + TextFormat.shortDebugString(request));
      }
      // Sanity check on the pb received.
      if (!request.getSpaceLimit().hasQuota()) {
        throw new IllegalArgumentException(
            "SpaceLimitRequest is missing the expected SpaceQuota.");
      }
      return QuotaSettingsFactory.fromSpace(
          tableName, namespace, request.getSpaceLimit().getQuota());
    } else if (request.hasThrottle()) {
      return new ThrottleSettings(username, tableName, namespace, regionServer,
          request.getThrottle());
    } else {
      throw new IllegalStateException("Unhandled SetRequestRequest state");
    }
  }

  /**
   * Convert a QuotaSettings to a protocol buffer SetQuotaRequest.
   * This is used internally by the Admin client to serialize the quota settings
   * and send them to the master.
   */
  @InterfaceAudience.Private
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
    if (settings.getRegionServer() != null) {
      builder.setRegionServer(settings.getRegionServer());
    }
    settings.setupSetQuotaRequest(builder);
    return builder.build();
  }

  /**
   * Called by toSetQuotaRequestProto()
   * the subclass should implement this method to set the specific SetQuotaRequest
   * properties.
   */
  @InterfaceAudience.Private
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
    if (regionServer != null) {
      builder.append("REGIONSERVER => ").append(regionServer).append(", ");
    }
    return builder.toString();
  }

  protected static String sizeToString(final long size) {
    if (size >= (1L << 50)) {
      return String.format("%.2fP", (double)size / (1L << 50));
    }
    if (size >= (1L << 40)) {
      return String.format("%.2fT", (double)size / (1L << 40));
    }
    if (size >= (1L << 30)) {
      return String.format("%.2fG", (double)size / (1L << 30));
    }
    if (size >= (1L << 20)) {
      return String.format("%.2fM", (double)size / (1L << 20));
    }
    if (size >= (1L << 10)) {
      return String.format("%.2fK", (double)size / (1L << 10));
    }
    return String.format("%.2fB", (double)size);
  }

  protected static String timeToString(final TimeUnit timeUnit) {
    switch (timeUnit) {
      case NANOSECONDS:  return "nsec";
      case MICROSECONDS: return "usec";
      case MILLISECONDS: return "msec";
      case SECONDS:      return "sec";
      case MINUTES:      return "min";
      case HOURS:        return "hour";
      case DAYS:         return "day";
    }
    throw new RuntimeException("Invalid TimeUnit " + timeUnit);
  }

  /**
   * Merges the provided settings with {@code this} and returns a new settings
   * object to the caller if the merged settings differ from the original.
   *
   * @param newSettings The new settings to merge in.
   * @return The merged {@link QuotaSettings} object or null if the quota should be deleted.
   */
  abstract QuotaSettings merge(QuotaSettings newSettings) throws IOException;

  /**
   * Validates that settings being merged into {@code this} is targeting the same "subject", e.g.
   * user, table, namespace.
   *
   * @param mergee The quota settings to be merged into {@code this}.
   * @throws IllegalArgumentException if the subjects are not equal.
   */
  void validateQuotaTarget(QuotaSettings mergee) {
    if (!Objects.equals(getUserName(), mergee.getUserName())) {
      throw new IllegalArgumentException("Mismatched user names on settings to merge");
    }
    if (!Objects.equals(getTableName(), mergee.getTableName())) {
      throw new IllegalArgumentException("Mismatched table names on settings to merge");
    }
    if (!Objects.equals(getNamespace(), mergee.getNamespace())) {
      throw new IllegalArgumentException("Mismatched namespace on settings to merge");
    }
    if (!Objects.equals(getRegionServer(), mergee.getRegionServer())) {
      throw new IllegalArgumentException("Mismatched region server on settings to merge");
    }
  }
}
