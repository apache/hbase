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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

@InterfaceAudience.Public
public class QuotaSettingsFactory {
  static class QuotaGlobalsSettingsBypass extends QuotaSettings {
    private final boolean bypassGlobals;

    QuotaGlobalsSettingsBypass(final String userName, final TableName tableName,
        final String namespace, final String regionServer, final boolean bypassGlobals) {
      super(userName, tableName, namespace, regionServer);
      this.bypassGlobals = bypassGlobals;
    }

    @Override
    public QuotaType getQuotaType() {
      return QuotaType.GLOBAL_BYPASS;
    }

    @Override
    protected void setupSetQuotaRequest(SetQuotaRequest.Builder builder) {
      builder.setBypassGlobals(bypassGlobals);
    }

    @Override
    public String toString() {
      return "GLOBAL_BYPASS => " + bypassGlobals;
    }

    protected boolean getBypass() {
      return bypassGlobals;
    }

    @Override
    protected QuotaGlobalsSettingsBypass merge(QuotaSettings newSettings) throws IOException {
      if (newSettings instanceof QuotaGlobalsSettingsBypass) {
        QuotaGlobalsSettingsBypass other = (QuotaGlobalsSettingsBypass) newSettings;

        validateQuotaTarget(other);

        if (getBypass() != other.getBypass()) {
          return other;
        }
      }
      return this;
    }
  }

  /* ==========================================================================
   *  QuotaSettings from the Quotas object
   */
  static List<QuotaSettings> fromUserQuotas(final String userName, final Quotas quotas) {
    return fromQuotas(userName, null, null, null, quotas);
  }

  static List<QuotaSettings> fromUserQuotas(final String userName, final TableName tableName,
      final Quotas quotas) {
    return fromQuotas(userName, tableName, null, null, quotas);
  }

  static List<QuotaSettings> fromUserQuotas(final String userName, final String namespace,
      final Quotas quotas) {
    return fromQuotas(userName, null, namespace, null, quotas);
  }

  static List<QuotaSettings> fromTableQuotas(final TableName tableName, final Quotas quotas) {
    return fromQuotas(null, tableName, null, null, quotas);
  }

  static List<QuotaSettings> fromNamespaceQuotas(final String namespace, final Quotas quotas) {
    return fromQuotas(null, null, namespace, null, quotas);
  }

  static List<QuotaSettings> fromRegionServerQuotas(final String regionServer,
      final Quotas quotas) {
    return fromQuotas(null, null, null, regionServer, quotas);
  }

  private static List<QuotaSettings> fromQuotas(final String userName, final TableName tableName,
      final String namespace, final String regionServer, final Quotas quotas) {
    List<QuotaSettings> settings = new ArrayList<>();
    if (quotas.hasThrottle()) {
      settings
          .addAll(fromThrottle(userName, tableName, namespace, regionServer, quotas.getThrottle()));
    }
    if (quotas.getBypassGlobals() == true) {
      settings
          .add(new QuotaGlobalsSettingsBypass(userName, tableName, namespace, regionServer, true));
    }
    if (quotas.hasSpace()) {
      settings.add(fromSpace(tableName, namespace, quotas.getSpace()));
    }
    return settings;
  }

  public static List<ThrottleSettings> fromTableThrottles(final TableName tableName,
      final QuotaProtos.Throttle throttle) {
    return fromThrottle(null, tableName, null, null, throttle);
  }

  protected static List<ThrottleSettings> fromThrottle(final String userName,
      final TableName tableName, final String namespace, final String regionServer,
      final QuotaProtos.Throttle throttle) {
    List<ThrottleSettings> settings = new ArrayList<>();
    if (throttle.hasReqNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.REQUEST_NUMBER, throttle.getReqNum()));
    }
    if (throttle.hasReqSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.REQUEST_SIZE, throttle.getReqSize()));
    }
    if (throttle.hasWriteNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.WRITE_NUMBER, throttle.getWriteNum()));
    }
    if (throttle.hasWriteSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.WRITE_SIZE, throttle.getWriteSize()));
    }
    if (throttle.hasReadNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.READ_NUMBER, throttle.getReadNum()));
    }
    if (throttle.hasReadSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.READ_SIZE, throttle.getReadSize()));
    }
    if (throttle.hasReqCapacityUnit()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.REQUEST_CAPACITY_UNIT, throttle.getReqCapacityUnit()));
    }
    if (throttle.hasReadCapacityUnit()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.READ_CAPACITY_UNIT, throttle.getReadCapacityUnit()));
    }
    if (throttle.hasWriteCapacityUnit()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace, regionServer,
        ThrottleType.WRITE_CAPACITY_UNIT, throttle.getWriteCapacityUnit()));
    }
    return settings;
  }

  static QuotaSettings fromSpace(TableName table, String namespace, SpaceQuota protoQuota) {
    if (protoQuota == null) {
      return null;
    }
    if ((table == null && namespace == null) || (table != null && namespace != null)) {
      throw new IllegalArgumentException(
          "Can only construct SpaceLimitSettings for a table or namespace.");
    }
    if (table != null) {
      if (protoQuota.getRemove()) {
        return new SpaceLimitSettings(table);
      }
      return SpaceLimitSettings.fromSpaceQuota(table, protoQuota);
    } else {
      if (protoQuota.getRemove()) {
        return new SpaceLimitSettings(namespace);
      }
      // namespace must be non-null
      return SpaceLimitSettings.fromSpaceQuota(namespace, protoQuota);
    }
  }

  /* ==========================================================================
   *  RPC Throttle
   */

  /**
   * Throttle the specified user.
   *
   * @param userName the user to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final ThrottleType type,
      final long limit, final TimeUnit timeUnit) {
    return throttleUser(userName, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified user.
   * @param userName the user to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the scope of throttling
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final ThrottleType type,
      final long limit, final TimeUnit timeUnit, QuotaScope scope) {
    return throttle(userName, null, null, null, type, limit, timeUnit, scope);
  }

  /**
   * Throttle the specified user on the specified table.
   *
   * @param userName the user to throttle
   * @param tableName the table to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final TableName tableName,
      final ThrottleType type, final long limit, final TimeUnit timeUnit) {
    return throttleUser(userName, tableName, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified user on the specified table.
   * @param userName the user to throttle
   * @param tableName the table to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the scope of throttling
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final TableName tableName,
      final ThrottleType type, final long limit, final TimeUnit timeUnit, QuotaScope scope) {
    return throttle(userName, tableName, null, null, type, limit, timeUnit, scope);
  }

  /**
   * Throttle the specified user on the specified namespace.
   *
   * @param userName the user to throttle
   * @param namespace the namespace to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final String namespace,
      final ThrottleType type, final long limit, final TimeUnit timeUnit) {
    return throttleUser(userName, namespace, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified user on the specified namespace.
   * @param userName the user to throttle
   * @param namespace the namespace to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the scope of throttling
   * @return the quota settings
   */
  public static QuotaSettings throttleUser(final String userName, final String namespace,
      final ThrottleType type, final long limit, final TimeUnit timeUnit, QuotaScope scope) {
    return throttle(userName, null, namespace, null, type, limit, timeUnit, scope);
  }

  /**
   * Remove the throttling for the specified user.
   *
   * @param userName the user
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName) {
    return throttle(userName, null, null, null, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified user.
   *
   * @param userName the user
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUserByThrottleType(final String userName,
      final ThrottleType type) {
    return throttle(userName, null, null, null, type, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified user on the specified table.
   *
   * @param userName the user
   * @param tableName the table
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName, final TableName tableName) {
    return throttle(userName, tableName, null, null, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified user on the specified table.
   *
   * @param userName the user
   * @param tableName the table
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUserByThrottleType(final String userName,
      final TableName tableName, final ThrottleType type) {
    return throttle(userName, tableName, null, null, type, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified user on the specified namespace.
   *
   * @param userName the user
   * @param namespace the namespace
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName, final String namespace) {
    return throttle(userName, null, namespace, null, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified user on the specified namespace.
   *
   * @param userName the user
   * @param namespace the namespace
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUserByThrottleType(final String userName,
      final String namespace, final ThrottleType type) {
    return throttle(userName, null, namespace, null, type, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified table.
   *
   * @param tableName the table to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleTable(final TableName tableName, final ThrottleType type,
      final long limit, final TimeUnit timeUnit) {
    return throttleTable(tableName, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified table.
   * @param tableName the table to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the scope of throttling
   * @return the quota settings
   */
  public static QuotaSettings throttleTable(final TableName tableName, final ThrottleType type,
      final long limit, final TimeUnit timeUnit, QuotaScope scope) {
    return throttle(null, tableName, null, null, type, limit, timeUnit, scope);
  }

  /**
   * Remove the throttling for the specified table.
   *
   * @param tableName the table
   * @return the quota settings
   */
  public static QuotaSettings unthrottleTable(final TableName tableName) {
    return throttle(null, tableName, null, null, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified table.
   *
   * @param tableName the table
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleTableByThrottleType(final TableName tableName,
      final ThrottleType type) {
    return throttle(null, tableName, null, null, type, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified namespace.
   *
   * @param namespace the namespace to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleNamespace(final String namespace, final ThrottleType type,
      final long limit, final TimeUnit timeUnit) {
    return throttleNamespace(namespace, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified namespace.
   * @param namespace the namespace to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @param scope the scope of throttling
   * @return the quota settings
   */
  public static QuotaSettings throttleNamespace(final String namespace, final ThrottleType type,
      final long limit, final TimeUnit timeUnit, QuotaScope scope) {
    return throttle(null, null, namespace, null, type, limit, timeUnit, scope);
  }

  /**
   * Remove the throttling for the specified namespace.
   *
   * @param namespace the namespace
   * @return the quota settings
   */
  public static QuotaSettings unthrottleNamespace(final String namespace) {
    return throttle(null, null, namespace, null, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified namespace by throttle type.
   *
   * @param namespace the namespace
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleNamespaceByThrottleType(final String namespace,
      final ThrottleType type) {
    return throttle(null, null, namespace, null, type, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Throttle the specified region server.
   *
   * @param regionServer the region server to throttle
   * @param type the type of throttling
   * @param limit the allowed number of request/data per timeUnit
   * @param timeUnit the limit time unit
   * @return the quota settings
   */
  public static QuotaSettings throttleRegionServer(final String regionServer,
      final ThrottleType type, final long limit, final TimeUnit timeUnit) {
    return throttle(null, null, null, regionServer, type, limit, timeUnit, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified region server.
   *
   * @param regionServer the region Server
   * @return the quota settings
   */
  public static QuotaSettings unthrottleRegionServer(final String regionServer) {
    return throttle(null, null, null, regionServer, null, 0, null, QuotaScope.MACHINE);
  }

  /**
   * Remove the throttling for the specified region server by throttle type.
   *
   * @param regionServer  the region Server
   * @param type the type of throttling
   * @return the quota settings
   */
  public static QuotaSettings unthrottleRegionServerByThrottleType(final String regionServer,
      final ThrottleType type) {
    return throttle(null, null, null, regionServer, type, 0, null, QuotaScope.MACHINE);
  }

  /* Throttle helper */
  private static QuotaSettings throttle(final String userName, final TableName tableName,
      final String namespace, final String regionServer, final ThrottleType type, final long limit,
      final TimeUnit timeUnit, QuotaScope scope) {
    QuotaProtos.ThrottleRequest.Builder builder = QuotaProtos.ThrottleRequest.newBuilder();
    if (type != null) {
      builder.setType(ProtobufUtil.toProtoThrottleType(type));
    }
    if (timeUnit != null) {
      builder.setTimedQuota(ProtobufUtil.toTimedQuota(limit, timeUnit, scope));
    }
    return new ThrottleSettings(userName, tableName, namespace, regionServer, builder.build());
  }

  /* ==========================================================================
   *  Global Settings
   */

  /**
   * Set the "bypass global settings" for the specified user
   *
   * @param userName the user to throttle
   * @param bypassGlobals true if the global settings should be bypassed
   * @return the quota settings
   */
  public static QuotaSettings bypassGlobals(final String userName, final boolean bypassGlobals) {
    return new QuotaGlobalsSettingsBypass(userName, null, null, null,  bypassGlobals);
  }

  /* ==========================================================================
   *  FileSystem Space Settings
   */

  /**
   * Creates a {@link QuotaSettings} object to limit the FileSystem space usage for the given table
   * to the given size in bytes. When the space usage is exceeded by the table, the provided
   * {@link SpaceViolationPolicy} is enacted on the table.
   *
   * @param tableName The name of the table on which the quota should be applied.
   * @param sizeLimit The limit of a table's size in bytes.
   * @param violationPolicy The action to take when the quota is exceeded.
   * @return An {@link QuotaSettings} object.
   */
  public static QuotaSettings limitTableSpace(
      final TableName tableName, long sizeLimit, final SpaceViolationPolicy violationPolicy) {
    return new SpaceLimitSettings(tableName, sizeLimit, violationPolicy);
  }

  /**
   * Creates a {@link QuotaSettings} object to remove the FileSystem space quota for the given
   * table.
   *
   * @param tableName The name of the table to remove the quota for.
   * @return A {@link QuotaSettings} object.
   */
  public static QuotaSettings removeTableSpaceLimit(TableName tableName) {
    return new SpaceLimitSettings(tableName);
  }

  /**
   * Creates a {@link QuotaSettings} object to limit the FileSystem space usage for the given
   * namespace to the given size in bytes. When the space usage is exceeded by all tables in the
   * namespace, the provided {@link SpaceViolationPolicy} is enacted on all tables in the namespace.
   *
   * @param namespace The namespace on which the quota should be applied.
   * @param sizeLimit The limit of the namespace's size in bytes.
   * @param violationPolicy The action to take when the the quota is exceeded.
   * @return An {@link QuotaSettings} object.
   */
  public static QuotaSettings limitNamespaceSpace(
      final String namespace, long sizeLimit, final SpaceViolationPolicy violationPolicy) {
    return new SpaceLimitSettings(namespace, sizeLimit, violationPolicy);
  }

  /**
   * Creates a {@link QuotaSettings} object to remove the FileSystem space quota for the given
   * namespace.
   *
   * @param namespace The namespace to remove the quota on.
   * @return A {@link QuotaSettings} object.
   */
  public static QuotaSettings removeNamespaceSpaceLimit(String namespace) {
    return new SpaceLimitSettings(namespace);
  }
}
