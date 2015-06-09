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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QuotaSettingsFactory {
  static class QuotaGlobalsSettingsBypass extends QuotaSettings {
    private final boolean bypassGlobals;

    QuotaGlobalsSettingsBypass(final String userName, final TableName tableName,
      final String namespace, final boolean bypassGlobals) {
      super(userName, tableName, namespace);
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
  }

  /* ==========================================================================
   *  QuotaSettings from the Quotas object
   */
  static List<QuotaSettings> fromUserQuotas(final String userName, final Quotas quotas) {
    return fromQuotas(userName, null, null, quotas);
  }

  static List<QuotaSettings> fromUserQuotas(final String userName, final TableName tableName,
      final Quotas quotas) {
    return fromQuotas(userName, tableName, null, quotas);
  }

  static List<QuotaSettings> fromUserQuotas(final String userName, final String namespace,
      final Quotas quotas) {
    return fromQuotas(userName, null, namespace, quotas);
  }

  static List<QuotaSettings> fromTableQuotas(final TableName tableName, final Quotas quotas) {
    return fromQuotas(null, tableName, null, quotas);
  }

  static List<QuotaSettings> fromNamespaceQuotas(final String namespace, final Quotas quotas) {
    return fromQuotas(null, null, namespace, quotas);
  }

  private static List<QuotaSettings> fromQuotas(final String userName, final TableName tableName,
      final String namespace, final Quotas quotas) {
    List<QuotaSettings> settings = new ArrayList<QuotaSettings>();
    if (quotas.hasThrottle()) {
      settings.addAll(fromThrottle(userName, tableName, namespace, quotas.getThrottle()));
    }
    if (quotas.getBypassGlobals() == true) {
      settings.add(new QuotaGlobalsSettingsBypass(userName, tableName, namespace, true));
    }
    return settings;
  }

  private static List<QuotaSettings> fromThrottle(final String userName, final TableName tableName,
      final String namespace, final QuotaProtos.Throttle throttle) {
    List<QuotaSettings> settings = new ArrayList<QuotaSettings>();
    if (throttle.hasReqNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.REQUEST_NUMBER, throttle.getReqNum()));
    }
    if (throttle.hasReqSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.REQUEST_SIZE, throttle.getReqSize()));
    }
    if (throttle.hasWriteNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.WRITE_NUMBER, throttle.getWriteNum()));
    }
    if (throttle.hasWriteSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.WRITE_SIZE, throttle.getWriteSize()));
    }
    if (throttle.hasReadNum()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.READ_NUMBER, throttle.getReadNum()));
    }
    if (throttle.hasReadSize()) {
      settings.add(ThrottleSettings.fromTimedQuota(userName, tableName, namespace,
          ThrottleType.READ_SIZE, throttle.getReadSize()));
    }
    return settings;
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
    return throttle(userName, null, null, type, limit, timeUnit);
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
    return throttle(userName, tableName, null, type, limit, timeUnit);
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
    return throttle(userName, null, namespace, type, limit, timeUnit);
  }

  /**
   * Remove the throttling for the specified user.
   *
   * @param userName the user
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName) {
    return throttle(userName, null, null, null, 0, null);
  }

  /**
   * Remove the throttling for the specified user on the specified table.
   *
   * @param userName the user
   * @param tableName the table
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName, final TableName tableName) {
    return throttle(userName, tableName, null, null, 0, null);
  }

  /**
   * Remove the throttling for the specified user on the specified namespace.
   *
   * @param userName the user
   * @param namespace the namespace
   * @return the quota settings
   */
  public static QuotaSettings unthrottleUser(final String userName, final String namespace) {
    return throttle(userName, null, namespace, null, 0, null);
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
    return throttle(null, tableName, null, type, limit, timeUnit);
  }

  /**
   * Remove the throttling for the specified table.
   *
   * @param tableName the table
   * @return the quota settings
   */
  public static QuotaSettings unthrottleTable(final TableName tableName) {
    return throttle(null, tableName, null, null, 0, null);
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
    return throttle(null, null, namespace, type, limit, timeUnit);
  }

  /**
   * Remove the throttling for the specified namespace.
   *
   * @param namespace the namespace
   * @return the quota settings
   */
  public static QuotaSettings unthrottleNamespace(final String namespace) {
    return throttle(null, null, namespace, null, 0, null);
  }

  /* Throttle helper */
  private static QuotaSettings throttle(final String userName, final TableName tableName,
      final String namespace, final ThrottleType type, final long limit,
      final TimeUnit timeUnit) {
    QuotaProtos.ThrottleRequest.Builder builder = QuotaProtos.ThrottleRequest.newBuilder();
    if (type != null) {
      builder.setType(ProtobufUtil.toProtoThrottleType(type));
    }
    if (timeUnit != null) {
      builder.setTimedQuota(ProtobufUtil.toTimedQuota(limit, timeUnit, QuotaScope.MACHINE));
    }
    return new ThrottleSettings(userName, tableName, namespace, builder.build());
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
    return new QuotaGlobalsSettingsBypass(userName, null, null, bypassGlobals);
  }
}
