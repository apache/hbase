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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.QuotaScope;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

/**
 * Helper class to interact with the quota table
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaUtil extends QuotaTableUtil {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaUtil.class);

  public static final String QUOTA_CONF_KEY = "hbase.quota.enabled";
  private static final boolean QUOTA_ENABLED_DEFAULT = false;

  public static final String READ_CAPACITY_UNIT_CONF_KEY = "hbase.quota.read.capacity.unit";
  // the default one read capacity unit is 1024 bytes (1KB)
  public static final long DEFAULT_READ_CAPACITY_UNIT = 1024;
  public static final String WRITE_CAPACITY_UNIT_CONF_KEY = "hbase.quota.write.capacity.unit";
  // the default one write capacity unit is 1024 bytes (1KB)
  public static final long DEFAULT_WRITE_CAPACITY_UNIT = 1024;

  /** Table descriptor for Quota internal table */
  public static final HTableDescriptor QUOTA_TABLE_DESC =
    new HTableDescriptor(QUOTA_TABLE_NAME);
  static {
    QUOTA_TABLE_DESC.addFamily(
      new HColumnDescriptor(QUOTA_FAMILY_INFO)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        .setBloomFilterType(BloomType.ROW)
        .setMaxVersions(1)
    );
    QUOTA_TABLE_DESC.addFamily(
      new HColumnDescriptor(QUOTA_FAMILY_USAGE)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        .setBloomFilterType(BloomType.ROW)
        .setMaxVersions(1)
    );
  }

  /** Returns true if the support for quota is enabled */
  public static boolean isQuotaEnabled(final Configuration conf) {
    return conf.getBoolean(QUOTA_CONF_KEY, QUOTA_ENABLED_DEFAULT);
  }

  /* =========================================================================
   *  Quota "settings" helpers
   */
  public static void addTableQuota(final Connection connection, final TableName table,
      final Quotas data) throws IOException {
    addQuotas(connection, getTableRowKey(table), data);
  }

  public static void deleteTableQuota(final Connection connection, final TableName table)
      throws IOException {
    deleteQuotas(connection, getTableRowKey(table));
  }

  public static void addNamespaceQuota(final Connection connection, final String namespace,
      final Quotas data) throws IOException {
    addQuotas(connection, getNamespaceRowKey(namespace), data);
  }

  public static void deleteNamespaceQuota(final Connection connection, final String namespace)
      throws IOException {
    deleteQuotas(connection, getNamespaceRowKey(namespace));
  }

  public static void addUserQuota(final Connection connection, final String user,
      final Quotas data) throws IOException {
    addQuotas(connection, getUserRowKey(user), data);
  }

  public static void addUserQuota(final Connection connection, final String user,
      final TableName table, final Quotas data) throws IOException {
    addQuotas(connection, getUserRowKey(user), getSettingsQualifierForUserTable(table), data);
  }

  public static void addUserQuota(final Connection connection, final String user,
      final String namespace, final Quotas data) throws IOException {
    addQuotas(connection, getUserRowKey(user),
        getSettingsQualifierForUserNamespace(namespace), data);
  }

  public static void deleteUserQuota(final Connection connection, final String user)
      throws IOException {
    deleteQuotas(connection, getUserRowKey(user));
  }

  public static void deleteUserQuota(final Connection connection, final String user,
      final TableName table) throws IOException {
    deleteQuotas(connection, getUserRowKey(user),
        getSettingsQualifierForUserTable(table));
  }

  public static void deleteUserQuota(final Connection connection, final String user,
      final String namespace) throws IOException {
    deleteQuotas(connection, getUserRowKey(user),
        getSettingsQualifierForUserNamespace(namespace));
  }

  public static void addRegionServerQuota(final Connection connection, final String regionServer,
      final Quotas data) throws IOException {
    addQuotas(connection, getRegionServerRowKey(regionServer), data);
  }

  public static void deleteRegionServerQuota(final Connection connection, final String regionServer)
      throws IOException {
    deleteQuotas(connection, getRegionServerRowKey(regionServer));
  }

  protected static void switchExceedThrottleQuota(final Connection connection,
      boolean exceedThrottleQuotaEnabled) throws IOException {
    if (exceedThrottleQuotaEnabled) {
      checkRSQuotaToEnableExceedThrottle(
        getRegionServerQuota(connection, QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY));
    }

    Put put = new Put(getExceedThrottleQuotaRowKey());
    put.addColumn(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS,
      Bytes.toBytes(exceedThrottleQuotaEnabled));
    doPut(connection, put);
  }

  private static void checkRSQuotaToEnableExceedThrottle(Quotas quotas) throws IOException {
    if (quotas != null && quotas.hasThrottle()) {
      Throttle throttle = quotas.getThrottle();
      // If enable exceed throttle quota, make sure that there are at least one read(req/read +
      // num/size/cu) and one write(req/write + num/size/cu) region server throttle quotas.
      boolean hasReadQuota = false;
      boolean hasWriteQuota = false;
      if (throttle.hasReqNum() || throttle.hasReqSize() || throttle.hasReqCapacityUnit()) {
        hasReadQuota = true;
        hasWriteQuota = true;
      }
      if (!hasReadQuota
          && (throttle.hasReadNum() || throttle.hasReadSize() || throttle.hasReadCapacityUnit())) {
        hasReadQuota = true;
      }
      if (!hasReadQuota) {
        throw new DoNotRetryIOException(
            "Please set at least one read region server quota before enable exceed throttle quota");
      }
      if (!hasWriteQuota && (throttle.hasWriteNum() || throttle.hasWriteSize()
          || throttle.hasWriteCapacityUnit())) {
        hasWriteQuota = true;
      }
      if (!hasWriteQuota) {
        throw new DoNotRetryIOException("Please set at least one write region server quota "
            + "before enable exceed throttle quota");
      }
      // If enable exceed throttle quota, make sure that region server throttle quotas are in
      // seconds time unit. Because once previous requests exceed their quota and consume region
      // server quota, quota in other time units may be refilled in a long time, this may affect
      // later requests.
      List<Pair<Boolean, TimedQuota>> list =
          Arrays.asList(Pair.newPair(throttle.hasReqNum(), throttle.getReqNum()),
            Pair.newPair(throttle.hasReadNum(), throttle.getReadNum()),
            Pair.newPair(throttle.hasWriteNum(), throttle.getWriteNum()),
            Pair.newPair(throttle.hasReqSize(), throttle.getReqSize()),
            Pair.newPair(throttle.hasReadSize(), throttle.getReadSize()),
            Pair.newPair(throttle.hasWriteSize(), throttle.getWriteSize()),
            Pair.newPair(throttle.hasReqCapacityUnit(), throttle.getReqCapacityUnit()),
            Pair.newPair(throttle.hasReadCapacityUnit(), throttle.getReadCapacityUnit()),
            Pair.newPair(throttle.hasWriteCapacityUnit(), throttle.getWriteCapacityUnit()));
      for (Pair<Boolean, TimedQuota> pair : list) {
        if (pair.getFirst()) {
          if (pair.getSecond().getTimeUnit() != TimeUnit.SECONDS) {
            throw new DoNotRetryIOException("All region server quota must be "
                + "in seconds time unit if enable exceed throttle quota");
          }
        }
      }
    } else {
      // If enable exceed throttle quota, make sure that region server quota is already set
      throw new DoNotRetryIOException(
          "Please set region server quota before enable exceed throttle quota");
    }
  }

  protected static boolean isExceedThrottleQuotaEnabled(final Connection connection)
      throws IOException {
    Get get = new Get(getExceedThrottleQuotaRowKey());
    get.addColumn(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    Result result = doGet(connection, get);
    if (result.isEmpty()) {
      return false;
    }
    return Bytes.toBoolean(result.getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS));
  }

  private static void addQuotas(final Connection connection, final byte[] rowKey,
      final Quotas data) throws IOException {
    addQuotas(connection, rowKey, QUOTA_QUALIFIER_SETTINGS, data);
  }

  private static void addQuotas(final Connection connection, final byte[] rowKey,
      final byte[] qualifier, final Quotas data) throws IOException {
    Put put = new Put(rowKey);
    put.addColumn(QUOTA_FAMILY_INFO, qualifier, quotasToData(data));
    doPut(connection, put);
  }

  private static void deleteQuotas(final Connection connection, final byte[] rowKey)
      throws IOException {
    deleteQuotas(connection, rowKey, null);
  }

  private static void deleteQuotas(final Connection connection, final byte[] rowKey,
      final byte[] qualifier) throws IOException {
    Delete delete = new Delete(rowKey);
    if (qualifier != null) {
      delete.addColumns(QUOTA_FAMILY_INFO, qualifier);
    }
    if (isNamespaceRowKey(rowKey)) {
      String ns = getNamespaceFromRowKey(rowKey);
      Quotas namespaceQuota = getNamespaceQuota(connection,ns);
      if (namespaceQuota != null && namespaceQuota.hasSpace()) {
        // When deleting namespace space quota, also delete table usage(u:p) snapshots
        deleteTableUsageSnapshotsForNamespace(connection, ns);
      }
    }
    doDelete(connection, delete);
  }

  public static Map<String, UserQuotaState> fetchUserQuotas(final Connection connection,
      final List<Get> gets, Map<TableName, Double> tableMachineQuotaFactors, double factor)
      throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTime();
    Result[] results = doGet(connection, gets);

    Map<String, UserQuotaState> userQuotas = new HashMap<>(results.length);
    for (int i = 0; i < results.length; ++i) {
      byte[] key = gets.get(i).getRow();
      assert isUserRowKey(key);
      String user = getUserFromRowKey(key);

      final UserQuotaState quotaInfo = new UserQuotaState(nowTs);
      userQuotas.put(user, quotaInfo);

      if (results[i].isEmpty()) continue;
      assert Bytes.equals(key, results[i].getRow());

      try {
        parseUserResult(user, results[i], new UserQuotasVisitor() {
          @Override
          public void visitUserQuotas(String userName, String namespace, Quotas quotas) {
            quotas = updateClusterQuotaToMachineQuota(quotas, factor);
            quotaInfo.setQuotas(namespace, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
            quotas = updateClusterQuotaToMachineQuota(quotas,
              tableMachineQuotaFactors.containsKey(table) ? tableMachineQuotaFactors.get(table)
                  : 1);
            quotaInfo.setQuotas(table, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, Quotas quotas) {
            quotas = updateClusterQuotaToMachineQuota(quotas, factor);
            quotaInfo.setQuotas(quotas);
          }
        });
      } catch (IOException e) {
        LOG.error("Unable to parse user '" + user + "' quotas", e);
        userQuotas.remove(user);
      }
    }
    return userQuotas;
  }

  public static Map<TableName, QuotaState> fetchTableQuotas(final Connection connection,
      final List<Get> gets, Map<TableName, Double> tableMachineFactors) throws IOException {
    return fetchGlobalQuotas("table", connection, gets, new KeyFromRow<TableName>() {
      @Override
      public TableName getKeyFromRow(final byte[] row) {
        assert isTableRowKey(row);
        return getTableFromRowKey(row);
      }

      @Override
      public double getFactor(TableName tableName) {
        return tableMachineFactors.containsKey(tableName) ? tableMachineFactors.get(tableName) : 1;
      }
    });
  }

  public static Map<String, QuotaState> fetchNamespaceQuotas(final Connection connection,
      final List<Get> gets, double factor) throws IOException {
    return fetchGlobalQuotas("namespace", connection, gets, new KeyFromRow<String>() {
      @Override
      public String getKeyFromRow(final byte[] row) {
        assert isNamespaceRowKey(row);
        return getNamespaceFromRowKey(row);
      }

      @Override
      public double getFactor(String s) {
        return factor;
      }
    });
  }

  public static Map<String, QuotaState> fetchRegionServerQuotas(final Connection connection,
      final List<Get> gets) throws IOException {
    return fetchGlobalQuotas("regionServer", connection, gets, new KeyFromRow<String>() {
      @Override
      public String getKeyFromRow(final byte[] row) {
        assert isRegionServerRowKey(row);
        return getRegionServerFromRowKey(row);
      }

      @Override
      public double getFactor(String s) {
        return 1;
      }
    });
  }

  public static <K> Map<K, QuotaState> fetchGlobalQuotas(final String type,
      final Connection connection, final List<Get> gets, final KeyFromRow<K> kfr)
  throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTime();
    Result[] results = doGet(connection, gets);

    Map<K, QuotaState> globalQuotas = new HashMap<>(results.length);
    for (int i = 0; i < results.length; ++i) {
      byte[] row = gets.get(i).getRow();
      K key = kfr.getKeyFromRow(row);

      QuotaState quotaInfo = new QuotaState(nowTs);
      globalQuotas.put(key, quotaInfo);

      if (results[i].isEmpty()) continue;
      assert Bytes.equals(row, results[i].getRow());

      byte[] data = results[i].getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
      if (data == null) continue;

      try {
        Quotas quotas = quotasFromData(data);
        quotas = updateClusterQuotaToMachineQuota(quotas,
          kfr.getFactor(key));
        quotaInfo.setQuotas(quotas);
      } catch (IOException e) {
        LOG.error("Unable to parse " + type + " '" + key + "' quotas", e);
        globalQuotas.remove(key);
      }
    }
    return globalQuotas;
  }

  /**
   * Convert cluster scope quota to machine scope quota
   * @param quotas the original quota
   * @param factor factor used to divide cluster limiter to machine limiter
   * @return the converted quota whose quota limiters all in machine scope
   */
  private static Quotas updateClusterQuotaToMachineQuota(Quotas quotas, double factor) {
    Quotas.Builder newQuotas = Quotas.newBuilder(quotas);
    if (newQuotas.hasThrottle()) {
      Throttle.Builder throttle = Throttle.newBuilder(newQuotas.getThrottle());
      if (throttle.hasReqNum()) {
        throttle.setReqNum(updateTimedQuota(throttle.getReqNum(), factor));
      }
      if (throttle.hasReqSize()) {
        throttle.setReqSize(updateTimedQuota(throttle.getReqSize(), factor));
      }
      if (throttle.hasReadNum()) {
        throttle.setReadNum(updateTimedQuota(throttle.getReadNum(), factor));
      }
      if (throttle.hasReadSize()) {
        throttle.setReadSize(updateTimedQuota(throttle.getReadSize(), factor));
      }
      if (throttle.hasWriteNum()) {
        throttle.setWriteNum(updateTimedQuota(throttle.getWriteNum(), factor));
      }
      if (throttle.hasWriteSize()) {
        throttle.setWriteSize(updateTimedQuota(throttle.getWriteSize(), factor));
      }
      if (throttle.hasReqCapacityUnit()) {
        throttle.setReqCapacityUnit(updateTimedQuota(throttle.getReqCapacityUnit(), factor));
      }
      if (throttle.hasReadCapacityUnit()) {
        throttle.setReadCapacityUnit(updateTimedQuota(throttle.getReadCapacityUnit(), factor));
      }
      if (throttle.hasWriteCapacityUnit()) {
        throttle.setWriteCapacityUnit(updateTimedQuota(throttle.getWriteCapacityUnit(), factor));
      }
      newQuotas.setThrottle(throttle.build());
    }
    return newQuotas.build();
  }

  private static TimedQuota updateTimedQuota(TimedQuota timedQuota, double factor) {
    if (timedQuota.getScope() == QuotaScope.CLUSTER) {
      TimedQuota.Builder newTimedQuota = TimedQuota.newBuilder(timedQuota);
      newTimedQuota.setSoftLimit(Math.max(1, (long) (timedQuota.getSoftLimit() * factor)))
          .setScope(QuotaScope.MACHINE);
      return newTimedQuota.build();
    } else {
      return timedQuota;
    }
  }

  private static interface KeyFromRow<T> {
    T getKeyFromRow(final byte[] row);
    double getFactor(T t);
  }

  /* =========================================================================
   *  HTable helpers
   */
  private static void doPut(final Connection connection, final Put put)
  throws IOException {
    try (Table table = connection.getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
      table.put(put);
    }
  }

  private static void doDelete(final Connection connection, final Delete delete)
  throws IOException {
    try (Table table = connection.getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
      table.delete(delete);
    }
  }

  /* =========================================================================
   *  Data Size Helpers
   */
  public static long calculateMutationSize(final Mutation mutation) {
    long size = 0;
    for (Map.Entry<byte[], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        size += cell.getSerializedSize();
      }
    }
    return size;
  }

  public static long calculateResultSize(final Result result) {
    long size = 0;
    for (Cell cell : result.rawCells()) {
      size += cell.getSerializedSize();
    }
    return size;
  }

  public static long calculateResultSize(final List<Result> results) {
    long size = 0;
    for (Result result: results) {
      for (Cell cell : result.rawCells()) {
        size += cell.getSerializedSize();
      }
    }
    return size;
  }

  /**
   * Method to enable a table, if not already enabled. This method suppresses
   * {@link TableNotDisabledException} and {@link TableNotFoundException}, if thrown while enabling
   * the table.
   * @param conn connection to re-use
   * @param tableName name of the table to be enabled
   */
  public static void enableTableIfNotEnabled(Connection conn, TableName tableName)
      throws IOException {
    try {
      conn.getAdmin().enableTable(tableName);
    } catch (TableNotDisabledException | TableNotFoundException e) {
      // ignore
    }
  }

  /**
   * Method to disable a table, if not already disabled. This method suppresses
   * {@link TableNotEnabledException}, if thrown while disabling the table.
   * @param conn connection to re-use
   * @param tableName table name which has moved into space quota violation
   */
  public static void disableTableIfNotDisabled(Connection conn, TableName tableName)
      throws IOException {
    try {
      conn.getAdmin().disableTable(tableName);
    } catch (TableNotEnabledException | TableNotFoundException e) {
      // ignore
    }
  }
}
