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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Helper class to interact with the quota table
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaUtil extends QuotaTableUtil {
  private static final Log LOG = LogFactory.getLog(QuotaUtil.class);

  public static final String QUOTA_CONF_KEY = "hbase.quota.enabled";
  private static final boolean QUOTA_ENABLED_DEFAULT = false;

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
      delete.deleteColumns(QUOTA_FAMILY_INFO, qualifier);
    }
    doDelete(connection, delete);
  }

  public static Map<String, UserQuotaState> fetchUserQuotas(final Connection connection,
      final List<Get> gets) throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTime();
    Result[] results = doGet(connection, gets);

    Map<String, UserQuotaState> userQuotas = new HashMap<String, UserQuotaState>(results.length);
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
            quotaInfo.setQuotas(namespace, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
            quotaInfo.setQuotas(table, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, Quotas quotas) {
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
      final List<Get> gets) throws IOException {
    return fetchGlobalQuotas("table", connection, gets, new KeyFromRow<TableName>() {
      @Override
      public TableName getKeyFromRow(final byte[] row) {
        assert isTableRowKey(row);
        return getTableFromRowKey(row);
      }
    });
  }

  public static Map<String, QuotaState> fetchNamespaceQuotas(final Connection connection,
      final List<Get> gets) throws IOException {
    return fetchGlobalQuotas("namespace", connection, gets, new KeyFromRow<String>() {
      @Override
      public String getKeyFromRow(final byte[] row) {
        assert isNamespaceRowKey(row);
        return getNamespaceFromRowKey(row);
      }
    });
  }

  public static <K> Map<K, QuotaState> fetchGlobalQuotas(final String type,
      final Connection connection, final List<Get> gets, final KeyFromRow<K> kfr)
  throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTime();
    Result[] results = doGet(connection, gets);

    Map<K, QuotaState> globalQuotas = new HashMap<K, QuotaState>(results.length);
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
        quotaInfo.setQuotas(quotas);
      } catch (IOException e) {
        LOG.error("Unable to parse " + type + " '" + key + "' quotas", e);
        globalQuotas.remove(key);
      }
    }
    return globalQuotas;
  }

  private static interface KeyFromRow<T> {
    T getKeyFromRow(final byte[] row);
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
    for (Map.Entry<byte [], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        size += KeyValueUtil.length(cell);
      }
    }
    return size;
  }

  public static long calculateResultSize(final Result result) {
    long size = 0;
    for (Cell cell : result.rawCells()) {
      size += KeyValueUtil.length(cell);
    }
    return size;
  }

  public static long calculateResultSize(final List<Result> results) {
    long size = 0;
    for (Result result: results) {
      for (Cell cell : result.rawCells()) {
        size += KeyValueUtil.length(cell);
      }
    }
    return size;
  }
}
