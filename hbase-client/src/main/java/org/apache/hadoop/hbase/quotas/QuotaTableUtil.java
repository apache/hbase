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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

/**
 * Helper class to interact with the quota table.
 * 
 * <pre>
 *     ROW-KEY      FAM/QUAL        DATA
 *   n.<namespace> q:s         <global-quotas>
 *   t.<table>     q:s         <global-quotas>
 *   u.<user>      q:s         <global-quotas>
 *   u.<user>      q:s.<table> <table-quotas>
 *   u.<user>      q:s.<ns>:   <namespace-quotas>
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaTableUtil {
  private static final Log LOG = LogFactory.getLog(QuotaTableUtil.class);

  /** System table for quotas */
  public static final TableName QUOTA_TABLE_NAME = TableName.valueOf(
    NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "quota");

  protected static final byte[] QUOTA_FAMILY_INFO = Bytes.toBytes("q");
  protected static final byte[] QUOTA_FAMILY_USAGE = Bytes.toBytes("u");
  protected static final byte[] QUOTA_QUALIFIER_SETTINGS = Bytes.toBytes("s");
  protected static final byte[] QUOTA_QUALIFIER_SETTINGS_PREFIX = Bytes.toBytes("s.");
  protected static final byte[] QUOTA_USER_ROW_KEY_PREFIX = Bytes.toBytes("u.");
  protected static final byte[] QUOTA_TABLE_ROW_KEY_PREFIX = Bytes.toBytes("t.");
  protected static final byte[] QUOTA_NAMESPACE_ROW_KEY_PREFIX = Bytes.toBytes("n.");

  /*
   * ========================================================================= Quota "settings"
   * helpers
   */
  public static Quotas getTableQuota(final Connection connection, final TableName table)
      throws IOException {
    return getQuotas(connection, getTableRowKey(table));
  }

  public static Quotas getNamespaceQuota(final Connection connection, final String namespace)
      throws IOException {
    return getQuotas(connection, getNamespaceRowKey(namespace));
  }

  public static Quotas getUserQuota(final Connection connection, final String user)
      throws IOException {
    return getQuotas(connection, getUserRowKey(user));
  }

  public static Quotas getUserQuota(final Connection connection, final String user,
      final TableName table) throws IOException {
    return getQuotas(connection, getUserRowKey(user), getSettingsQualifierForUserTable(table));
  }

  public static Quotas getUserQuota(final Connection connection, final String user,
      final String namespace) throws IOException {
    return getQuotas(connection, getUserRowKey(user),
      getSettingsQualifierForUserNamespace(namespace));
  }

  private static Quotas getQuotas(final Connection connection, final byte[] rowKey)
      throws IOException {
    return getQuotas(connection, rowKey, QUOTA_QUALIFIER_SETTINGS);
  }

  private static Quotas getQuotas(final Connection connection, final byte[] rowKey,
      final byte[] qualifier) throws IOException {
    Get get = new Get(rowKey);
    get.addColumn(QUOTA_FAMILY_INFO, qualifier);
    Result result = doGet(connection, get);
    if (result.isEmpty()) {
      return null;
    }
    return quotasFromData(result.getValue(QUOTA_FAMILY_INFO, qualifier));
  }

  public static Get makeGetForTableQuotas(final TableName table) {
    Get get = new Get(getTableRowKey(table));
    get.addFamily(QUOTA_FAMILY_INFO);
    return get;
  }

  public static Get makeGetForNamespaceQuotas(final String namespace) {
    Get get = new Get(getNamespaceRowKey(namespace));
    get.addFamily(QUOTA_FAMILY_INFO);
    return get;
  }

  public static Get makeGetForUserQuotas(final String user, final Iterable<TableName> tables,
      final Iterable<String> namespaces) {
    Get get = new Get(getUserRowKey(user));
    get.addColumn(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    for (final TableName table : tables) {
      get.addColumn(QUOTA_FAMILY_INFO, getSettingsQualifierForUserTable(table));
    }
    for (final String ns : namespaces) {
      get.addColumn(QUOTA_FAMILY_INFO, getSettingsQualifierForUserNamespace(ns));
    }
    return get;
  }

  public static Scan makeScan(final QuotaFilter filter) {
    Scan scan = new Scan();
    scan.addFamily(QUOTA_FAMILY_INFO);
    if (filter != null && !filter.isNull()) {
      scan.setFilter(makeFilter(filter));
    }
    return scan;
  }

  /**
   * converts quotafilter to serializeable filterlists.
   */
  public static Filter makeFilter(final QuotaFilter filter) {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    if (!Strings.isEmpty(filter.getUserFilter())) {
      FilterList userFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
      boolean hasFilter = false;

      if (!Strings.isEmpty(filter.getNamespaceFilter())) {
        FilterList nsFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        nsFilters.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(
            getUserRowKeyRegex(filter.getUserFilter()), 0)));
        nsFilters.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator(getSettingsQualifierRegexForUserNamespace(filter
                .getNamespaceFilter()), 0)));
        userFilters.addFilter(nsFilters);
        hasFilter = true;
      }
      if (!Strings.isEmpty(filter.getTableFilter())) {
        FilterList tableFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        tableFilters.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator(getUserRowKeyRegex(filter.getUserFilter()), 0)));
        tableFilters.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator(
                getSettingsQualifierRegexForUserTable(filter.getTableFilter()), 0)));
        userFilters.addFilter(tableFilters);
        hasFilter = true;
      }
      if (!hasFilter) {
        userFilters.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator(getUserRowKeyRegex(filter.getUserFilter()), 0)));
      }

      filterList.addFilter(userFilters);
    } else if (!Strings.isEmpty(filter.getTableFilter())) {
      filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(
          getTableRowKeyRegex(filter.getTableFilter()), 0)));
    } else if (!Strings.isEmpty(filter.getNamespaceFilter())) {
      filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(
          getNamespaceRowKeyRegex(filter.getNamespaceFilter()), 0)));
    }
    return filterList;
  }

  public static interface UserQuotasVisitor {
    void visitUserQuotas(final String userName, final Quotas quotas) throws IOException;

    void visitUserQuotas(final String userName, final TableName table, final Quotas quotas)
        throws IOException;

    void visitUserQuotas(final String userName, final String namespace, final Quotas quotas)
        throws IOException;
  }

  public static interface TableQuotasVisitor {
    void visitTableQuotas(final TableName tableName, final Quotas quotas) throws IOException;
  }

  public static interface NamespaceQuotasVisitor {
    void visitNamespaceQuotas(final String namespace, final Quotas quotas) throws IOException;
  }

  public static interface QuotasVisitor extends UserQuotasVisitor, TableQuotasVisitor,
      NamespaceQuotasVisitor {
  }

  public static void parseResult(final Result result, final QuotasVisitor visitor)
      throws IOException {
    byte[] row = result.getRow();
    if (isNamespaceRowKey(row)) {
      parseNamespaceResult(result, visitor);
    } else if (isTableRowKey(row)) {
      parseTableResult(result, visitor);
    } else if (isUserRowKey(row)) {
      parseUserResult(result, visitor);
    } else {
      LOG.warn("unexpected row-key: " + Bytes.toString(row));
    }
  }

  public static void
      parseNamespaceResult(final Result result, final NamespaceQuotasVisitor visitor)
          throws IOException {
    String namespace = getNamespaceFromRowKey(result.getRow());
    parseNamespaceResult(namespace, result, visitor);
  }

  protected static void parseNamespaceResult(final String namespace, final Result result,
      final NamespaceQuotasVisitor visitor) throws IOException {
    byte[] data = result.getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    if (data != null) {
      Quotas quotas = quotasFromData(data);
      visitor.visitNamespaceQuotas(namespace, quotas);
    }
  }

  public static void parseTableResult(final Result result, final TableQuotasVisitor visitor)
      throws IOException {
    TableName table = getTableFromRowKey(result.getRow());
    parseTableResult(table, result, visitor);
  }

  protected static void parseTableResult(final TableName table, final Result result,
      final TableQuotasVisitor visitor) throws IOException {
    byte[] data = result.getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    if (data != null) {
      Quotas quotas = quotasFromData(data);
      visitor.visitTableQuotas(table, quotas);
    }
  }

  public static void parseUserResult(final Result result, final UserQuotasVisitor visitor)
      throws IOException {
    String userName = getUserFromRowKey(result.getRow());
    parseUserResult(userName, result, visitor);
  }

  protected static void parseUserResult(final String userName, final Result result,
      final UserQuotasVisitor visitor) throws IOException {
    Map<byte[], byte[]> familyMap = result.getFamilyMap(QUOTA_FAMILY_INFO);
    if (familyMap == null || familyMap.isEmpty()) return;

    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
      Quotas quotas = quotasFromData(entry.getValue());
      if (Bytes.startsWith(entry.getKey(), QUOTA_QUALIFIER_SETTINGS_PREFIX)) {
        String name = Bytes.toString(entry.getKey(), QUOTA_QUALIFIER_SETTINGS_PREFIX.length);
        if (name.charAt(name.length() - 1) == TableName.NAMESPACE_DELIM) {
          String namespace = name.substring(0, name.length() - 1);
          visitor.visitUserQuotas(userName, namespace, quotas);
        } else {
          TableName table = TableName.valueOf(name);
          visitor.visitUserQuotas(userName, table, quotas);
        }
      } else if (Bytes.equals(entry.getKey(), QUOTA_QUALIFIER_SETTINGS)) {
        visitor.visitUserQuotas(userName, quotas);
      }
    }
  }

  /*
   * ========================================================================= Quotas protobuf
   * helpers
   */
  protected static Quotas quotasFromData(final byte[] data) throws IOException {
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    if (!ProtobufUtil.isPBMagicPrefix(data, 0, magicLen)) {
      throw new IOException("Missing pb magic prefix");
    }
    return Quotas.parseFrom(new ByteArrayInputStream(data, magicLen, data.length - magicLen));
  }

  protected static byte[] quotasToData(final Quotas data) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(ProtobufUtil.PB_MAGIC);
    data.writeTo(stream);
    return stream.toByteArray();
  }

  public static boolean isEmptyQuota(final Quotas quotas) {
    boolean hasSettings = false;
    hasSettings |= quotas.hasThrottle();
    hasSettings |= quotas.hasBypassGlobals();
    return !hasSettings;
  }

  /*
   * ========================================================================= HTable helpers
   */
  protected static Result doGet(final Connection connection, final Get get) throws IOException {
    try (Table table = connection.getTable(QUOTA_TABLE_NAME)) {
      return table.get(get);
    }
  }

  protected static Result[] doGet(final Connection connection, final List<Get> gets)
      throws IOException {
    try (Table table = connection.getTable(QUOTA_TABLE_NAME)) {
      return table.get(gets);
    }
  }

  /*
   * ========================================================================= Quota table row key
   * helpers
   */
  protected static byte[] getUserRowKey(final String user) {
    return Bytes.add(QUOTA_USER_ROW_KEY_PREFIX, Bytes.toBytes(user));
  }

  protected static byte[] getTableRowKey(final TableName table) {
    return Bytes.add(QUOTA_TABLE_ROW_KEY_PREFIX, table.getName());
  }

  protected static byte[] getNamespaceRowKey(final String namespace) {
    return Bytes.add(QUOTA_NAMESPACE_ROW_KEY_PREFIX, Bytes.toBytes(namespace));
  }

  protected static byte[] getSettingsQualifierForUserTable(final TableName tableName) {
    return Bytes.add(QUOTA_QUALIFIER_SETTINGS_PREFIX, tableName.getName());
  }

  protected static byte[] getSettingsQualifierForUserNamespace(final String namespace) {
    return Bytes.add(QUOTA_QUALIFIER_SETTINGS_PREFIX,
      Bytes.toBytes(namespace + TableName.NAMESPACE_DELIM));
  }

  protected static String getUserRowKeyRegex(final String user) {
    return getRowKeyRegEx(QUOTA_USER_ROW_KEY_PREFIX, user);
  }

  protected static String getTableRowKeyRegex(final String table) {
    return getRowKeyRegEx(QUOTA_TABLE_ROW_KEY_PREFIX, table);
  }

  protected static String getNamespaceRowKeyRegex(final String namespace) {
    return getRowKeyRegEx(QUOTA_NAMESPACE_ROW_KEY_PREFIX, namespace);
  }

  private static String getRowKeyRegEx(final byte[] prefix, final String regex) {
    return '^' + Pattern.quote(Bytes.toString(prefix)) + regex + '$';
  }

  protected static String getSettingsQualifierRegexForUserTable(final String table) {
    return '^' + Pattern.quote(Bytes.toString(QUOTA_QUALIFIER_SETTINGS_PREFIX)) + table + "(?<!"
        + Pattern.quote(Character.toString(TableName.NAMESPACE_DELIM)) + ")$";
  }

  protected static String getSettingsQualifierRegexForUserNamespace(final String namespace) {
    return '^' + Pattern.quote(Bytes.toString(QUOTA_QUALIFIER_SETTINGS_PREFIX)) + namespace
        + Pattern.quote(Character.toString(TableName.NAMESPACE_DELIM)) + '$';
  }

  protected static boolean isNamespaceRowKey(final byte[] key) {
    return Bytes.startsWith(key, QUOTA_NAMESPACE_ROW_KEY_PREFIX);
  }

  protected static String getNamespaceFromRowKey(final byte[] key) {
    return Bytes.toString(key, QUOTA_NAMESPACE_ROW_KEY_PREFIX.length);
  }

  protected static boolean isTableRowKey(final byte[] key) {
    return Bytes.startsWith(key, QUOTA_TABLE_ROW_KEY_PREFIX);
  }

  protected static TableName getTableFromRowKey(final byte[] key) {
    return TableName.valueOf(Bytes.toString(key, QUOTA_TABLE_ROW_KEY_PREFIX.length));
  }

  protected static boolean isUserRowKey(final byte[] key) {
    return Bytes.startsWith(key, QUOTA_USER_ROW_KEY_PREFIX);
  }

  protected static String getUserFromRowKey(final byte[] key) {
    return Bytes.toString(key, QUOTA_USER_ROW_KEY_PREFIX.length);
  }
}
