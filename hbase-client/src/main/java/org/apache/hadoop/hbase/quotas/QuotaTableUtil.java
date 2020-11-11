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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * Helper class to interact with the quota table.
 * <table>
 *   <tr><th>ROW-KEY</th><th>FAM/QUAL</th><th>DATA</th><th>DESC</th></tr>
 *   <tr><td>n.&lt;namespace&gt;</td><td>q:s</td><td>&lt;global-quotas&gt;</td></tr>
 *   <tr><td>n.&lt;namespace&gt;</td><td>u:p</td><td>&lt;namespace-quota policy&gt;</td></tr>
 *   <tr><td>n.&lt;namespace&gt;</td><td>u:s</td><td>&lt;SpaceQuotaSnapshot&gt;</td>
 *      <td>The size of all snapshots against tables in the namespace</td></tr>
 *   <tr><td>t.&lt;table&gt;</td><td>q:s</td><td>&lt;global-quotas&gt;</td></tr>
 *   <tr><td>t.&lt;table&gt;</td><td>u:p</td><td>&lt;table-quota policy&gt;</td></tr>
 *   <tr><td>t.&lt;table&gt;</td><td>u:ss.&lt;snapshot name&gt;</td>
 *      <td>&lt;SpaceQuotaSnapshot&gt;</td><td>The size of a snapshot against a table</td></tr>
 *   <tr><td>u.&lt;user&gt;</td><td>q:s</td><td>&lt;global-quotas&gt;</td></tr>
 *   <tr><td>u.&lt;user&gt;</td><td>q:s.&lt;table&gt;</td><td>&lt;table-quotas&gt;</td></tr>
 *   <tr><td>u.&lt;user&gt;</td><td>q:s.&lt;ns&gt;</td><td>&lt;namespace-quotas&gt;</td></tr>
 * </table>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaTableUtil {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaTableUtil.class);

  /** System table for quotas */
  public static final TableName QUOTA_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "quota");

  protected static final byte[] QUOTA_FAMILY_INFO = Bytes.toBytes("q");
  protected static final byte[] QUOTA_FAMILY_USAGE = Bytes.toBytes("u");
  protected static final byte[] QUOTA_QUALIFIER_SETTINGS = Bytes.toBytes("s");
  protected static final byte[] QUOTA_QUALIFIER_SETTINGS_PREFIX = Bytes.toBytes("s.");
  protected static final byte[] QUOTA_QUALIFIER_POLICY = Bytes.toBytes("p");
  protected static final byte[] QUOTA_SNAPSHOT_SIZE_QUALIFIER = Bytes.toBytes("ss");
  protected static final String QUOTA_POLICY_COLUMN =
      Bytes.toString(QUOTA_FAMILY_USAGE) + ":" + Bytes.toString(QUOTA_QUALIFIER_POLICY);
  protected static final byte[] QUOTA_USER_ROW_KEY_PREFIX = Bytes.toBytes("u.");
  protected static final byte[] QUOTA_TABLE_ROW_KEY_PREFIX = Bytes.toBytes("t.");
  protected static final byte[] QUOTA_NAMESPACE_ROW_KEY_PREFIX = Bytes.toBytes("n.");
  protected static final byte[] QUOTA_REGION_SERVER_ROW_KEY_PREFIX = Bytes.toBytes("r.");
  private static final byte[] QUOTA_EXCEED_THROTTLE_QUOTA_ROW_KEY =
      Bytes.toBytes("exceedThrottleQuota");

  /*
   * TODO: Setting specified region server quota isn't supported currently and the row key "r.all"
   * represents the throttle quota of all region servers
   */
  public static final String QUOTA_REGION_SERVER_ROW_KEY = "all";

  /* =========================================================================
   *  Quota "settings" helpers
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

  public static Quotas getRegionServerQuota(final Connection connection, final String regionServer)
      throws IOException {
    return getQuotas(connection, getRegionServerRowKey(regionServer));
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

  public static Get makeGetForRegionServerQuotas(final String regionServer) {
    Get get = new Get(getRegionServerRowKey(regionServer));
    get.addFamily(QUOTA_FAMILY_INFO);
    return get;
  }

  public static Get makeGetForUserQuotas(final String user, final Iterable<TableName> tables,
      final Iterable<String> namespaces) {
    Get get = new Get(getUserRowKey(user));
    get.addColumn(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    for (final TableName table: tables) {
      get.addColumn(QUOTA_FAMILY_INFO, getSettingsQualifierForUserTable(table));
    }
    for (final String ns: namespaces) {
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
    if (StringUtils.isNotEmpty(filter.getUserFilter())) {
      FilterList userFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
      boolean hasFilter = false;

      if (StringUtils.isNotEmpty(filter.getNamespaceFilter())) {
        FilterList nsFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        nsFilters.addFilter(new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(getUserRowKeyRegex(filter.getUserFilter()), 0)));
        nsFilters.addFilter(new QualifierFilter(CompareOperator.EQUAL,
            new RegexStringComparator(
              getSettingsQualifierRegexForUserNamespace(filter.getNamespaceFilter()), 0)));
        userFilters.addFilter(nsFilters);
        hasFilter = true;
      }
      if (StringUtils.isNotEmpty(filter.getTableFilter())) {
        FilterList tableFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        tableFilters.addFilter(new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(getUserRowKeyRegex(filter.getUserFilter()), 0)));
        tableFilters.addFilter(new QualifierFilter(CompareOperator.EQUAL,
            new RegexStringComparator(
              getSettingsQualifierRegexForUserTable(filter.getTableFilter()), 0)));
        userFilters.addFilter(tableFilters);
        hasFilter = true;
      }
      if (!hasFilter) {
        userFilters.addFilter(new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(getUserRowKeyRegex(filter.getUserFilter()), 0)));
      }

      filterList.addFilter(userFilters);
    } else if (StringUtils.isNotEmpty(filter.getTableFilter())) {
      filterList.addFilter(new RowFilter(CompareOperator.EQUAL,
          new RegexStringComparator(getTableRowKeyRegex(filter.getTableFilter()), 0)));
    } else if (StringUtils.isNotEmpty(filter.getNamespaceFilter())) {
      filterList.addFilter(new RowFilter(CompareOperator.EQUAL,
          new RegexStringComparator(getNamespaceRowKeyRegex(filter.getNamespaceFilter()), 0)));
    } else if (StringUtils.isNotEmpty(filter.getRegionServerFilter())) {
      filterList.addFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(
          getRegionServerRowKeyRegex(filter.getRegionServerFilter()), 0)));
    }
    return filterList;
  }

  /**
   * Creates a {@link Scan} which returns only quota snapshots from the quota table.
   */
  public static Scan makeQuotaSnapshotScan() {
    return makeQuotaSnapshotScanForTable(null);
  }

  /**
   * Fetches all {@link SpaceQuotaSnapshot} objects from the {@code hbase:quota} table.
   *
   * @param conn The HBase connection
   * @return A map of table names and their computed snapshot.
   */
  public static Map<TableName,SpaceQuotaSnapshot> getSnapshots(Connection conn) throws IOException {
    Map<TableName,SpaceQuotaSnapshot> snapshots = new HashMap<>();
    try (Table quotaTable = conn.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(makeQuotaSnapshotScan())) {
      for (Result r : rs) {
        extractQuotaSnapshot(r, snapshots);
      }
    }
    return snapshots;
  }

  /**
   * Creates a {@link Scan} which returns only {@link SpaceQuotaSnapshot} from the quota table for a
   * specific table.
   * @param tn Optionally, a table name to limit the scan's rowkey space. Can be null.
   */
  public static Scan makeQuotaSnapshotScanForTable(TableName tn) {
    Scan s = new Scan();
    // Limit to "u:v" column
    s.addColumn(QUOTA_FAMILY_USAGE, QUOTA_QUALIFIER_POLICY);
    if (null == tn) {
      s.setRowPrefixFilter(QUOTA_TABLE_ROW_KEY_PREFIX);
    } else {
      byte[] row = getTableRowKey(tn);
      // Limit rowspace to the "t:" prefix
      s.withStartRow(row, true).withStopRow(row, true);
    }
    return s;
  }

  /**
   * Creates a {@link Get} which returns only {@link SpaceQuotaSnapshot} from the quota table for a
   * specific table.
   * @param tn table name to get from. Can't be null.
   */
  public static Get makeQuotaSnapshotGetForTable(TableName tn) {
    Get g = new Get(getTableRowKey(tn));
    // Limit to "u:v" column
    g.addColumn(QUOTA_FAMILY_USAGE, QUOTA_QUALIFIER_POLICY);
    return g;
  }

  /**
   * Extracts the {@link SpaceViolationPolicy} and {@link TableName} from the provided
   * {@link Result} and adds them to the given {@link Map}. If the result does not contain
   * the expected information or the serialized policy in the value is invalid, this method
   * will throw an {@link IllegalArgumentException}.
   *
   * @param result A row from the quota table.
   * @param snapshots A map of snapshots to add the result of this method into.
   */
  public static void extractQuotaSnapshot(
      Result result, Map<TableName,SpaceQuotaSnapshot> snapshots) {
    byte[] row = Objects.requireNonNull(result).getRow();
    if (row == null || row.length == 0) {
      throw new IllegalArgumentException("Provided result had a null row");
    }
    final TableName targetTableName = getTableFromRowKey(row);
    Cell c = result.getColumnLatestCell(QUOTA_FAMILY_USAGE, QUOTA_QUALIFIER_POLICY);
    if (c == null) {
      throw new IllegalArgumentException("Result did not contain the expected column "
          + QUOTA_POLICY_COLUMN + ", " + result.toString());
    }
    ByteString buffer = UnsafeByteOperations.unsafeWrap(
        c.getValueArray(), c.getValueOffset(), c.getValueLength());
    try {
      QuotaProtos.SpaceQuotaSnapshot snapshot = QuotaProtos.SpaceQuotaSnapshot.parseFrom(buffer);
      snapshots.put(targetTableName, SpaceQuotaSnapshot.toSpaceQuotaSnapshot(snapshot));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Result did not contain a valid SpaceQuota protocol buffer message", e);
    }
  }

  public static interface UserQuotasVisitor {
    void visitUserQuotas(final String userName, final Quotas quotas)
      throws IOException;
    void visitUserQuotas(final String userName, final TableName table, final Quotas quotas)
      throws IOException;
    void visitUserQuotas(final String userName, final String namespace, final Quotas quotas)
      throws IOException;
  }

  public static interface TableQuotasVisitor {
    void visitTableQuotas(final TableName tableName, final Quotas quotas)
      throws IOException;
  }

  public static interface NamespaceQuotasVisitor {
    void visitNamespaceQuotas(final String namespace, final Quotas quotas)
      throws IOException;
  }

  private static interface RegionServerQuotasVisitor {
    void visitRegionServerQuotas(final String regionServer, final Quotas quotas)
      throws IOException;
  }

  public static interface QuotasVisitor extends UserQuotasVisitor, TableQuotasVisitor,
      NamespaceQuotasVisitor, RegionServerQuotasVisitor {
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
    } else if (isRegionServerRowKey(row)) {
      parseRegionServerResult(result, visitor);
    } else if (isExceedThrottleQuotaRowKey(row)) {
      // skip exceed throttle quota row key
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip exceedThrottleQuota row-key when parse quota result");
      }
    } else {
      LOG.warn("unexpected row-key: " + Bytes.toString(row));
    }
  }

  public static void parseResultToCollection(final Result result,
      Collection<QuotaSettings> quotaSettings) throws IOException {

    QuotaTableUtil.parseResult(result, new QuotaTableUtil.QuotasVisitor() {
      @Override
      public void visitUserQuotas(String userName, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromUserQuotas(userName, quotas));
      }

      @Override
      public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromUserQuotas(userName, table, quotas));
      }

      @Override
      public void visitUserQuotas(String userName, String namespace, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromUserQuotas(userName, namespace, quotas));
      }

      @Override
      public void visitTableQuotas(TableName tableName, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromTableQuotas(tableName, quotas));
      }

      @Override
      public void visitNamespaceQuotas(String namespace, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromNamespaceQuotas(namespace, quotas));
      }

      @Override
      public void visitRegionServerQuotas(String regionServer, Quotas quotas) {
        quotaSettings.addAll(QuotaSettingsFactory.fromRegionServerQuotas(regionServer, quotas));
      }
    });
  }

  public static void parseNamespaceResult(final Result result,
      final NamespaceQuotasVisitor visitor) throws IOException {
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

  private static void parseRegionServerResult(final Result result,
      final RegionServerQuotasVisitor visitor) throws IOException {
    String rs = getRegionServerFromRowKey(result.getRow());
    parseRegionServerResult(rs, result, visitor);
  }

  private static void parseRegionServerResult(final String regionServer, final Result result,
      final RegionServerQuotasVisitor visitor) throws IOException {
    byte[] data = result.getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
    if (data != null) {
      Quotas quotas = quotasFromData(data);
      visitor.visitRegionServerQuotas(regionServer, quotas);
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

    for (Map.Entry<byte[], byte[]> entry: familyMap.entrySet()) {
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

  /**
   * Creates a {@link Put} to store the given {@code snapshot} for the given {@code tableName} in
   * the quota table.
   */
  static Put createPutForSpaceSnapshot(TableName tableName, SpaceQuotaSnapshot snapshot) {
    Put p = new Put(getTableRowKey(tableName));
    p.addColumn(
        QUOTA_FAMILY_USAGE, QUOTA_QUALIFIER_POLICY,
        SpaceQuotaSnapshot.toProtoSnapshot(snapshot).toByteArray());
    return p;
  }

  /**
   * Creates a {@link Get} for the HBase snapshot's size against the given table.
   */
  static Get makeGetForSnapshotSize(TableName tn, String snapshot) {
    Get g = new Get(Bytes.add(QUOTA_TABLE_ROW_KEY_PREFIX, Bytes.toBytes(tn.toString())));
    g.addColumn(
        QUOTA_FAMILY_USAGE,
        Bytes.add(QUOTA_SNAPSHOT_SIZE_QUALIFIER, Bytes.toBytes(snapshot)));
    return g;
  }

  /**
   * Creates a {@link Put} to persist the current size of the {@code snapshot} with respect to
   * the given {@code table}.
   */
  static Put createPutForSnapshotSize(TableName tableName, String snapshot, long size) {
    // We just need a pb message with some `long usage`, so we can just reuse the
    // SpaceQuotaSnapshot message instead of creating a new one.
    Put p = new Put(getTableRowKey(tableName));
    p.addColumn(QUOTA_FAMILY_USAGE, getSnapshotSizeQualifier(snapshot),
        org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuotaSnapshot
            .newBuilder().setQuotaUsage(size).build().toByteArray());
    return p;
  }

  /**
   * Creates a {@code Put} for the namespace's total snapshot size.
   */
  static Put createPutForNamespaceSnapshotSize(String namespace, long size) {
    Put p = new Put(getNamespaceRowKey(namespace));
    p.addColumn(QUOTA_FAMILY_USAGE, QUOTA_SNAPSHOT_SIZE_QUALIFIER,
        org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuotaSnapshot
            .newBuilder().setQuotaUsage(size).build().toByteArray());
    return p;
  }

  /**
   * Returns a list of {@code Delete} to remove given table snapshot
   * entries to remove from quota table
   * @param snapshotEntriesToRemove the entries to remove
   */
  static List<Delete> createDeletesForExistingTableSnapshotSizes(
      Multimap<TableName, String> snapshotEntriesToRemove) {
    List<Delete> deletes = new ArrayList<>();
    for (Map.Entry<TableName, Collection<String>> entry : snapshotEntriesToRemove.asMap()
        .entrySet()) {
      for (String snapshot : entry.getValue()) {
        Delete d = new Delete(getTableRowKey(entry.getKey()));
        d.addColumns(QUOTA_FAMILY_USAGE,
            Bytes.add(QUOTA_SNAPSHOT_SIZE_QUALIFIER, Bytes.toBytes(snapshot)));
        deletes.add(d);
      }
    }
    return deletes;
  }

  /**
   * Returns a list of {@code Delete} to remove all table snapshot entries from quota table.
   * @param connection connection to re-use
   */
  static List<Delete> createDeletesForExistingTableSnapshotSizes(Connection connection)
      throws IOException {
    return createDeletesForExistingSnapshotsFromScan(connection, createScanForSpaceSnapshotSizes());
  }

  /**
   * Returns a list of {@code Delete} to remove given namespace snapshot
   * entries to removefrom quota table
   * @param snapshotEntriesToRemove the entries to remove
   */
  static List<Delete> createDeletesForExistingNamespaceSnapshotSizes(
      Set<String> snapshotEntriesToRemove) {
    List<Delete> deletes = new ArrayList<>();
    for (String snapshot : snapshotEntriesToRemove) {
      Delete d = new Delete(getNamespaceRowKey(snapshot));
      d.addColumns(QUOTA_FAMILY_USAGE, QUOTA_SNAPSHOT_SIZE_QUALIFIER);
      deletes.add(d);
    }
    return deletes;
  }

  /**
   * Returns a list of {@code Delete} to remove all namespace snapshot entries from quota table.
   * @param connection connection to re-use
   */
  static List<Delete> createDeletesForExistingNamespaceSnapshotSizes(Connection connection)
      throws IOException {
    return createDeletesForExistingSnapshotsFromScan(connection,
        createScanForNamespaceSnapshotSizes());
  }

  /**
   * Returns a list of {@code Delete} to remove all entries returned by the passed scanner.
   * @param connection connection to re-use
   * @param scan the scanner to use to generate the list of deletes
   */
  static List<Delete> createDeletesForExistingSnapshotsFromScan(Connection connection, Scan scan)
      throws IOException {
    List<Delete> deletes = new ArrayList<>();
    try (Table quotaTable = connection.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(scan)) {
      for (Result r : rs) {
        CellScanner cs = r.cellScanner();
        while (cs.advance()) {
          Cell c = cs.current();
          byte[] family = Bytes.copy(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
          byte[] qual =
              Bytes.copy(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
          Delete d = new Delete(r.getRow());
          d.addColumns(family, qual);
          deletes.add(d);
        }
      }
      return deletes;
    }
  }

  /**
   * Remove table usage snapshots (u:p columns) for the namespace passed
   * @param connection connection to re-use
   * @param namespace the namespace to fetch the list of table usage snapshots
   */
  static void deleteTableUsageSnapshotsForNamespace(Connection connection, String namespace)
    throws IOException {
    Scan s = new Scan();
    //Get rows for all tables in namespace
    s.setRowPrefixFilter(Bytes.add(QUOTA_TABLE_ROW_KEY_PREFIX, Bytes.toBytes(namespace + TableName.NAMESPACE_DELIM)));
    //Scan for table usage column (u:p) in quota table
    s.addColumn(QUOTA_FAMILY_USAGE,QUOTA_QUALIFIER_POLICY);
    //Scan for table quota column (q:s) if table has a space quota defined
    s.addColumn(QUOTA_FAMILY_INFO,QUOTA_QUALIFIER_SETTINGS);
    try (Table quotaTable = connection.getTable(QUOTA_TABLE_NAME);
         ResultScanner rs = quotaTable.getScanner(s)) {
      for (Result r : rs) {
        byte[] data = r.getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
        //if table does not have a table space quota defined, delete table usage column (u:p)
        if (data == null) {
          Delete delete = new Delete(r.getRow());
          delete.addColumns(QUOTA_FAMILY_USAGE,QUOTA_QUALIFIER_POLICY);
          quotaTable.delete(delete);
        }
      }
    }
  }

  /**
   * Fetches the computed size of all snapshots against tables in a namespace for space quotas.
   */
  static long getNamespaceSnapshotSize(
      Connection conn, String namespace) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      Result r = quotaTable.get(createGetNamespaceSnapshotSize(namespace));
      if (r.isEmpty()) {
        return 0L;
      }
      r.advance();
      return parseSnapshotSize(r.current());
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("Could not parse snapshot size value for namespace " + namespace, e);
    }
  }

  /**
   * Creates a {@code Get} to fetch the namespace's total snapshot size.
   */
  static Get createGetNamespaceSnapshotSize(String namespace) {
    Get g = new Get(getNamespaceRowKey(namespace));
    g.addColumn(QUOTA_FAMILY_USAGE, QUOTA_SNAPSHOT_SIZE_QUALIFIER);
    return g;
  }

  /**
   * Parses the snapshot size from the given Cell's value.
   */
  static long parseSnapshotSize(Cell c) throws InvalidProtocolBufferException {
    ByteString bs = UnsafeByteOperations.unsafeWrap(
        c.getValueArray(), c.getValueOffset(), c.getValueLength());
    return QuotaProtos.SpaceQuotaSnapshot.parseFrom(bs).getQuotaUsage();
  }

  /**
   * Returns a scanner for all existing namespace snapshot entries.
   */
  static Scan createScanForNamespaceSnapshotSizes() {
    return createScanForNamespaceSnapshotSizes(null);
  }

  /**
   * Returns a scanner for all namespace snapshot entries of the given namespace
   * @param namespace name of the namespace whose snapshot entries are to be scanned
   */
  static Scan createScanForNamespaceSnapshotSizes(String namespace) {
    Scan s = new Scan();
    if (namespace == null || namespace.isEmpty()) {
      // Read all namespaces, just look at the row prefix
      s.setRowPrefixFilter(QUOTA_NAMESPACE_ROW_KEY_PREFIX);
    } else {
      // Fetch the exact row for the table
      byte[] rowkey = getNamespaceRowKey(namespace);
      // Fetch just this one row
      s.withStartRow(rowkey).withStopRow(rowkey, true);
    }

    // Just the usage family and only the snapshot size qualifiers
    return s.addFamily(QUOTA_FAMILY_USAGE)
        .setFilter(new ColumnPrefixFilter(QUOTA_SNAPSHOT_SIZE_QUALIFIER));
  }

  static Scan createScanForSpaceSnapshotSizes() {
    return createScanForSpaceSnapshotSizes(null);
  }

  static Scan createScanForSpaceSnapshotSizes(TableName table) {
    Scan s = new Scan();
    if (null == table) {
      // Read all tables, just look at the row prefix
      s.setRowPrefixFilter(QUOTA_TABLE_ROW_KEY_PREFIX);
    } else {
      // Fetch the exact row for the table
      byte[] rowkey = getTableRowKey(table);
      // Fetch just this one row
      s.withStartRow(rowkey).withStopRow(rowkey, true);
    }

    // Just the usage family and only the snapshot size qualifiers
    return s.addFamily(QUOTA_FAMILY_USAGE).setFilter(
        new ColumnPrefixFilter(QUOTA_SNAPSHOT_SIZE_QUALIFIER));
  }

  /**
   * Fetches any persisted HBase snapshot sizes stored in the quota table. The sizes here are
   * computed relative to the table which the snapshot was created from. A snapshot's size will
   * not include the size of files which the table still refers. These sizes, in bytes, are what
   * is used internally to compute quota violation for tables and namespaces.
   *
   * @return A map of snapshot name to size in bytes per space quota computations
   */
  public static Map<String,Long> getObservedSnapshotSizes(Connection conn) throws IOException {
    try (Table quotaTable = conn.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(createScanForSpaceSnapshotSizes())) {
      final Map<String,Long> snapshotSizes = new HashMap<>();
      for (Result r : rs) {
        CellScanner cs = r.cellScanner();
        while (cs.advance()) {
          Cell c = cs.current();
          final String snapshot = extractSnapshotNameFromSizeCell(c);
          final long size = parseSnapshotSize(c);
          snapshotSizes.put(snapshot, size);
        }
      }
      return snapshotSizes;
    }
  }

  /**
   * Returns a multimap for all existing table snapshot entries.
   * @param conn connection to re-use
   */
  public static Multimap<TableName, String> getTableSnapshots(Connection conn) throws IOException {
    try (Table quotaTable = conn.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(createScanForSpaceSnapshotSizes())) {
      Multimap<TableName, String> snapshots = HashMultimap.create();
      for (Result r : rs) {
        CellScanner cs = r.cellScanner();
        while (cs.advance()) {
          Cell c = cs.current();

          final String snapshot = extractSnapshotNameFromSizeCell(c);
          snapshots.put(getTableFromRowKey(r.getRow()), snapshot);
        }
      }
      return snapshots;
    }
  }

  /**
   * Returns a set of the names of all namespaces containing snapshot entries.
   * @param conn connection to re-use
   */
  public static Set<String> getNamespaceSnapshots(Connection conn) throws IOException {
    try (Table quotaTable = conn.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(createScanForNamespaceSnapshotSizes())) {
      Set<String> snapshots = new HashSet<>();
      for (Result r : rs) {
        CellScanner cs = r.cellScanner();
        while (cs.advance()) {
          cs.current();
          snapshots.add(getNamespaceFromRowKey(r.getRow()));
        }
      }
      return snapshots;
    }
  }

  /**
   * Returns the current space quota snapshot of the given {@code tableName} from
   * {@code QuotaTableUtil.QUOTA_TABLE_NAME} or null if the no quota information is available for
   * that tableName.
   * @param conn connection to re-use
   * @param tableName name of the table whose current snapshot is to be retreived
   */
  public static SpaceQuotaSnapshot getCurrentSnapshotFromQuotaTable(Connection conn,
      TableName tableName) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      Map<TableName, SpaceQuotaSnapshot> snapshots = new HashMap<>(1);
      Result result = quotaTable.get(makeQuotaSnapshotGetForTable(tableName));
      // if we don't have any row corresponding to this get, return null
      if (result.isEmpty()) {
        return null;
      }
      // otherwise, extract quota snapshot in snapshots object
      extractQuotaSnapshot(result, snapshots);
      return snapshots.get(tableName);
    }
  }

  /* =========================================================================
   *  Quotas protobuf helpers
   */
  protected static Quotas quotasFromData(final byte[] data) throws IOException {
    return quotasFromData(data, 0, data.length);
  }

  protected static Quotas quotasFromData(
      final byte[] data, int offset, int length) throws IOException {
    int magicLen = ProtobufMagic.lengthOfPBMagic();
    if (!ProtobufMagic.isPBMagicPrefix(data, offset, magicLen)) {
      throw new IOException("Missing pb magic prefix");
    }
    return Quotas.parseFrom(new ByteArrayInputStream(data, offset + magicLen, length - magicLen));
  }

  protected static byte[] quotasToData(final Quotas data) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(ProtobufMagic.PB_MAGIC);
    data.writeTo(stream);
    return stream.toByteArray();
  }

  public static boolean isEmptyQuota(final Quotas quotas) {
    boolean hasSettings = false;
    hasSettings |= quotas.hasThrottle();
    hasSettings |= quotas.hasBypassGlobals();
    // Only when there is a space quota, make sure there's actually both fields provided
    // Otherwise, it's a noop.
    if (quotas.hasSpace()) {
      hasSettings |= (quotas.getSpace().hasSoftLimit() && quotas.getSpace().hasViolationPolicy());
    }
    return !hasSettings;
  }

  /* =========================================================================
   *  HTable helpers
   */
  protected static Result doGet(final Connection connection, final Get get)
      throws IOException {
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

  /* =========================================================================
   *  Quota table row key helpers
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

  protected static byte[] getRegionServerRowKey(final String regionServer) {
    return Bytes.add(QUOTA_REGION_SERVER_ROW_KEY_PREFIX, Bytes.toBytes(regionServer));
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

  private static String getRegionServerRowKeyRegex(final String regionServer) {
    return getRowKeyRegEx(QUOTA_REGION_SERVER_ROW_KEY_PREFIX, regionServer);
  }

  protected static byte[] getExceedThrottleQuotaRowKey() {
    return QUOTA_EXCEED_THROTTLE_QUOTA_ROW_KEY;
  }

  private static String getRowKeyRegEx(final byte[] prefix, final String regex) {
    return '^' + Pattern.quote(Bytes.toString(prefix)) + regex + '$';
  }

  protected static String getSettingsQualifierRegexForUserTable(final String table) {
    return '^' + Pattern.quote(Bytes.toString(QUOTA_QUALIFIER_SETTINGS_PREFIX)) +
          table + "(?<!" + Pattern.quote(Character.toString(TableName.NAMESPACE_DELIM)) + ")$";
  }

  protected static String getSettingsQualifierRegexForUserNamespace(final String namespace) {
    return '^' + Pattern.quote(Bytes.toString(QUOTA_QUALIFIER_SETTINGS_PREFIX)) +
                  namespace + Pattern.quote(Character.toString(TableName.NAMESPACE_DELIM)) + '$';
  }

  protected static boolean isNamespaceRowKey(final byte[] key) {
    return Bytes.startsWith(key, QUOTA_NAMESPACE_ROW_KEY_PREFIX);
  }

  protected static String getNamespaceFromRowKey(final byte[] key) {
    return Bytes.toString(key, QUOTA_NAMESPACE_ROW_KEY_PREFIX.length);
  }

  protected static boolean isRegionServerRowKey(final byte[] key) {
    return Bytes.startsWith(key, QUOTA_REGION_SERVER_ROW_KEY_PREFIX);
  }

  private static boolean isExceedThrottleQuotaRowKey(final byte[] key) {
    return Bytes.equals(key, QUOTA_EXCEED_THROTTLE_QUOTA_ROW_KEY);
  }

  protected static String getRegionServerFromRowKey(final byte[] key) {
    return Bytes.toString(key, QUOTA_REGION_SERVER_ROW_KEY_PREFIX.length);
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

  protected static SpaceQuota getProtoViolationPolicy(SpaceViolationPolicy policy) {
    return SpaceQuota.newBuilder()
          .setViolationPolicy(ProtobufUtil.toProtoViolationPolicy(policy))
          .build();
  }

  protected static SpaceViolationPolicy getViolationPolicy(SpaceQuota proto) {
    if (!proto.hasViolationPolicy()) {
      throw new IllegalArgumentException("Protobuf SpaceQuota does not have violation policy.");
    }
    return ProtobufUtil.toViolationPolicy(proto.getViolationPolicy());
  }

  protected static byte[] getSnapshotSizeQualifier(String snapshotName) {
    return Bytes.add(QUOTA_SNAPSHOT_SIZE_QUALIFIER, Bytes.toBytes(snapshotName));
  }

  protected static String extractSnapshotNameFromSizeCell(Cell c) {
    return Bytes.toString(
        c.getQualifierArray(), c.getQualifierOffset() + QUOTA_SNAPSHOT_SIZE_QUALIFIER.length,
        c.getQualifierLength() - QUOTA_SNAPSHOT_SIZE_QUALIFIER.length);
  }

  protected static long extractSnapshotSize(
      byte[] data, int offset, int length) throws InvalidProtocolBufferException {
    ByteString byteStr = UnsafeByteOperations.unsafeWrap(data, offset, length);
    return org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuotaSnapshot
        .parseFrom(byteStr).getQuotaUsage();
  }
}
