/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Helper class to interact with the hbase:storefile system table
 *
 * <pre>
 *   ROW-KEY              FAMILY:QUALIFIER      DATA VALUE
 *   region-store-table   included:files        List&lt;Path&gt; filesIncludedInRead
 * </pre>
 *
 * The region encoded name is set as prefix for region split loading balance, and we use the
 * target table name as suffix such that operator can identify the records per table.
 *
 * included:files is used for persisting storefiles of StoreFileManager in the cases of store
 * opens and store closes. Meanwhile compactedFiles of StoreFileManager isn't being tracked
 * off-memory, because the updated included:files contains compactedFiles and the leftover
 * compactedFiles are either archived when a store closes or opens.
 *
 * TODO we will need a followup change to introduce in-memory temporarily file, such that further
 *      we can introduce a non-tracking temporarily storefiles left from a flush or compaction when
 *      a regionserver crashes without closing the store properly
 */

@InterfaceAudience.Private
public class HTableStoreFilePathAccessor extends AbstractStoreFilePathAccessor {

  public static final byte[] STOREFILE_FAMILY_INCLUDED = Bytes.toBytes(STOREFILE_INCLUDED_STR);

  private static final String DASH_SEPARATOR = "-";
  private static final String STOREFILE_QUALIFIER_STR = "filepaths";
  private static final byte[] STOREFILE_QUALIFIER = Bytes.toBytes(STOREFILE_QUALIFIER_STR);
  private static final int STOREFILE_TABLE_VERSIONS = 3;

  // TODO find a way for system table to support region split at table creation or remove this
  //  comment when we merge into hbase:meta table
  public static final TableDescriptor STOREFILE_TABLE_DESC =
    TableDescriptorBuilder.newBuilder(TableName.STOREFILE_TABLE_NAME)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(STOREFILE_FAMILY_INCLUDED)
          .setMaxVersions(STOREFILE_TABLE_VERSIONS)
          .setInMemory(true)
          .build())
      .setRegionSplitPolicyClassName(BusyRegionSplitPolicy.class.getName())
      .build();

  private Connection connection;

  public HTableStoreFilePathAccessor(Configuration conf, Connection connection) {
    super(conf);
    Preconditions.checkNotNull(connection, "connection cannot be null");
    this.connection = connection;
  }

  @Override
  public void initialize(final MasterServices masterServices) throws IOException {
    StorefileTrackingUtils.init(masterServices);
  }

  @Override
  List<Path> getStoreFilePaths(final String tableName, final String regionName,
    final String storeName, final String colFamily) throws IOException {
    validate(tableName, regionName, storeName, colFamily);
    byte[] colFamilyBytes = Bytes.toBytes(colFamily);
    Get get =
      new Get(Bytes.toBytes(getKey(tableName, regionName, storeName)));
    get.addColumn(colFamilyBytes, STOREFILE_QUALIFIER);
    Result result = doGet(get);
    if (result == null || result.isEmpty()) {
      return new ArrayList<>();
    }
    return byteToStoreFileList(result.getValue(colFamilyBytes, STOREFILE_QUALIFIER));
  }

  @Override
  public void writeStoreFilePaths(final String tableName, final String regionName,
    final String storeName, StoreFilePathUpdate storeFilePathUpdate)
    throws IOException {
    validate(tableName, regionName, storeName, storeFilePathUpdate);
    Put put = generatePutForStoreFilePaths(tableName, regionName, storeName, storeFilePathUpdate);
    doPut(put);
  }


  private Put generatePutForStoreFilePaths(final String tableName, final String regionName,
    final String storeName, final StoreFilePathUpdate storeFilePathUpdate) {
    Put put = new Put(Bytes.toBytes(getKey(tableName, regionName, storeName)));
    if (storeFilePathUpdate.hasStoreFilesUpdate()) {
      put.addColumn(Bytes.toBytes(STOREFILE_INCLUDED_STR), STOREFILE_QUALIFIER,
        storeFileListToByteArray(storeFilePathUpdate.getStoreFiles()));
    }
    return put;
  }

  @Override
  public void deleteStoreFilePaths(final String tableName, final String regionName,
    final String storeName) throws IOException {
    validate(tableName, regionName, storeName);
    Delete delete = new Delete(
      Bytes.toBytes(getKey(tableName, regionName, storeName)));
    delete.addColumns(STOREFILE_FAMILY_INCLUDED, STOREFILE_QUALIFIER);
    doDelete(Lists.newArrayList(delete));
  }

  @Override
  public void deleteRegion(String regionName) throws IOException {
    Scan scan = getScanWithFilter(regionName);
    List<Delete> familiesToDelete = new ArrayList<>();
    for (Result result : getResultScanner(scan)) {
      String rowKey = Bytes.toString(result.getRow());
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      familiesToDelete.add(delete);
    }
    doDelete(familiesToDelete);
  }

  @Override
  public Set<String> getTrackedFamilies(String tableName, String regionName)
    throws IOException {
    // find all rows by regionName
    Scan scan = getScanWithFilter(regionName);

    Set<String> families = new HashSet<>();
    for (Result result : getResultScanner(scan)) {
      String rowKey = Bytes.toString(result.getRow());
      String family =
        StorefileTrackingUtils.getFamilyFromKey(rowKey, tableName, regionName, getSeparator());
      families.add(family);
    }
    return families;
  }

  @Override
  String getSeparator() {
    return DASH_SEPARATOR;
  }

  private Result doGet(final Get get) throws IOException {
    try (Table table = getConnection().getTable(TableName.STOREFILE_TABLE_NAME)) {
      return table.get(get);
    }
  }

  private void doPut(final Put put) throws IOException {
    try (Table table = getConnection().getTable(TableName.STOREFILE_TABLE_NAME)) {
      table.put(put);
    }
  }

  private void doDelete(final List<Delete> delete) throws IOException {
    try (Table table = getConnection().getTable(TableName.STOREFILE_TABLE_NAME)) {
      table.delete(delete);
    }
  }

  private ResultScanner getResultScanner(final Scan scan) throws IOException {
    try (Table table = getConnection().getTable(TableName.STOREFILE_TABLE_NAME)) {
      return table.getScanner(scan);
    }
  }

  private Connection getConnection() throws IOException {
    if (connection == null) {
      throw new IOException("Connection should be provided by region server "
        + "and should not be null after initialized.");
    }
    return connection;
  }


  private Scan getScanWithFilter(String regionName) {
    Scan scan = new Scan();
    String regexPattern = "^" + regionName + getSeparator();
    RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL,
      new RegexStringComparator(regexPattern));
    scan.setFilter(rowFilter);
    scan.addColumn(STOREFILE_FAMILY_INCLUDED, STOREFILE_QUALIFIER);
    return scan;
  }

}
