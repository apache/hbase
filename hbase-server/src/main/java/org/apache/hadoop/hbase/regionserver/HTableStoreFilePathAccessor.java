/**
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Helper class to interact with the hbase:storefile system table
 *
 * <pre>
 *   ROW-KEY              FAMILY:QUALIFIER      DATA VALUE
 *   table-region-store   included:files        List<Path> filesIncludedInRead
 *   table-region-store   excluded:files        List<Path> filesExcludedFromRead/compactedFiles
 * </pre>
 */

@InterfaceAudience.Private
public class HTableStoreFilePathAccessor extends AbstractStoreFilePathAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(HTableStoreFilePathAccessor.class);

  public static final byte[] STOREFILE_FAMILY_INCLUDED = Bytes.toBytes(STOREFILE_INCLUDED_STR);
  public static final byte[] STOREFILE_FAMILY_EXCLUDED = Bytes.toBytes(STOREFILE_EXCLUDED_STR);

  private static final String DASH_SEPARATOR = "-";
  private static final String STOREFILE_QUALIFIER_STR = "filepaths";
  private static final byte[] STOREFILE_QUALIFIER = Bytes.toBytes(STOREFILE_QUALIFIER_STR);
  private static final long SLEEP_DELTA_MS = TimeUnit.MILLISECONDS.toMillis(100);
  private static final int STOREFILE_TABLE_VERSIONS = 3;
  private static final TableDescriptor STOREFILE_TABLE_DESC =
      TableDescriptorBuilder.newBuilder(TableName.STOREFILE_TABLE_NAME)
          .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(STOREFILE_FAMILY_INCLUDED)
                  .setMaxVersions(STOREFILE_TABLE_VERSIONS)
                  .setInMemory(true)
                  .build())
          .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(STOREFILE_FAMILY_EXCLUDED)
                  .setMaxVersions(STOREFILE_TABLE_VERSIONS)
                  .setInMemory(true)
                  .build())
          .build();

  private Connection connection;

  public HTableStoreFilePathAccessor(Configuration conf) {
    super(conf);
  }

  @Override
  public void initialize(final MasterServices masterServices) throws IOException {
    if (MetaTableAccessor.getTableState(getConnection(), TableName.STOREFILE_TABLE_NAME) != null) {
      LOG.info("{} table not found. Creating...", TableName.STOREFILE_TABLE_NAME);
      masterServices.createSystemTable(STOREFILE_TABLE_DESC);
    }
    waitForStoreFileTableOnline(masterServices);
  }

  @Override
  public List<Path> getIncludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName) throws IOException {
    validate(tableName, regionName, storeName);
    return getStoreFilePaths(tableName, regionName, storeName, STOREFILE_FAMILY_INCLUDED);
  }

  @Override
  public List<Path> getExcludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName) throws IOException {
    validate(tableName, regionName, storeName);
    return getStoreFilePaths(tableName, regionName, storeName, STOREFILE_FAMILY_EXCLUDED);
  }

  private List<Path> getStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final byte[] colFamily) throws IOException {
    Get get =
        new Get(Bytes.toBytes(getKey(tableName, regionName, storeName)));
    get.addColumn(colFamily, STOREFILE_QUALIFIER);
    Result result = doGet(get);
    if (result == null || result.isEmpty()) {
      return new ArrayList<>();
    }
    return byteToStoreFileList(result.getValue(colFamily, STOREFILE_QUALIFIER));
  }

  @Override
  public void writeIncludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final List<Path> storeFilePaths) throws IOException {
    validate(tableName, regionName, storeName);
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(storeFilePaths),
        "Storefile paths should not be empty when writing to " + STOREFILE_INCLUDED_STR
            + " data set");
    writeStoreFilePaths(tableName, regionName, storeName, STOREFILE_FAMILY_INCLUDED, storeFilePaths);
  }

  @Override
  public void writeExcludedStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final List<Path> storeFilePaths) throws IOException {
    validate(tableName, regionName, storeName);
    writeStoreFilePaths(tableName, regionName, storeName, STOREFILE_FAMILY_EXCLUDED, storeFilePaths);
  }

  private void writeStoreFilePaths(final String tableName, final String regionName,
      final String storeName, final byte[] colFamily, final List<Path> storeFilePaths)
      throws IOException {
    if (storeFilePaths == null) {
      return;
    }
    // we allow write with empty list for a newly created region or store with no files.
    Put put =
        new Put(Bytes.toBytes(getKey(tableName, regionName, storeName)));
    put.addColumn(colFamily, STOREFILE_QUALIFIER, storeFileListToByteArray(storeFilePaths));
    doPut(put);
  }

  @Override
  public void deleteStoreFilePaths(final String tableName, final String regionName,
      final String storeName) throws IOException {
    validate(tableName, regionName, storeName);
    Delete delete = new Delete(
        Bytes.toBytes(getKey(tableName, regionName, storeName)));
    delete.addColumns(STOREFILE_FAMILY_INCLUDED, STOREFILE_QUALIFIER);
    delete.addColumns(STOREFILE_FAMILY_EXCLUDED, STOREFILE_QUALIFIER);
    doDelete(delete);
  }

  @Override
  String getSeparator(){
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

  private void doDelete(final Delete delete) throws IOException {
    try (Table table = getConnection().getTable(TableName.STOREFILE_TABLE_NAME)) {
      table.delete(delete);
    }
  }

  private Connection getConnection() throws IOException {
    // singleton support and don't expect multi-threads have access to the same connection
    if (connection == null) {
      connection = ConnectionFactory.createConnection(conf);
    }
    return connection;
  }

  @Override
  public void close() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }

  private void waitForStoreFileTableOnline(MasterServices masterServices) throws IOException {
    try {
      long startTime = EnvironmentEdgeManager.currentTime();
      long timeout = conf.getLong(HConstants.STOREFILE_TABLE_INIT_TIMEOUT,
          HConstants.DEFAULT_STOREFILE_TABLE_INIT_TIMEOUT_MS);
      while (!isStoreFileTableAssignedAndEnabled(masterServices)) {
        if (EnvironmentEdgeManager.currentTime() - startTime + SLEEP_DELTA_MS > timeout) {
          throw new IOException("Time out " + timeout + " ms waiting for hbase:storefile table to "
              + "be assigned and enabled: " + masterServices.getTableStateManager()
              .getTableState(TableName.STOREFILE_TABLE_NAME));
        }
        Thread.sleep(SLEEP_DELTA_MS);
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted when wait for " + TableName.STOREFILE_TABLE_NAME
          + " to be assigned and enabled", e);
    }
  }

  boolean isStoreFileTableAssignedAndEnabled(MasterServices masterServices)
      throws IOException {
    return masterServices.getAssignmentManager().getRegionStates()
        .hasTableRegionStates(TableName.STOREFILE_TABLE_NAME) && masterServices
        .getTableStateManager().getTableState(TableName.STOREFILE_TABLE_NAME).isEnabled();
  }
}
