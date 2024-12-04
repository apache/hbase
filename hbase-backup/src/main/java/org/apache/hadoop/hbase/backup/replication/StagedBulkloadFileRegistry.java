/*
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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;

/**
 * A registry for managing staged bulk load files associated with a replication peer in HBase. This
 * class ensures the required table for storing bulk load file metadata exists and provides methods
 * to add, remove, and retrieve staged files for a given peer.
 */
@InterfaceAudience.Private
public class StagedBulkloadFileRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(StagedBulkloadFileRegistry.class);
  private static final String TABLE_NAME = "staged_bulkload_files";
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("files");

  private final Connection connection;
  private final String peerId;

  /**
   * Constructs a registry for managing staged bulk load files. Ensures the required table exists in
   * the HBase cluster.
   * @param connection the HBase connection
   * @param peerId     the replication peer ID associated with this registry
   * @throws IOException if an error occurs while ensuring the table exists
   */
  public StagedBulkloadFileRegistry(Connection connection, String peerId) throws IOException {
    this.connection = connection;
    this.peerId = peerId;

    // Ensure the table exists
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf(TABLE_NAME);

    if (!admin.tableExists(tableName)) {
      LOG.info("Table '{}' does not exist. Creating it now.", TABLE_NAME);
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();
      admin.createTable(tableDescriptor);
      LOG.info("Table '{}' created successfully.", TABLE_NAME);
    }
    admin.close();
  }

  /**
   * Fetches the list of staged bulk load files for the current replication peer.
   * @return a list of file paths as strings
   * @throws IOException if an error occurs while fetching data from HBase
   */
  public List<String> getStagedFiles() throws IOException {
    LOG.debug("{} Fetching staged files.", Utils.logPeerId(peerId));
    List<String> stagedFiles = new ArrayList<>();
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Get get = new Get(Bytes.toBytes(peerId));
      get.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER);

      Result result = table.get(get);
      byte[] filesData = result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER);

      if (filesData != null) {
        stagedFiles = deserializeFiles(filesData);
        LOG.debug("{} Fetched {} staged files.", Utils.logPeerId(peerId), stagedFiles.size());
      } else {
        LOG.debug("{} No staged files found.", Utils.logPeerId(peerId));
      }
    }
    return stagedFiles;
  }

  /**
   * Adds new staged bulk load files for the current replication peer. Existing files are preserved,
   * and the new files are appended to the list.
   * @param newFiles a list of file paths to add
   * @throws IOException if an error occurs while updating HBase
   */
  public void addStagedFiles(List<Path> newFiles) throws IOException {
    LOG.debug("{} Adding {} new staged files.", Utils.logPeerId(peerId), newFiles.size());
    List<String> existingFiles = getStagedFiles();
    existingFiles.addAll(newFiles.stream().map(Path::toString).toList());

    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Put put = new Put(Bytes.toBytes(peerId));
      put.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, serializeFiles(existingFiles));
      table.put(put);
      LOG.debug("{} Successfully added {} files.", Utils.logPeerId(peerId), newFiles.size());
    }
  }

  /**
   * Removes specified bulk load files from the staged files for the current replication peer.
   * @param filesToRemove a list of file paths to remove
   * @throws IOException if an error occurs while updating HBase
   */
  public void removeStagedFiles(List<Path> filesToRemove) throws IOException {
    LOG.debug("{} Removing {} staged files.", Utils.logPeerId(peerId), filesToRemove.size());
    List<String> existingFiles = getStagedFiles();
    existingFiles.removeAll(filesToRemove.stream().map(Path::toString).toList());

    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Put put = new Put(Bytes.toBytes(peerId));
      put.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, serializeFiles(existingFiles));
      table.put(put);
      LOG.debug("{} Successfully removed {} files.", Utils.logPeerId(peerId), filesToRemove.size());
    }
  }

  private byte[] serializeFiles(List<String> files) {
    LOG.trace("{} Serializing {} files.", Utils.logPeerId(peerId), files.size());
    BackupProtos.StagedBulkloadFilesInfo.Builder protoBuilder =
      BackupProtos.StagedBulkloadFilesInfo.newBuilder();
    protoBuilder.addAllFiles(files);
    return protoBuilder.build().toByteArray();
  }

  private List<String> deserializeFiles(byte[] data) throws IOException {
    LOG.trace("{} Deserializing staged bulkload files.", Utils.logPeerId(peerId));
    BackupProtos.StagedBulkloadFilesInfo proto =
      BackupProtos.StagedBulkloadFilesInfo.parseFrom(data);
    return new ArrayList<>(proto.getFilesList());
  }

  /**
   * Lists all staged bulk load files across all peers in the HBase cluster.
   * @param connection the HBase connection
   * @return a set of file paths as strings representing all staged bulk load files
   * @throws IOException if an error occurs while scanning the table
   */
  public static Set<String> listAllBulkloadFiles(Connection connection) throws IOException {
    LOG.debug("Listing all staged bulkload files from table '{}'.", TABLE_NAME);
    Set<String> allFiles = new HashSet<>();

    try (Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
        LOG.debug("Table '{}' does not exist. Returning empty set.", TABLE_NAME);
        return allFiles;
      }

      try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner scanner = table.getScanner(new Scan())) {

        for (Result result : scanner) {
          byte[] filesData = result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER);
          if (filesData != null) {
            List<String> files =
              BackupProtos.StagedBulkloadFilesInfo.parseFrom(filesData).getFilesList();
            allFiles.addAll(files);
          }
        }
        LOG.debug("Listed a total of {} staged bulkload files.", allFiles.size());
      }
    }

    return allFiles;
  }
}
