package org.apache.hadoop.hbase.backup.replication;

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
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Private
public class StagedBulkloadFileRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(StagedBulkloadFileRegistry.class);
  private static final String TABLE_NAME = "staged_bulkload_files";
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("files");

  private final Connection connection;
  private final String peerId;

  public StagedBulkloadFileRegistry(Connection connection, String peerId) throws IOException {
    this.connection = connection;
    this.peerId = peerId;

    // Ensure the table exists
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf(TABLE_NAME);

    if (!admin.tableExists(tableName)) {
      LOG.info("Table '{}' does not exist. Creating it now.", TABLE_NAME);
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
      admin.createTable(tableDescriptor);
      LOG.info("Table '{}' created successfully.", TABLE_NAME);
    }
    admin.close();
  }

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
    BackupProtos.StagedBulkloadFilesInfo.Builder protoBuilder = BackupProtos.StagedBulkloadFilesInfo.newBuilder();
    protoBuilder.addAllFiles(files);
    return protoBuilder.build().toByteArray();
  }

  private List<String> deserializeFiles(byte[] data) throws IOException {
    LOG.trace("{} Deserializing staged bulkload files.", Utils.logPeerId(peerId));
    BackupProtos.StagedBulkloadFilesInfo proto = BackupProtos.StagedBulkloadFilesInfo.parseFrom(data);
    return new ArrayList<>(proto.getFilesList());
  }

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
                      List<String> files = BackupProtos.StagedBulkloadFilesInfo.parseFrom(filesData).getFilesList();
                      allFiles.addAll(files);
                  }
              }
              LOG.debug("Listed a total of {} staged bulkload files.", allFiles.size());
          }
      }

    return allFiles;
  }
}
