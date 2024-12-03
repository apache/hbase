package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
public final class BulkLoadProcessor {
  private BulkLoadProcessor() { }

  public static List<Path> processBulkLoadFiles(TableName tableName, List<WAL.Entry> walEntries)
    throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();
    String namespace = tableName.getNamespaceAsString();
    String table = tableName.getQualifierAsString();

    for (WAL.Entry entry : walEntries) {
      WALEdit edit = entry.getEdit();
      for (Cell cell : edit.getCells()) {
        if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
          bulkLoadFilePaths.addAll(processBulkLoadDescriptor(cell, namespace, table));
        }
      }
    }
    return bulkLoadFilePaths;
  }

  private static List<Path> processBulkLoadDescriptor(Cell cell, String namespace, String table)
    throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();
    WALProtos.BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);

    if (bld == null || !bld.getReplicate() || bld.getEncodedRegionName() == null) {
      return bulkLoadFilePaths;  // Skip if not replicable
    }

    String regionName = bld.getEncodedRegionName().toStringUtf8();
    for (WALProtos.StoreDescriptor storeDescriptor : bld.getStoresList()) {
      bulkLoadFilePaths.addAll(processStoreDescriptor(storeDescriptor, namespace, table, regionName));
    }

    return bulkLoadFilePaths;
  }

  private static List<Path> processStoreDescriptor(WALProtos.StoreDescriptor storeDescriptor,
    String namespace, String table, String regionName) {
    List<Path> paths = new ArrayList<>();
    String columnFamily = storeDescriptor.getFamilyName().toStringUtf8();

    for (String storeFile : storeDescriptor.getStoreFileList()) {
      paths.add(new Path(namespace, new Path(table, new Path(regionName, new
        Path(columnFamily, storeFile)))));
    }

    return paths;
  }
}
