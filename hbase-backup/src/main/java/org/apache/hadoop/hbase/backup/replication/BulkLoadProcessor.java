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
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Processes bulk load files from Write-Ahead Log (WAL) entries for HBase replication.
 * <p>
 * This utility class extracts and constructs the file paths of bulk-loaded files based on WAL
 * entries. It processes bulk load descriptors and their associated store descriptors to generate
 * the paths for each bulk-loaded file.
 * <p>
 * The class is designed for scenarios where replicable bulk load operations need to be parsed and
 * their file paths need to be determined programmatically.
 * </p>
 */
@InterfaceAudience.Private
public final class BulkLoadProcessor {
  private BulkLoadProcessor() {
  }

  public static List<Path> processBulkLoadFiles(List<WAL.Entry> walEntries) throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();

    for (WAL.Entry entry : walEntries) {
      WALEdit edit = entry.getEdit();
      for (Cell cell : edit.getCells()) {
        if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
          TableName tableName = entry.getKey().getTableName();
          String namespace = tableName.getNamespaceAsString();
          String table = tableName.getQualifierAsString();
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
      return bulkLoadFilePaths; // Skip if not replicable
    }

    String regionName = bld.getEncodedRegionName().toStringUtf8();
    for (WALProtos.StoreDescriptor storeDescriptor : bld.getStoresList()) {
      bulkLoadFilePaths
        .addAll(processStoreDescriptor(storeDescriptor, namespace, table, regionName));
    }

    return bulkLoadFilePaths;
  }

  private static List<Path> processStoreDescriptor(WALProtos.StoreDescriptor storeDescriptor,
    String namespace, String table, String regionName) {
    List<Path> paths = new ArrayList<>();
    String columnFamily = storeDescriptor.getFamilyName().toStringUtf8();

    for (String storeFile : storeDescriptor.getStoreFileList()) {
      paths.add(new Path(namespace,
        new Path(table, new Path(regionName, new Path(columnFamily, storeFile)))));
    }

    return paths;
  }
}
