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
package org.apache.hadoop.hbase.backup.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Processes bulk load files from Write-Ahead Log (WAL) entries.
 * <p>
 * Used by backup/restore and replication flows to discover HFiles referenced by bulk-load WALEdits.
 * Returned {@link Path}s are constructed from the namespace/table/region/family/file components.
 * </p>
 */
@InterfaceAudience.Private
public final class BulkLoadProcessor {
  private BulkLoadProcessor() {
  }

  /**
   * Extract bulk-load file {@link Path}s from a list of {@link WAL.Entry}.
   * @param walEntries list of WAL entries.
   * @return list of Paths in discovery order; empty list if none
   * @throws IOException if descriptor parsing fails
   */
  public static List<Path> processBulkLoadFiles(List<WAL.Entry> walEntries) throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();

    for (WAL.Entry entry : walEntries) {
      bulkLoadFilePaths.addAll(processBulkLoadFiles(entry.getKey(), entry.getEdit()));
    }
    return bulkLoadFilePaths;
  }

  /**
   * Extract bulk-load file {@link Path}s from a single WAL entry.
   * @param key  WALKey containing table information; if null returns empty list
   * @param edit WALEdit to scan; if null returns empty list
   * @return list of Paths referenced by bulk-load descriptor(s) in this edit; may be empty or
   *         contain duplicates
   * @throws IOException if descriptor parsing fails
   */
  public static List<Path> processBulkLoadFiles(WALKey key, WALEdit edit) throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();

    for (Cell cell : edit.getCells()) {
      if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
        TableName tableName = key.getTableName();
        String namespace = tableName.getNamespaceAsString();
        String table = tableName.getQualifierAsString();
        bulkLoadFilePaths.addAll(processBulkLoadDescriptor(cell, namespace, table));
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
