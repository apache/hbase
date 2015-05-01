/**
 *
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
package org.apache.hadoop.hbase.mob.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.mapreduce.SweepJob.SweepCounter;
import org.apache.hadoop.hbase.mob.mapreduce.SweepReducer.SweepPartitionId;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The wrapper of a DefaultMemStore.
 * This wrapper is used in the sweep reducer to buffer and sort the cells written from
 * the invalid and small mob files.
 * It's flushed when it's full, the mob data are written to the mob files, and their file names
 * are written back to store files of HBase.
 * This memStore is used to sort the cells in mob files.
 * In a reducer of sweep tool, the mob files are grouped by the same prefix (start key and date),
 * in each group, the reducer iterates the files and read the cells to a new and bigger mob file.
 * The cells in the same mob file are ordered, but cells across mob files are not.
 * So we need this MemStoreWrapper to sort those cells come from different mob files before
 * flushing them to the disk, when the memStore is big enough it's flushed as a new mob file.
 */
@InterfaceAudience.Private
public class MemStoreWrapper {

  private static final Log LOG = LogFactory.getLog(MemStoreWrapper.class);

  private MemStore memstore;
  private long flushSize;
  private SweepPartitionId partitionId;
  private Context context;
  private Configuration conf;
  private BufferedMutator table;
  private HColumnDescriptor hcd;
  private Path mobFamilyDir;
  private FileSystem fs;
  private CacheConfig cacheConfig;

  public MemStoreWrapper(Context context, FileSystem fs, BufferedMutator table, HColumnDescriptor hcd,
      MemStore memstore, CacheConfig cacheConfig) throws IOException {
    this.memstore = memstore;
    this.context = context;
    this.fs = fs;
    this.table = table;
    this.hcd = hcd;
    this.conf = context.getConfiguration();
    this.cacheConfig = cacheConfig;
    flushSize = this.conf.getLong(MobConstants.MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE,
        MobConstants.DEFAULT_MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE);
    mobFamilyDir = MobUtils.getMobFamilyPath(conf, table.getName(), hcd.getNameAsString());
  }

  public void setPartitionId(SweepPartitionId partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Flushes the memstore if the size is large enough.
   * @throws IOException
   */
  private void flushMemStoreIfNecessary() throws IOException {
    if (memstore.heapSize() >= flushSize) {
      flushMemStore();
    }
  }

  /**
   * Flushes the memstore anyway.
   * @throws IOException
   */
  public void flushMemStore() throws IOException {
    MemStoreSnapshot snapshot = memstore.snapshot();
    internalFlushCache(snapshot);
    memstore.clearSnapshot(snapshot.getId());
  }

  /**
   * Flushes the snapshot of the memstore.
   * Flushes the mob data to the mob files, and flushes the name of these mob files to HBase.
   * @param snapshot The snapshot of the memstore.
   * @throws IOException
   */
  private void internalFlushCache(final MemStoreSnapshot snapshot)
      throws IOException {
    if (snapshot.getCellsCount() == 0) {
      return;
    }
    // generate the files into a temp directory.
    String tempPathString = context.getConfiguration().get(SweepJob.WORKING_FILES_DIR_KEY);
    StoreFile.Writer mobFileWriter = MobUtils.createWriter(conf, fs, hcd,
        partitionId.getDate(), new Path(tempPathString), snapshot.getCellsCount(),
        hcd.getCompactionCompression(), partitionId.getStartKey(), cacheConfig);

    String relativePath = mobFileWriter.getPath().getName();
    LOG.info("Create files under a temp directory " + mobFileWriter.getPath().toString());

    byte[] referenceValue = Bytes.toBytes(relativePath);
    KeyValueScanner scanner = snapshot.getScanner();
    Cell cell = null;
    while (null != (cell = scanner.next())) {
      KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
      mobFileWriter.append(kv);
    }
    scanner.close();
    // Write out the log sequence number that corresponds to this output
    // hfile. The hfile is current up to and including logCacheFlushId.
    mobFileWriter.appendMetadata(Long.MAX_VALUE, false, snapshot.getCellsCount());
    mobFileWriter.close();

    MobUtils.commitFile(conf, fs, mobFileWriter.getPath(), mobFamilyDir, cacheConfig);
    context.getCounter(SweepCounter.FILE_AFTER_MERGE_OR_CLEAN).increment(1);
    // write reference/fileName back to the store files of HBase.
    scanner = snapshot.getScanner();
    scanner.seek(KeyValueUtil.createFirstOnRow(HConstants.EMPTY_START_ROW));
    cell = null;
    Tag tableNameTag = new Tag(TagType.MOB_TABLE_NAME_TAG_TYPE, Bytes.toBytes(this.table.getName().toString()));
    while (null != (cell = scanner.next())) {
      KeyValue reference = MobUtils.createMobRefKeyValue(cell, referenceValue, tableNameTag);
      Put put =
          new Put(reference.getRowArray(), reference.getRowOffset(), reference.getRowLength());
      put.add(reference);
      table.mutate(put);
      context.getCounter(SweepCounter.RECORDS_UPDATED).increment(1);
    }
    table.flush();
    scanner.close();
  }

  /**
   * Adds a KeyValue into the memstore.
   * @param kv The KeyValue to be added.
   * @throws IOException
   */
  public void addToMemstore(KeyValue kv) throws IOException {
    memstore.add(kv);
    // flush the memstore if it's full.
    flushMemStoreIfNecessary();
  }

}
