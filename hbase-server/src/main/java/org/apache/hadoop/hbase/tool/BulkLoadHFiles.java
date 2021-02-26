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
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The tool to let you load the output of {@code HFileOutputFormat} into an existing table
 * programmatically. Not thread safe.
 */
@InterfaceAudience.Public
public interface BulkLoadHFiles {

  static final String RETRY_ON_IO_EXCEPTION = "hbase.bulkload.retries.retryOnIOException";
  static final String MAX_FILES_PER_REGION_PER_FAMILY =
      "hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily";
  static final String ASSIGN_SEQ_IDS = "hbase.mapreduce.bulkload.assign.sequenceNumbers";
  static final String CREATE_TABLE_CONF_KEY = "create.table";
  static final String IGNORE_UNMATCHED_CF_CONF_KEY = "ignore.unmatched.families";
  static final String ALWAYS_COPY_FILES = "always.copy.files";
  /**
   * HBASE-24221 Support bulkLoadHFile by family to avoid long time waiting of bulkLoadHFile because
   * of compacting at server side
   */
  public static final String BULK_LOAD_HFILES_BY_FAMILY = "hbase.mapreduce.bulkload.by.family";

  /**
   * Represents an HFile waiting to be loaded. An queue is used in this class in order to support
   * the case where a region has split during the process of the load. When this happens, the HFile
   * is split into two physical parts across the new region boundary, and each part is added back
   * into the queue. The import process finishes when the queue is empty.
   */
  @InterfaceAudience.Public
  public static class LoadQueueItem {

    private final byte[] family;

    private final Path hfilePath;

    public LoadQueueItem(byte[] family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }

    @Override
    public String toString() {
      return "family:" + Bytes.toString(family) + " path:" + hfilePath.toString();
    }

    public byte[] getFamily() {
      return family;
    }

    public Path getFilePath() {
      return hfilePath;
    }
  }

  /**
   * Perform a bulk load of the given directory into the given pre-existing table.
   * @param tableName the table to load into
   * @param family2Files map of family to List of hfiles
   * @throws TableNotFoundException if table does not yet exist
   */
  Map<LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName, Map<byte[], List<Path>> family2Files)
      throws TableNotFoundException, IOException;

  /**
   * Perform a bulk load of the given directory into the given pre-existing table.
   * @param tableName the table to load into
   * @param dir the directory that was provided as the output path of a job using
   *          {@code HFileOutputFormat}
   * @throws TableNotFoundException if table does not yet exist
   */
  Map<LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName, Path dir)
      throws TableNotFoundException, IOException;

  static BulkLoadHFiles create(Configuration conf) {
    return new BulkLoadHFilesTool(conf);
  }

}
