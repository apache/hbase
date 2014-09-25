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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.annotations.VisibleForTesting;

/**
 * Writes HFiles. Passed KeyValues must arrive in order.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created hfiles. Calling write(null,null) will forcibly roll
 * all HFiles being written.
 * <p>
 * Using this class as part of a MapReduce job is best done
 * using {@link #configureIncrementalLoad(Job, HTable)}.
 * @see KeyValueSortReducer
 * @deprecated use {@link HFileOutputFormat2} instead.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  static Log LOG = LogFactory.getLog(HFileOutputFormat.class);

  // This constant is public since the client can modify this when setting
  // up their conf object and thus refer to this symbol.
  // It is present for backwards compatibility reasons. Use it only to
  // override the auto-detection of datablock encoding.
  public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY =
    HFileOutputFormat2.DATABLOCK_ENCODING_OVERRIDE_CONF_KEY;

  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
      final TaskAttemptContext context) throws IOException, InterruptedException {
    return HFileOutputFormat2.createRecordWriter(context);
  }

  /**
   * Configure a MapReduce Job to perform an incremental load into the given
   * table. This
   * <ul>
   *   <li>Inspects the table to configure a total order partitioner</li>
   *   <li>Uploads the partitions file to the cluster and adds it to the DistributedCache</li>
   *   <li>Sets the number of reduce tasks to match the current number of regions</li>
   *   <li>Sets the output key/value class to match HFileOutputFormat's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   * </ul>
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, HTable table)
      throws IOException {
    HFileOutputFormat2.configureIncrementalLoad(job, table, HFileOutputFormat.class);
  }

  /**
   * Runs inside the task to deserialize column family to compression algorithm
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured compression algorithm
   */
  @VisibleForTesting
  static Map<byte[], Algorithm> createFamilyCompressionMap(Configuration
      conf) {
    return HFileOutputFormat2.createFamilyCompressionMap(conf);
  }

  /**
   * Runs inside the task to deserialize column family to bloom filter type
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the the configured bloom filter type
   */
  @VisibleForTesting
  static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
    return HFileOutputFormat2.createFamilyBloomTypeMap(conf);
  }

  /**
   * Runs inside the task to deserialize column family to block size
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured block size
   */
  @VisibleForTesting
  static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf) {
    return HFileOutputFormat2.createFamilyBlockSizeMap(conf);
  }

  /**
   * Runs inside the task to deserialize column family to data block encoding
   * type map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to HFileDataBlockEncoder for the
   *         configured data block type for the family
   */
  @VisibleForTesting
  static Map<byte[], DataBlockEncoding> createFamilyDataBlockEncodingMap(
      Configuration conf) {
    return HFileOutputFormat2.createFamilyDataBlockEncodingMap(conf);
  }

  /**
   * Configure <code>job</code> with a TotalOrderPartitioner, partitioning against
   * <code>splitPoints</code>. Cleans up the partitions file after job exists.
   */
  static void configurePartitioner(Job job, List<ImmutableBytesWritable> splitPoints)
      throws IOException {
    HFileOutputFormat2.configurePartitioner(job, splitPoints);
  }

  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException
   *           on failure to read column family descriptors
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @VisibleForTesting
  static void configureCompression(HTable table, Configuration conf) throws IOException {
    HFileOutputFormat2.configureCompression(table, conf);
  }

  /**
   * Serialize column family to block size map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException
   *           on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureBlockSize(HTable table, Configuration conf) throws IOException {
    HFileOutputFormat2.configureBlockSize(table, conf);
  }

  /**
   * Serialize column family to bloom type map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException
   *           on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureBloomType(HTable table, Configuration conf) throws IOException {
    HFileOutputFormat2.configureBloomType(table, conf);
  }

  /**
   * Serialize column family to data block encoding map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException
   *           on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureDataBlockEncoding(HTable table,
      Configuration conf) throws IOException {
    HFileOutputFormat2.configureDataBlockEncoding(table, conf);
  }
}
