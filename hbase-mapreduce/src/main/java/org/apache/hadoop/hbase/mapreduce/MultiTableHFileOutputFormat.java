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

package org.apache.hadoop.hbase.mapreduce;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Create 3 level tree directory, first level is using table name as parent
 * directory and then use family name as child directory, and all related HFiles
 * for one family are under child directory
 * -tableName1
 *     -columnFamilyName1
 *     -columnFamilyName2
 *         -HFiles
 * -tableName2
 *     -columnFamilyName1
 *         -HFiles
 *     -columnFamilyName2
 */
@InterfaceAudience.Public
public class MultiTableHFileOutputFormat extends HFileOutputFormat2 {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableHFileOutputFormat.class);

  /**
   * Creates a composite key to use as a mapper output key when using
   * MultiTableHFileOutputFormat.configureIncrementaLoad to set up bulk ingest job
   *
   * @param tableName Name of the Table - Eg: TableName.getNameAsString()
   * @param suffix    Usually represents a rowkey when creating a mapper key or column family
   * @return          byte[] representation of composite key
   */
  public static byte[] createCompositeKey(byte[] tableName,
                                          byte[] suffix) {
    return combineTableNameSuffix(tableName, suffix);
  }

  /**
   * Alternate api which accepts an ImmutableBytesWritable for the suffix
   * @see MultiTableHFileOutputFormat#createCompositeKey(byte[], byte[])
   */
  public static byte[] createCompositeKey(byte[] tableName,
                                          ImmutableBytesWritable suffix) {
    return combineTableNameSuffix(tableName, suffix.get());
  }

  /**
   * Alternate api which accepts a String for the tableName and ImmutableBytesWritable for the
   * suffix
   * @see MultiTableHFileOutputFormat#createCompositeKey(byte[], byte[])
   */
  public static byte[] createCompositeKey(String tableName,
                                          ImmutableBytesWritable suffix) {
    return combineTableNameSuffix(tableName.getBytes(Charset.forName("UTF-8")), suffix.get());
  }

  /**
   * Analogous to
   * {@link HFileOutputFormat2#configureIncrementalLoad(Job, TableDescriptor, RegionLocator)},
   * this function will configure the requisite number of reducers to write HFiles for multple
   * tables simultaneously
   *
   * @param job                   See {@link org.apache.hadoop.mapreduce.Job}
   * @param multiTableDescriptors Table descriptor and region locator pairs
   * @throws IOException
   */
  public static void configureIncrementalLoad(Job job, List<TableInfo>
      multiTableDescriptors)
      throws IOException {
    MultiTableHFileOutputFormat.configureIncrementalLoad(job, multiTableDescriptors,
            MultiTableHFileOutputFormat.class);
  }

  final private static int validateCompositeKey(byte[] keyBytes) {

    int separatorIdx = Bytes.indexOf(keyBytes, tableSeparator);

    // Either the separator was not found or a tablename wasn't present or a key wasn't present
    if (separatorIdx == -1) {
      throw new IllegalArgumentException("Invalid format for composite key [" + Bytes
              .toStringBinary(keyBytes) + "]. Cannot extract tablename and suffix from key");
    }
    return separatorIdx;
  }

  protected static byte[] getTableName(byte[] keyBytes) {
    int separatorIdx = validateCompositeKey(keyBytes);
    return Bytes.copy(keyBytes, 0, separatorIdx);
  }

  protected static byte[] getSuffix(byte[] keyBytes) {
    int separatorIdx = validateCompositeKey(keyBytes);
    return Bytes.copy(keyBytes, separatorIdx+1, keyBytes.length - separatorIdx - 1);
  }
}
