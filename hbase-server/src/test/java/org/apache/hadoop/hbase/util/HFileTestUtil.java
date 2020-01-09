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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobUtils;

/**
 * Utility class for HFile-related testing.
 */
public class HFileTestUtil {

  public static final String OPT_DATA_BLOCK_ENCODING_USAGE =
    "Encoding algorithm (e.g. prefix "
        + "compression) to use for data blocks in the test column family, "
        + "one of " + Arrays.toString(DataBlockEncoding.values()) + ".";
  public static final String OPT_DATA_BLOCK_ENCODING =
      HColumnDescriptor.DATA_BLOCK_ENCODING.toLowerCase(Locale.ROOT);
  /** Column family used by the test */
  public static byte[] DEFAULT_COLUMN_FAMILY = Bytes.toBytes("test_cf");
  /** Column families used by the test */
  public static final byte[][] DEFAULT_COLUMN_FAMILIES = { DEFAULT_COLUMN_FAMILY };

  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key @ family:qualifier.  The value will be the key value.
   * This file will not have tags.
   */
  public static void createHFile(
      Configuration configuration,
      FileSystem fs, Path path,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException {
      createHFile(configuration, fs, path, DataBlockEncoding.NONE, family, qualifier,
        startKey, endKey, numRows, false);
  }

  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key @ family:qualifier.  The value will be the key value.
   * This file will use certain data block encoding algorithm.
   */
  public static void createHFileWithDataBlockEncoding(
      Configuration configuration,
      FileSystem fs, Path path, DataBlockEncoding encoding,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException {
      createHFile(configuration, fs, path, encoding, family, qualifier, startKey, endKey,
        numRows, false);
  }

  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key @ family:qualifier.  The value will be the key value.
   * This cells will also have a tag whose value is the key.
   */
  public static void createHFileWithTags(
      Configuration configuration,
      FileSystem fs, Path path,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException {
      createHFile(configuration, fs, path, DataBlockEncoding.NONE, family, qualifier,
        startKey, endKey, numRows, true);
  }

  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key @ family:qualifier.
   * If withTag is true, we add the rowKey as the tag value for
   * tagtype MOB_TABLE_NAME_TAG_TYPE
   */
  public static void createHFile(
      Configuration configuration,
      FileSystem fs, Path path, DataBlockEncoding encoding,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows, boolean withTag) throws IOException {
    HFileContext meta = new HFileContextBuilder()
        .withIncludesTags(withTag)
        .withDataBlockEncoding(encoding)
        .withColumnFamily(family)
        .build();
    HFile.Writer writer = HFile.getWriterFactory(configuration, new CacheConfig(configuration))
        .withPath(fs, path)
        .withFileContext(meta)
        .create();
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows - 2)) {
        Cell kv = new KeyValue(key, family, qualifier, now, key);
        if (withTag) {
          // add a tag.  Arbitrarily chose mob tag since we have a helper already.
          Tag tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE, key);
          kv = MobUtils.createMobRefCell(kv, key, tableNameTag);

          // verify that the kv has the tag.
          Optional<Tag> tag = PrivateCellUtil.getTag(kv, TagType.MOB_TABLE_NAME_TAG_TYPE);
          if (!tag.isPresent()) {
            throw new IllegalStateException("Tag didn't stick to KV " + kv.toString());
          }
        }
        writer.append(kv);
      }
    } finally {
      writer.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
      writer.close();
    }
  }

  /**
   * This verifies that each cell has a tag that is equal to its rowkey name.  For this to work
   * the hbase instance must have HConstants.RPC_CODEC_CONF_KEY set to
   * KeyValueCodecWithTags.class.getCanonicalName());
   * @param table table containing tagged cells
   * @throws IOException if problems reading table
   */
  public static void verifyTags(Table table) throws IOException {
    ResultScanner s = table.getScanner(new Scan());
    for (Result r : s) {
      for (Cell c : r.listCells()) {
        Optional<Tag> tag = PrivateCellUtil.getTag(c, TagType.MOB_TABLE_NAME_TAG_TYPE);
        if (!tag.isPresent()) {
          fail(c.toString() + " has null tag");
          continue;
        }
        Tag t = tag.get();
        byte[] tval = Tag.cloneValue(t);
        assertArrayEquals(c.toString() + " has tag" + Bytes.toString(tval),
            r.getRow(), tval);
      }
    }
  }
}
