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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link HFileScanner#reseekTo(org.apache.hadoop.hbase.Cell)}
 */
@Category({IOTests.class, SmallTests.class})
public class TestReseekTo {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testReseekTo() throws Exception {
    testReseekToInternals(TagUsage.NO_TAG);
    testReseekToInternals(TagUsage.ONLY_TAG);
    testReseekToInternals(TagUsage.PARTIAL_TAG);
  }

  private void testReseekToInternals(TagUsage tagUsage) throws IOException {
    Path ncTFile = new Path(TEST_UTIL.getDataTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
    if(tagUsage != TagUsage.NO_TAG){
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    }
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    HFileContext context = new HFileContextBuilder().withBlockSize(4000).build();
    HFile.Writer writer = HFile.getWriterFactory(
        TEST_UTIL.getConfiguration(), cacheConf)
            .withOutputStream(fout)
            .withFileContext(context)
            // NOTE: This test is dependent on this deprecated nonstandard comparator
            .withComparator(KeyValue.COMPARATOR)
            .create();
    int numberOfKeys = 1000;

    String valueString = "Value";

    List<Integer> keyList = new ArrayList<Integer>();
    List<String> valueList = new ArrayList<String>();

    for (int key = 0; key < numberOfKeys; key++) {
      String value = valueString + key;
      KeyValue kv;
      keyList.add(key);
      valueList.add(value);
      if(tagUsage == TagUsage.NO_TAG){
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            Bytes.toBytes(value));
        writer.append(kv);
      } else if (tagUsage == TagUsage.ONLY_TAG) {
        Tag t = new Tag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value), tags);
        writer.append(kv);
      } else {
        if (key % 4 == 0) {
          Tag t = new Tag((byte) 1, "myTag1");
          Tag[] tags = new Tag[1];
          tags[0] = t;
          kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
              HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value), tags);
          writer.append(kv);
        } else {
          kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
              HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value));
          writer.append(kv);
        }
      }
    }
    writer.close();
    fout.close();

    HFile.Reader reader = HFile.createReader(TEST_UTIL.getTestFileSystem(),
        ncTFile, cacheConf, TEST_UTIL.getConfiguration());
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i++) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      long start = System.nanoTime();
      scanner.seekTo(new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes
          .toBytes("qual"), Bytes.toBytes(value)));
      assertEquals(value, scanner.getValueString());
    }

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i += 10) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      long start = System.nanoTime();
      scanner.reseekTo(new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes
          .toBytes("qual"), Bytes.toBytes(value)));
      assertEquals("i is " + i, value, scanner.getValueString());
    }

    reader.close();
  }


}

