/**
 * Copyright 2010 The Apache Software Foundation
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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link HFileScanner#reseekTo(byte[])}
 */
@Category(MediumTests.class)
public class TestReseekTo {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testReseekTo() throws Exception {

    Path ncTFile = new Path(TEST_UTIL.getTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    HFile.Writer writer = HFile.getWriterFactory(
        TEST_UTIL.getConfiguration(), cacheConf)
            .withOutputStream(fout)
            .withBlockSize(4000)
            .create();
    int numberOfKeys = 1000;

    String valueString = "Value";

    List<Integer> keyList = new ArrayList<Integer>();
    List<String> valueList = new ArrayList<String>();

    for (int key = 0; key < numberOfKeys; key++) {
      String value = valueString + key;
      keyList.add(key);
      valueList.add(value);
      writer.append(Bytes.toBytes(key), Bytes.toBytes(value));
    }
    writer.close();
    fout.close();

    HFile.Reader reader = HFile.createReader(TEST_UTIL.getTestFileSystem(),
        ncTFile, cacheConf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true, false);

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i++) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      scanner.seekTo(Bytes.toBytes(key));
      assertEquals(value, scanner.getValueString());
    }

    scanner.seekTo();
    for (int i = 0; i < keyList.size(); i += 10) {
      Integer key = keyList.get(i);
      String value = valueList.get(i);
      scanner.reseekTo(Bytes.toBytes(key));
      assertEquals("i is " + i, value, scanner.getValueString());
    }
  }

}
