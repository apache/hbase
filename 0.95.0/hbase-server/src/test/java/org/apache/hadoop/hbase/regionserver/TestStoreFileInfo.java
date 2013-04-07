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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test HStoreFile
 */
@Category(SmallTests.class)
public class TestStoreFileInfo extends HBaseTestCase {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * Validate that we can handle valid tables with '.', '_', and '-' chars.
   */
  public void testStoreFileNames() {
    String[] legalHFileLink = { "MyTable_02=abc012-def345", "MyTable_02.300=abc012-def345",
      "MyTable_02-400=abc012-def345", "MyTable_02-400.200=abc012-def345",
      "MyTable_02=abc012-def345_SeqId_1_", "MyTable_02=abc012-def345_SeqId_20_" };
    for (String name: legalHFileLink) {
      assertTrue("should be a valid link: " + name, HFileLink.isHFileLink(name));
      assertTrue("should be a valid StoreFile" + name, StoreFileInfo.validateStoreFileName(name));
      assertFalse("should not be a valid reference: " + name, StoreFileInfo.isReference(name));

      String refName = name + ".6789";
      assertTrue("should be a valid link reference: " + refName,
          StoreFileInfo.isReference(refName));
      assertTrue("should be a valid StoreFile" + refName,
          StoreFileInfo.validateStoreFileName(refName));
    }

    String[] illegalHFileLink = { ".MyTable_02=abc012-def345", "-MyTable_02.300=abc012-def345",
      "MyTable_02-400=abc0_12-def345", "MyTable_02-400.200=abc012-def345...." };
    for (String name: illegalHFileLink) {
      assertFalse("should not be a valid link: " + name, HFileLink.isHFileLink(name));
    }
  }
}

