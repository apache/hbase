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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreUtils.class);

  @Test
  public void testGetBlockSize() throws IOException {
    int eightK = 8 * 1024;
    int oneM = 1024 * 1024;
    ColumnFamilyDescriptor cfDefault = ColumnFamilyDescriptorBuilder.of("cf1");
    ColumnFamilyDescriptor cf8K =
      ColumnFamilyDescriptorBuilder.newBuilder("cf2".getBytes()).setBlocksize(eightK).build();
    Configuration confDefault = new Configuration();
    Configuration conf8K = new Configuration();
    conf8K.setInt(HFile.BLOCK_SIZE_KEY, eightK);
    Configuration conf1M = new Configuration();
    conf1M.setInt(HFile.BLOCK_SIZE_KEY, oneM);
    assertEquals("Ignore null configuration with defaults", HConstants.DEFAULT_BLOCKSIZE,
      StoreUtils.getBlockSize(null, cfDefault.getBlocksize()));
    assertEquals("Ignore null configuration with a non-default column family block size", eightK,
      StoreUtils.getBlockSize(null, cf8K.getBlocksize()));
    assertEquals("Default case should hold no surprises", HConstants.DEFAULT_BLOCKSIZE,
      StoreUtils.getBlockSize(confDefault, cfDefault.getBlocksize()));
    assertEquals("Configuration should not override the non-default column family block size",
      eightK, StoreUtils.getBlockSize(confDefault, cf8K.getBlocksize()));
    assertEquals("Configuration should not override the non-default column family block size",
      eightK, StoreUtils.getBlockSize(conf1M, cf8K.getBlocksize()));
    assertEquals(
      "Default column family block size should not override a non-default size in configuration",
      eightK, StoreUtils.getBlockSize(conf8K, cfDefault.getBlocksize()));
    assertEquals(
      "Default column family block size should not override a non-default size in configuration",
      oneM, StoreUtils.getBlockSize(conf1M, cfDefault.getBlocksize()));
    assertEquals(
      "Non-default column family block size should override a non-default size in configuration",
      eightK, StoreUtils.getBlockSize(conf1M, cf8K.getBlocksize()));
  }

}
