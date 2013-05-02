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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestColumnCountGetFilter {

  private final static HBaseTestingUtility TEST_UTIL = new
      HBaseTestingUtility();

  @Test
  public void testColumnCountGetFilter() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor("testColumnCountGetFilter");
    htd.addFamily(new HColumnDescriptor(family));
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.
      getDataTestDir(), TEST_UTIL.getConfiguration(), htd);
    try {
      String valueString = "ValueString";
      String row = "row-1";
      List<String> columns = generateRandomWords(10000, "column");
      Put p = new Put(Bytes.toBytes(row));
      p.setWriteToWAL(false);
      for (String column : columns) {
        KeyValue kv = KeyValueTestUtil.create(row, family, column, 0, valueString);
        p.add(kv);
      }
      region.put(p);

      Get get = new Get(row.getBytes());
      Filter filter = new ColumnCountGetFilter(100);
      get.setFilter(filter);
      Scan scan = new Scan(get);
      InternalScanner scanner = region.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      scanner.next(results);
      assertEquals(100, results.size());
    } finally {
      region.close();
      region.getLog().closeAndDelete();
    }

    region.close();
    region.getLog().closeAndDelete();
  }

  @Test
  public void testColumnCountGetFilterWithFilterList() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor("testColumnCountGetFilter");
    htd.addFamily(new HColumnDescriptor(family));
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.
      getDataTestDir(), TEST_UTIL.getConfiguration(), htd);
    try {
      String valueString = "ValueString";
      String row = "row-1";
      List<String> columns = generateRandomWords(10000, "column");
      Put p = new Put(Bytes.toBytes(row));
      p.setWriteToWAL(false);
      for (String column : columns) {
        KeyValue kv = KeyValueTestUtil.create(row, family, column, 0, valueString);
        p.add(kv);
      }
      region.put(p);

      Get get = new Get(row.getBytes());
      FilterList filterLst = new FilterList ();
      filterLst.addFilter( new ColumnCountGetFilter(100));
      get.setFilter(filterLst);
      Scan scan = new Scan(get);
      InternalScanner scanner = region.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      scanner.next(results);
      assertEquals(100, results.size());
    } finally {
      region.close();
      region.getLog().closeAndDelete();
    }

    region.close();
    region.getLog().closeAndDelete();
  }

  List<String> generateRandomWords(int numberOfWords, String suffix) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random()*2) + 1;
      char[] wordChar = new char[lengthOfWords];
      for (int j = 0; j < wordChar.length; j++) {
        wordChar[j] = (char) (Math.random() * 26 + 97);
      }
      String word;
      if (suffix == null) {
        word = new String(wordChar);
      } else {
        word = new String(wordChar) + suffix;
      }
      wordSet.add(word);
    }
    List<String> wordList = new ArrayList<String>(wordSet);
    return wordList;
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

