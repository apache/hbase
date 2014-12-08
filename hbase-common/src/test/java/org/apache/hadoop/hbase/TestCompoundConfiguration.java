/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCompoundConfiguration extends TestCase {
  private Configuration baseConf;
  private int baseConfSize;

  @Override
  protected void setUp() throws Exception {
    baseConf = new Configuration();
    baseConf.set("A", "1");
    baseConf.setInt("B", 2);
    baseConf.set("C", "3");
    baseConfSize = baseConf.size();
  }

  @Test
  public void testBasicFunctionality() throws ClassNotFoundException {
    CompoundConfiguration compoundConf = new CompoundConfiguration()
        .add(baseConf); 
    assertEquals("1", compoundConf.get("A"));
    assertEquals(2, compoundConf.getInt("B", 0));
    assertEquals(3, compoundConf.getInt("C", 0));
    assertEquals(0, compoundConf.getInt("D", 0));

    assertEquals(CompoundConfiguration.class, compoundConf
        .getClassByName(CompoundConfiguration.class.getName()));
    try {
      compoundConf.getClassByName("bad_class_name");
      fail("Trying to load bad_class_name should throw an exception");
    } catch (ClassNotFoundException e) {
      // win!
    }
  }

  @Test
  public void testPut() {
    CompoundConfiguration compoundConf = new CompoundConfiguration()
      .add(baseConf);
    assertEquals("1", compoundConf.get("A"));
    assertEquals(2, compoundConf.getInt("B", 0));
    assertEquals(3, compoundConf.getInt("C", 0));
    assertEquals(0, compoundConf.getInt("D", 0));

    compoundConf.set("A", "1337");
    compoundConf.set("string", "stringvalue");
    assertEquals(1337, compoundConf.getInt("A", 0));
    assertEquals("stringvalue", compoundConf.get("string"));

    // we didn't modify the base conf
    assertEquals("1", baseConf.get("A"));
    assertNull(baseConf.get("string"));

    // adding to the base shows up in the compound
    baseConf.set("setInParent", "fromParent");
    assertEquals("fromParent", compoundConf.get("setInParent"));
  }

  @Test
  public void testWithConfig() {
    Configuration conf = new Configuration();
    conf.set("B", "2b");
    conf.set("C", "33");
    conf.set("D", "4");

    CompoundConfiguration compoundConf = new CompoundConfiguration()
        .add(baseConf)
        .add(conf);
    assertEquals("1", compoundConf.get("A"));
    assertEquals("2b", compoundConf.get("B"));
    assertEquals(33, compoundConf.getInt("C", 0));
    assertEquals("4", compoundConf.get("D"));
    assertEquals(4, compoundConf.getInt("D", 0));
    assertNull(compoundConf.get("E"));
    assertEquals(6, compoundConf.getInt("F", 6));
    
    int cnt = 0;
    for (Map.Entry<String,String> entry : compoundConf) {
      cnt++;
      if (entry.getKey().equals("B")) assertEquals("2b", entry.getValue());
      else if (entry.getKey().equals("G")) assertEquals(null, entry.getValue());
    }
    // verify that entries from ImmutableConfigMap's are merged in the iterator's view
    assertEquals(baseConfSize + 1, cnt);
  }

  private ImmutableBytesWritable strToIbw(String s) {
    return new ImmutableBytesWritable(Bytes.toBytes(s));
  }

  @Test
  public void testWithIbwMap() {
    Map<ImmutableBytesWritable, ImmutableBytesWritable> map =
      new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();
    map.put(strToIbw("B"), strToIbw("2b"));
    map.put(strToIbw("C"), strToIbw("33"));
    map.put(strToIbw("D"), strToIbw("4"));
    // unlike config, note that IBW Maps can accept null values
    map.put(strToIbw("G"), null);

    CompoundConfiguration compoundConf = new CompoundConfiguration()
      .add(baseConf)
      .addWritableMap(map);
    assertEquals("1", compoundConf.get("A"));
    assertEquals("2b", compoundConf.get("B"));
    assertEquals(33, compoundConf.getInt("C", 0));
    assertEquals("4", compoundConf.get("D"));
    assertEquals(4, compoundConf.getInt("D", 0));
    assertNull(compoundConf.get("E"));
    assertEquals(6, compoundConf.getInt("F", 6));
    assertNull(compoundConf.get("G"));
    
    int cnt = 0;
    for (Map.Entry<String,String> entry : compoundConf) {
      cnt++;
      if (entry.getKey().equals("B")) assertEquals("2b", entry.getValue());
      else if (entry.getKey().equals("G")) assertEquals(null, entry.getValue());
    }
    // verify that entries from ImmutableConfigMap's are merged in the iterator's view
    assertEquals(baseConfSize + 2, cnt);

    // Verify that adding map after compound configuration is modified overrides properly
    CompoundConfiguration conf2 = new CompoundConfiguration();
    conf2.set("X", "modification");
    conf2.set("D", "not4");
    assertEquals("modification", conf2.get("X"));
    assertEquals("not4", conf2.get("D"));
    conf2.addWritableMap(map);
    assertEquals("4", conf2.get("D")); // map overrides
  }

  @Test
  public void testWithStringMap() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("B", "2b");
    map.put("C", "33");
    map.put("D", "4");
    // unlike config, note that IBW Maps can accept null values
    map.put("G", null);

    CompoundConfiguration compoundConf = new CompoundConfiguration().addStringMap(map);
    assertEquals("2b", compoundConf.get("B"));
    assertEquals(33, compoundConf.getInt("C", 0));
    assertEquals("4", compoundConf.get("D"));
    assertEquals(4, compoundConf.getInt("D", 0));
    assertNull(compoundConf.get("E"));
    assertEquals(6, compoundConf.getInt("F", 6));
    assertNull(compoundConf.get("G"));

    int cnt = 0;
    for (Map.Entry<String,String> entry : compoundConf) {
      cnt++;
      if (entry.getKey().equals("B")) assertEquals("2b", entry.getValue());
      else if (entry.getKey().equals("G")) assertEquals(null, entry.getValue());
    }
    // verify that entries from ImmutableConfigMap's are merged in the iterator's view
    assertEquals(4, cnt);
    
    // Verify that adding map after compound configuration is modified overrides properly
    CompoundConfiguration conf2 = new CompoundConfiguration();
    conf2.set("X", "modification");
    conf2.set("D", "not4");
    assertEquals("modification", conf2.get("X"));
    assertEquals("not4", conf2.get("D"));
    conf2.addStringMap(map);
    assertEquals("4", conf2.get("D")); // map overrides
  }

  @Test
  public void testLaterConfigsOverrideEarlier() {
    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("A", "2");
    map1.put("D", "5");
    Map<String, String> map2 = new HashMap<String, String>();
    String newValueForA = "3", newValueForB = "4";
    map2.put("A", newValueForA);
    map2.put("B", newValueForB);

    CompoundConfiguration compoundConf = new CompoundConfiguration()
      .addStringMap(map1).add(baseConf);
    assertEquals("1", compoundConf.get("A"));
    assertEquals("5", compoundConf.get("D"));
    compoundConf.addStringMap(map2);
    assertEquals(newValueForA, compoundConf.get("A"));
    assertEquals(newValueForB, compoundConf.get("B"));
    assertEquals("5", compoundConf.get("D"));

    int cnt = 0;
    for (Map.Entry<String,String> entry : compoundConf) {
      cnt++;
      if (entry.getKey().equals("A")) assertEquals(newValueForA, entry.getValue());
      else if (entry.getKey().equals("B")) assertEquals(newValueForB, entry.getValue());
    }
    // verify that entries from ImmutableConfigMap's are merged in the iterator's view
    assertEquals(baseConfSize + 1, cnt);
  }
}
