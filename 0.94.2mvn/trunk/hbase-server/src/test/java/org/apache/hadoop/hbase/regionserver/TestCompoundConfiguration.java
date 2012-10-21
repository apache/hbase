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
package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.CompoundConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.SmallTests;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import junit.framework.TestCase;

@Category(SmallTests.class)
public class TestCompoundConfiguration extends TestCase {
  private Configuration baseConf;

  @Override
  protected void setUp() throws Exception {
    baseConf = new Configuration();
    baseConf.set("A", "1");
    baseConf.setInt("B", 2);
    baseConf.set("C", "3");
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
      .add(map);
    assertEquals("1", compoundConf.get("A"));
    assertEquals("2b", compoundConf.get("B"));
    assertEquals(33, compoundConf.getInt("C", 0));
    assertEquals("4", compoundConf.get("D"));
    assertEquals(4, compoundConf.getInt("D", 0));
    assertNull(compoundConf.get("E"));
    assertEquals(6, compoundConf.getInt("F", 6));
    assertNull(compoundConf.get("G"));
  }

}
