/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category({ SmallTests.class })
public class TestConfigurationUtil {

  private Configuration conf;
  private Map<String, String> keyValues;
  private String key;

  @Before
  public void setUp() throws Exception {
    this.conf = new Configuration();
    this.keyValues = ImmutableMap.of("k1", "v1", "k2", "v2");
    this.key = "my_conf_key";
  }

  public void callSetKeyValues() {
    ConfigurationUtil.setKeyValues(conf, key, keyValues.entrySet());
  }

  public List<Map.Entry<String, String>> callGetKeyValues() {
    return ConfigurationUtil.getKeyValues(conf, key);
  }

  @Test
  public void testGetAndSetKeyValuesWithValues() throws Exception {
    callSetKeyValues();
    assertEquals(Lists.newArrayList(this.keyValues.entrySet()), callGetKeyValues());
  }

  @Test
  public void testGetKeyValuesWithUnsetKey() throws Exception {
    assertNull(callGetKeyValues());
  }

}
