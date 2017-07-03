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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestJSONMetricUtil {

  private static final Log LOG = LogFactory.getLog(TestJSONMetricUtil.class);

  @Test
  public void testBuildHashtable() {
    String[] keys = {"type", "name"};
    String[] emptyKey = {};
    String[] values = {"MemoryPool", "Par Eden Space"};
    String[] values2 = {"MemoryPool", "Par Eden Space", "Test"};
    String[] emptyValue = {};
    Hashtable<String, String> properties = JSONMetricUtil.buldKeyValueTable(keys, values);
    Hashtable<String, String> nullObject = JSONMetricUtil.buldKeyValueTable(keys, values2);
    Hashtable<String, String> nullObject1 = JSONMetricUtil.buldKeyValueTable(keys, emptyValue);
    Hashtable<String, String> nullObject2 = JSONMetricUtil.buldKeyValueTable(emptyKey, values2);
    Hashtable<String, String> nullObject3 = JSONMetricUtil.buldKeyValueTable(emptyKey, emptyValue);
    assertEquals(properties.get("type"), values[0]);
    assertEquals(properties.get("name"), values[1]);
    assertEquals(nullObject, null);
    assertEquals(nullObject1, null);
    assertEquals(nullObject2, null);
    assertEquals(nullObject3, null);
  }

  @Test
  public void testSearchJson() throws JsonProcessingException, IOException {
    String jsonString = "{\"test\":[{\"data1\":100,\"data2\":\"hello\",\"data3\": [1 , 2 , 3]}, "
        + "{\"data4\":0}]}";
    JsonNode  node = JSONMetricUtil.mappStringToJsonNode(jsonString);
    JsonNode r1 = JSONMetricUtil.searchJson(node, "data1");
    JsonNode r2 = JSONMetricUtil.searchJson(node, "data2");
    JsonNode r3 = JSONMetricUtil.searchJson(node, "data3");
    JsonNode r4 = JSONMetricUtil.searchJson(node, "data4");
    assertEquals(r1.getIntValue(), 100);
    assertEquals(r2.getTextValue(), "hello");
    assertEquals(r3.get(0).getIntValue(), 1);
    assertEquals(r4.getIntValue(), 0);
  }

  @Test
  public void testBuildObjectName() throws MalformedObjectNameException {
    String[] keys = {"type", "name"};
    String[] values = {"MemoryPool", "Par Eden Space"};
    Hashtable<String, String> properties = JSONMetricUtil.buldKeyValueTable(keys, values);
    ObjectName testObject = JSONMetricUtil.buildObjectName(JSONMetricUtil.JAVA_LANG_DOMAIN,
      properties);
    assertEquals(testObject.getDomain(), JSONMetricUtil.JAVA_LANG_DOMAIN);
    assertEquals(testObject.getKeyPropertyList(), properties);
  }

  @Test
  public void testGetLastGCInfo() {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for(GarbageCollectorMXBean bean:gcBeans) {
      ObjectName on = bean.getObjectName();
      Object value = JSONMetricUtil.getValueFromMBean(on, "LastGcInfo");
      LOG.info("Collector Info: "+ value);
      if (value != null && value instanceof CompositeData) {
        CompositeData cds = (CompositeData)value;
        assertNotNull(cds.get("duration"));
      }
    }
  }
}
