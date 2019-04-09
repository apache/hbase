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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, SmallTests.class })
public class TestJSONMetricUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestJSONMetricUtil.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestJSONMetricUtil.class);

  @Test
  public void testBuildHashtable() {
    String[] keys = { "type", "name" };
    String[] emptyKey = {};
    String[] values = { "MemoryPool", "Par Eden Space" };
    String[] values2 = { "MemoryPool", "Par Eden Space", "Test" };
    String[] emptyValue = {};
    Map<String, String> properties = JSONMetricUtil.buldKeyValueTable(keys, values);
    assertEquals(values[0], properties.get("type"));
    assertEquals(values[1], properties.get("name"));

    assertNull(JSONMetricUtil.buldKeyValueTable(keys, values2));
    assertNull(JSONMetricUtil.buldKeyValueTable(keys, emptyValue));
    assertNull(JSONMetricUtil.buldKeyValueTable(emptyKey, values2));
    assertNull(JSONMetricUtil.buldKeyValueTable(emptyKey, emptyValue));
  }

  @Test
  public void testBuildObjectName() throws MalformedObjectNameException {
    String[] keys = { "type", "name" };
    String[] values = { "MemoryPool", "Par Eden Space" };
    Hashtable<String, String> properties = JSONMetricUtil.buldKeyValueTable(keys, values);
    ObjectName testObject =
      JSONMetricUtil.buildObjectName(JSONMetricUtil.JAVA_LANG_DOMAIN, properties);
    assertEquals(JSONMetricUtil.JAVA_LANG_DOMAIN, testObject.getDomain());
    assertEquals(testObject.getKeyPropertyList(), properties);
  }

  @Test
  public void testGetLastGCInfo() {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean bean : gcBeans) {
      ObjectName on = bean.getObjectName();
      Object value = JSONMetricUtil.getValueFromMBean(on, "LastGcInfo");
      LOG.info("Collector Info: " + value);
      if (value != null && value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        assertNotNull(cds.get("duration"));
      }
    }
  }
}
