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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.reflect.TypeToken;
import org.apache.hbase.thirdparty.com.google.gson.Gson;

/**
 * Test {@link JSONBean}.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestJSONBean {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestJSONBean.class);

  private MBeanServer getMockMBeanServer() throws Exception {
    MBeanServer mbeanServer = mock(MBeanServer.class);
    Set<ObjectName> names = new HashSet<>();
    names.add(new ObjectName("test1:type=test2"));
    when(mbeanServer.queryNames(any(), any())).thenReturn(names);
    MBeanInfo mbeanInfo = mock(MBeanInfo.class);
    when(mbeanInfo.getClassName()).thenReturn("testClassName");
    String[] attributeNames = new String[] {"intAttr", "nanAttr", "infinityAttr",
      "strAttr", "boolAttr", "test:Attr"};
    MBeanAttributeInfo[] attributeInfos = new MBeanAttributeInfo[attributeNames.length];
    for (int i = 0; i < attributeInfos.length; i++) {
      attributeInfos[i] = new MBeanAttributeInfo(attributeNames[i],
        null,
        null,
        true,
        false,
        false);
    }
    when(mbeanInfo.getAttributes()).thenReturn(attributeInfos);
    when(mbeanServer.getMBeanInfo(any())).thenReturn(mbeanInfo);
    when(mbeanServer.getAttribute(any(), eq("intAttr"))).thenReturn(3);
    when(mbeanServer.getAttribute(any(), eq("nanAttr"))).thenReturn(Double.NaN);
    when(mbeanServer.getAttribute(any(), eq("infinityAttr"))).
      thenReturn(Double.POSITIVE_INFINITY);
    when(mbeanServer.getAttribute(any(), eq("strAttr"))).thenReturn("aString");
    when(mbeanServer.getAttribute(any(), eq("boolAttr"))).thenReturn(true);
    when(mbeanServer.getAttribute(any(), eq("test:Attr"))).thenReturn("aString");
    return mbeanServer;
  }

  private String getExpectedJSON() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("{");
    pw.println("  \"beans\": [");
    pw.println("    {");
    pw.println("      \"name\": \"test1:type=test2\",");
    pw.println("      \"modelerType\": \"testClassName\",");
    pw.println("      \"intAttr\": 3,");
    pw.println("      \"nanAttr\": \"NaN\",");
    pw.println("      \"infinityAttr\": \"Infinity\",");
    pw.println("      \"strAttr\": \"aString\",");
    pw.println("      \"boolAttr\": true,");
    pw.println("      \"test:Attr\": aString");
    pw.println("    }");
    pw.println("  ]");
    pw.print("}");
    return sw.toString();
  }

  @Test
  public void testJSONBeanValueTypes() throws Exception {
    JSONBean bean = new JSONBean();
    StringWriter stringWriter = new StringWriter();
    try (
      PrintWriter printWriter = new PrintWriter(stringWriter);
      JSONBean.Writer jsonWriter = bean.open(printWriter)) {
      jsonWriter.write(getMockMBeanServer(), null, null, false);
    }

    final Gson gson = GsonUtil.createGson().create();
    Type typeOfHashMap = new TypeToken<Map<String, Object>>() {}.getType();
    Map<String, Object> expectedJson = gson.fromJson(getExpectedJSON(), typeOfHashMap);
    Map<String, Object> actualJson = gson.fromJson(stringWriter.toString(), typeOfHashMap);
    assertEquals(expectedJson, actualJson);
  }
}
