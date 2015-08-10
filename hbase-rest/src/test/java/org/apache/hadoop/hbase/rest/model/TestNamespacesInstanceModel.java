/*
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

package org.apache.hadoop.hbase.rest.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestNamespacesInstanceModel extends TestModelBase<NamespacesInstanceModel> {

  public static final Map<String,String> NAMESPACE_PROPERTIES = new HashMap<String, String>();
  public static final String NAMESPACE_NAME = "namespaceName";

  public TestNamespacesInstanceModel() throws Exception {
    super(NamespacesInstanceModel.class);

    NAMESPACE_PROPERTIES.put("KEY_1","VALUE_1");
    NAMESPACE_PROPERTIES.put("KEY_2","VALUE_2");
    NAMESPACE_PROPERTIES.put("NAME","testNamespace");

    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
      "<NamespaceProperties><properties><entry><key>NAME</key><value>testNamespace" +
      "</value></entry><entry><key>KEY_2</key><value>VALUE_2" +
      "</value></entry><entry><key>KEY_1</key><value>VALUE_1</value></entry>" +
      "</properties></NamespaceProperties>";

    AS_PB = "ChUKBE5BTUUSDXRlc3ROYW1lc3BhY2UKEAoFS0VZXzESB1ZBTFVFXzEKEAoFS0VZXzISB1ZBTFVFXzI=";

    AS_JSON = "{\"properties\":{\"NAME\":\"testNamespace\"," +
      "\"KEY_1\":\"VALUE_1\",\"KEY_2\":\"VALUE_2\"}}";
  }

  protected NamespacesInstanceModel buildTestModel() {
    return buildTestModel(NAMESPACE_NAME, NAMESPACE_PROPERTIES);
  }

  public NamespacesInstanceModel buildTestModel(String namespace, Map<String,String> properties) {
    NamespacesInstanceModel model = new NamespacesInstanceModel();
    for(String key: properties.keySet()){
      model.addProperty(key, properties.get(key));
    }
    return model;
  }

  protected void checkModel(NamespacesInstanceModel model) {
    checkModel(model, NAMESPACE_NAME, NAMESPACE_PROPERTIES);
  }

  public void checkModel(NamespacesInstanceModel model, String namespace,
      Map<String,String> properties) {
    Map<String,String> modProperties = model.getProperties();
    assertEquals(properties.size(), modProperties.size());
    // Namespace name comes from REST URI, not properties.
    assertNotSame(namespace, model.getNamespaceName());
    for(String property: properties.keySet()){
      assertEquals(properties.get(property), modProperties.get(property));
    }
  }

  @Test
  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  @Test
  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  @Test
  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }
}

