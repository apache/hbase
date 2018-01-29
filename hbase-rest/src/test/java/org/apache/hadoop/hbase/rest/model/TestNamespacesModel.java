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
package org.apache.hadoop.hbase.rest.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestNamespacesModel extends TestModelBase<NamespacesModel> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespacesModel.class);

  public static final String NAMESPACE_NAME_1 = "testNamespace1";
  public static final String NAMESPACE_NAME_2 = "testNamespace2";

  public TestNamespacesModel() throws Exception {
    super(NamespacesModel.class);

    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
      "<Namespaces><Namespace>testNamespace1</Namespace>" +
      "<Namespace>testNamespace2</Namespace></Namespaces>";

    AS_PB = "Cg50ZXN0TmFtZXNwYWNlMQoOdGVzdE5hbWVzcGFjZTI=";

    AS_JSON = "{\"Namespace\":[\"testNamespace1\",\"testNamespace2\"]}";
  }

  @Override
  protected NamespacesModel buildTestModel() {
    return buildTestModel(NAMESPACE_NAME_1, NAMESPACE_NAME_2);
  }

  public NamespacesModel buildTestModel(String... namespaces) {
    NamespacesModel model = new NamespacesModel();
    model.setNamespaces(Arrays.asList(namespaces));
    return model;
  }

  @Override
  protected void checkModel(NamespacesModel model) {
    checkModel(model, NAMESPACE_NAME_1, NAMESPACE_NAME_2);
  }

  public void checkModel(NamespacesModel model, String... namespaceName) {
    List<String> namespaces = model.getNamespaces();
    assertEquals(namespaceName.length, namespaces.size());
    for(int i = 0; i < namespaceName.length; i++){
      assertTrue(namespaces.contains(namespaceName[i]));
    }
  }

  @Override
  @Test
  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  @Override
  @Test
  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  @Override
  @Test
  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }
}
