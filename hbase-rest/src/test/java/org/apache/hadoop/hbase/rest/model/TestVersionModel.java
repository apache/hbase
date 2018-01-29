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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestVersionModel extends TestModelBase<VersionModel> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVersionModel.class);

  private static final String REST_VERSION = "0.0.1";
  private static final String OS_VERSION =
    "Linux 2.6.18-128.1.6.el5.centos.plusxen amd64";
  private static final String JVM_VERSION =
    "Sun Microsystems Inc. 1.6.0_13-11.3-b02";
  private static final String JETTY_VERSION = "6.1.14";
  private static final String JERSEY_VERSION = "1.1.0-ea";

  public TestVersionModel() throws Exception {
    super(VersionModel.class);
    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Version JVM=\"Sun " +
          "Microsystems Inc. 1.6.0_13-11.3-b02\" Jersey=\"1.1.0-ea\" " +
          "OS=\"Linux 2.6.18-128.1.6.el5.centos.plusxen amd64\" REST=\"0.0.1\" Server=\"6.1.14\"/>";

    AS_PB =
      "CgUwLjAuMRInU3VuIE1pY3Jvc3lzdGVtcyBJbmMuIDEuNi4wXzEzLTExLjMtYjAyGi1MaW51eCAy" +
      "LjYuMTgtMTI4LjEuNi5lbDUuY2VudG9zLnBsdXN4ZW4gYW1kNjQiBjYuMS4xNCoIMS4xLjAtZWE=";

    AS_JSON =
      "{\"JVM\":\"Sun Microsystems Inc. 1.6.0_13-11.3-b02\",\"Jersey\":\"1.1.0-ea\"," +
          "\"OS\":\"Linux 2.6.18-128.1.6.el5.centos.plusxen amd64\",\"" +
          "REST\":\"0.0.1\",\"Server\":\"6.1.14\"}";
  }

  @Override
  protected VersionModel buildTestModel() {
    VersionModel model = new VersionModel();
    model.setRESTVersion(REST_VERSION);
    model.setOSVersion(OS_VERSION);
    model.setJVMVersion(JVM_VERSION);
    model.setServerVersion(JETTY_VERSION);
    model.setJerseyVersion(JERSEY_VERSION);
    return model;
  }

  @Override
  protected void checkModel(VersionModel model) {
    assertEquals(REST_VERSION, model.getRESTVersion());
    assertEquals(OS_VERSION, model.getOSVersion());
    assertEquals(JVM_VERSION, model.getJVMVersion());
    assertEquals(JETTY_VERSION, model.getServerVersion());
    assertEquals(JERSEY_VERSION, model.getJerseyVersion());
  }
}

