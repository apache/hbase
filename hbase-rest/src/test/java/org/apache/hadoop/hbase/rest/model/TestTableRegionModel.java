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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestTableRegionModel extends TestModelBase<TableRegionModel> {
  private static final String TABLE = "testtable";
  private static final byte[] START_KEY = Bytes.toBytes("abracadbra");
  private static final byte[] END_KEY = Bytes.toBytes("zzyzx");
  private static final long ID = 8731042424L;
  private static final String LOCATION = "testhost:9876";

  public TestTableRegionModel() throws Exception {
    super(TableRegionModel.class);

    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Region endKey=\"enp5eng=\" " +
          "id=\"8731042424\" location=\"testhost:9876\" " +
          "name=\"testtable,abracadbra,8731042424.ad9860f031282c46ed431d7af8f94aca.\" " +
          "startKey=\"YWJyYWNhZGJyYQ==\"/>";

    AS_JSON =
      "{\"endKey\":\"enp5eng=\",\"id\":8731042424,\"location\":\"testhost:9876\"," +
          "\"name\":\"testtable,abracadbra,8731042424.ad9860f031282c46ed431d7af8f94aca.\",\"" +
          "startKey\":\"YWJyYWNhZGJyYQ==\"}";
  }

  protected TableRegionModel buildTestModel() {
    TableRegionModel model =
      new TableRegionModel(TABLE, ID, START_KEY, END_KEY, LOCATION);
    return model;
  }

  protected void checkModel(TableRegionModel model) {
    assertTrue(Bytes.equals(model.getStartKey(), START_KEY));
    assertTrue(Bytes.equals(model.getEndKey(), END_KEY));
    assertEquals(model.getId(), ID);
    assertEquals(model.getLocation(), LOCATION);
    assertEquals(model.getName(), 
      TABLE + "," + Bytes.toString(START_KEY) + "," + Long.toString(ID) +
      ".ad9860f031282c46ed431d7af8f94aca.");
  }

  public void testGetName() {
    TableRegionModel model = buildTestModel();
    String modelName = model.getName();
    HRegionInfo hri = new HRegionInfo(TableName.valueOf(TABLE),
      START_KEY, END_KEY, false, ID);
    assertEquals(modelName, hri.getRegionNameAsString());
  }

  public void testSetName() {
    TableRegionModel model = buildTestModel();
    String name = model.getName();
    model.setName(name);
    assertEquals(name, model.getName());
  }

  @Override
  public void testFromPB() throws Exception {
    //no pb ignore
  }
}

