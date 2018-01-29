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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestTableInfoModel extends TestModelBase<TableInfoModel> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableInfoModel.class);

  private static final String TABLE = "testtable";
  private static final byte[] START_KEY = Bytes.toBytes("abracadbra");
  private static final byte[] END_KEY = Bytes.toBytes("zzyzx");
  private static final long ID = 8731042424L;
  private static final String LOCATION = "testhost:9876";

  public TestTableInfoModel() throws Exception {
    super(TableInfoModel.class);
    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><TableInfo " +
      "name=\"testtable\"><Region endKey=\"enp5eng=\" id=\"8731042424\" " +
      "location=\"testhost:9876\" " +
      "name=\"testtable,abracadbra,8731042424.ad9860f031282c46ed431d7af8f94aca.\" " +
      "startKey=\"YWJyYWNhZGJyYQ==\"/></TableInfo>";

    AS_PB =
      "Cgl0ZXN0dGFibGUSSQofdGVzdHRhYmxlLGFicmFjYWRicmEsODczMTA0MjQyNBIKYWJyYWNhZGJy" +
      "YRoFenp5engg+MSkwyAqDXRlc3Rob3N0Ojk4NzY=";

    AS_JSON =
      "{\"name\":\"testtable\",\"Region\":[{\"endKey\":\"enp5eng=\",\"id\":8731042424," +
      "\"location\":\"testhost:9876\",\"" +
      "name\":\"testtable,abracadbra,8731042424.ad9860f031282c46ed431d7af8f94aca.\",\"" +
      "startKey\":\"YWJyYWNhZGJyYQ==\"}]}";
  }

  @Override
  protected TableInfoModel buildTestModel() {
    TableInfoModel model = new TableInfoModel();
    model.setName(TABLE);
    model.add(new TableRegionModel(TABLE, ID, START_KEY, END_KEY, LOCATION));
    return model;
  }

  @Override
  protected void checkModel(TableInfoModel model) {
    assertEquals(TABLE, model.getName());
    Iterator<TableRegionModel> regions = model.getRegions().iterator();
    TableRegionModel region = regions.next();
    assertTrue(Bytes.equals(region.getStartKey(), START_KEY));
    assertTrue(Bytes.equals(region.getEndKey(), END_KEY));
    assertEquals(ID, region.getId());
    assertEquals(LOCATION, region.getLocation());
    assertFalse(regions.hasNext());
  }

  @Override
  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  @Override
  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  @Override
  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }

}

