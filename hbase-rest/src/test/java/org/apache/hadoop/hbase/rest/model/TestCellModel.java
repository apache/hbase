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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestCellModel extends TestModelBase<CellModel> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellModel.class);

  private static final long TIMESTAMP = 1245219839331L;
  private static final byte[] COLUMN = Bytes.toBytes("testcolumn");
  private static final byte[] VALUE = Bytes.toBytes("testvalue");

  public TestCellModel() throws Exception {
    super(CellModel.class);
    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Cell " +
          "column=\"dGVzdGNvbHVtbg==\" timestamp=\"1245219839331\">dGVzdHZhbHVl</Cell>";
    AS_PB =
      "Egp0ZXN0Y29sdW1uGOO6i+eeJCIJdGVzdHZhbHVl";

    AS_JSON =
      "{\"column\":\"dGVzdGNvbHVtbg==\",\"timestamp\":1245219839331,\"$\":\"dGVzdHZhbHVl\"}";
  }

  @Override
  protected CellModel buildTestModel() {
    CellModel model = new CellModel();
    model.setColumn(COLUMN);
    model.setTimestamp(TIMESTAMP);
    model.setValue(VALUE);
    return model;
  }

  @Override
  protected void checkModel(CellModel model) {
    assertTrue(Bytes.equals(model.getColumn(), COLUMN));
    assertTrue(Bytes.equals(model.getValue(), VALUE));
    assertTrue(model.hasUserTimestamp());
    assertEquals(TIMESTAMP, model.getTimestamp());
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

  @Test
  public void testEquals() throws Exception {
    CellModel cellModel1 = buildTestModel();
    CellModel cellModel2 = buildTestModel();

    assertEquals(cellModel1, cellModel2);

    CellModel cellModel3 = new CellModel();
    assertFalse(cellModel1.equals(cellModel3));
  }

  @Test
  public void testToString() throws Exception {
    String expectedColumn = ToStringBuilder.reflectionToString(COLUMN, ToStringStyle.SIMPLE_STYLE);

    CellModel cellModel = buildTestModel();
    System.out.println(cellModel);

    assertTrue(StringUtils.contains(cellModel.toString(), expectedColumn));
  }
}

