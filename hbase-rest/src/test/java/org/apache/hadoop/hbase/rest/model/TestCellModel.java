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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCellModel extends TestModelBase<CellModel> {

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

  protected CellModel buildTestModel() {
    CellModel model = new CellModel();
    model.setColumn(COLUMN);
    model.setTimestamp(TIMESTAMP);
    model.setValue(VALUE);
    return model;
  }

  protected void checkModel(CellModel model) {
    assertTrue(Bytes.equals(model.getColumn(), COLUMN));
    assertTrue(Bytes.equals(model.getValue(), VALUE));
    assertTrue(model.hasUserTimestamp());
    assertEquals(model.getTimestamp(), TIMESTAMP);
  }

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }

}

