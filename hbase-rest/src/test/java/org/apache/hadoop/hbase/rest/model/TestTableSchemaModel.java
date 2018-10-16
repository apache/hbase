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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RestTests.class, SmallTests.class})
public class TestTableSchemaModel extends TestModelBase<TableSchemaModel> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableSchemaModel.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTableSchemaModel.class);

  public static final String TABLE_NAME = "testTable";
  private static final boolean IS_META = false;
  private static final boolean IS_ROOT = false;
  private static final boolean READONLY = false;

  TestColumnSchemaModel testColumnSchemaModel;

  public TestTableSchemaModel() throws Exception {
    super(TableSchemaModel.class);
    testColumnSchemaModel = new TestColumnSchemaModel();

    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
      "<TableSchema name=\"testTable\" IS_META=\"false\" IS_ROOT=\"false\" READONLY=\"false\">" +
      "<ColumnSchema name=\"testcolumn\" BLOCKSIZE=\"16384\" BLOOMFILTER=\"NONE\" " +
      "BLOCKCACHE=\"true\" COMPRESSION=\"GZ\" VERSIONS=\"1\" TTL=\"86400\" IN_MEMORY=\"false\"/>" +
      "</TableSchema>";

    AS_PB =
      "Cgl0ZXN0VGFibGUSEAoHSVNfTUVUQRIFZmFsc2USEAoHSVNfUk9PVBIFZmFsc2USEQoIUkVBRE9O" +
      "TFkSBWZhbHNlGpcBCgp0ZXN0Y29sdW1uEhIKCUJMT0NLU0laRRIFMTYzODQSEwoLQkxPT01GSUxU" +
      "RVISBE5PTkUSEgoKQkxPQ0tDQUNIRRIEdHJ1ZRIRCgtDT01QUkVTU0lPThICR1oSDQoIVkVSU0lP" +
      "TlMSATESDAoDVFRMEgU4NjQwMBISCglJTl9NRU1PUlkSBWZhbHNlGICjBSABKgJHWigA";

    AS_JSON =
      "{\"name\":\"testTable\",\"IS_META\":\"false\",\"IS_ROOT\":\"false\"," +
      "\"READONLY\":\"false\",\"ColumnSchema\":[{\"name\":\"testcolumn\"," +
      "\"BLOCKSIZE\":\"16384\",\"BLOOMFILTER\":\"NONE\",\"BLOCKCACHE\":\"true\"," +
      "\"COMPRESSION\":\"GZ\",\"VERSIONS\":\"1\",\"TTL\":\"86400\",\"IN_MEMORY\":\"false\"}]}";
  }

  @Override
  protected TableSchemaModel buildTestModel() {
    return buildTestModel(TABLE_NAME);
  }

  public TableSchemaModel buildTestModel(String name) {
    TableSchemaModel model = new TableSchemaModel();
    model.setName(name);
    model.__setIsMeta(IS_META);
    model.__setIsRoot(IS_ROOT);
    model.__setReadOnly(READONLY);
    model.addColumnFamily(testColumnSchemaModel.buildTestModel());
    return model;
  }

  @Override
  protected void checkModel(TableSchemaModel model) {
    checkModel(model, TABLE_NAME);
  }

  public void checkModel(TableSchemaModel model, String tableName) {
    assertEquals(model.getName(), tableName);
    assertEquals(IS_META, model.__getIsMeta());
    assertEquals(IS_ROOT, model.__getIsRoot());
    assertEquals(READONLY, model.__getReadOnly());
    Iterator<ColumnSchemaModel> families = model.getColumns().iterator();
    assertTrue(families.hasNext());
    ColumnSchemaModel family = families.next();
    testColumnSchemaModel.checkModel(family);
    assertFalse(families.hasNext());
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

