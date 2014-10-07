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

import java.util.Iterator;

import javax.xml.bind.JAXBContext;

import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestTableSchemaModel extends TestModelBase<TableSchemaModel> {

  public static final String TABLE_NAME = "testTable";
  private static final boolean IS_META = false;
  private static final boolean IS_ROOT = false;
  private static final boolean READONLY = false;

  TestColumnSchemaModel testColumnSchemaModel;

  private JAXBContext context;

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

  protected void checkModel(TableSchemaModel model) {
    checkModel(model, TABLE_NAME);
  }

  public void checkModel(TableSchemaModel model, String tableName) {
    assertEquals(model.getName(), tableName);
    assertEquals(model.__getIsMeta(), IS_META);
    assertEquals(model.__getIsRoot(), IS_ROOT);
    assertEquals(model.__getReadOnly(), READONLY);
    Iterator<ColumnSchemaModel> families = model.getColumns().iterator();
    assertTrue(families.hasNext());
    ColumnSchemaModel family = families.next();
    testColumnSchemaModel.checkModel(family);
    assertFalse(families.hasNext());
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

