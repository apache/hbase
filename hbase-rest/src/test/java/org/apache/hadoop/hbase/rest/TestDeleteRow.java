/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.rest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.rest.client.Response;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MediumTests.class)
public class TestDeleteRow extends RowResourceBase {

  @Test
  public void testDeleteNonExistentColumn() throws Exception {
    Response response = putValueJson(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);

    response = checkAndDeleteJson(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(304, response.getCode());
    assertEquals(200, getValueJson(TABLE, ROW_1, COLUMN_1).getCode());

    response = checkAndDeleteJson(TABLE, ROW_2, COLUMN_1, VALUE_2);
    assertEquals(304, response.getCode());
    assertEquals(200, getValueJson(TABLE, ROW_1, COLUMN_1).getCode());

    response = checkAndDeleteJson(TABLE, ROW_1, "dummy", VALUE_1);
    assertEquals(400, response.getCode());
    assertEquals(200, getValueJson(TABLE, ROW_1, COLUMN_1).getCode());

    response = checkAndDeleteJson(TABLE, ROW_1, "dummy:test", VALUE_1);
    assertEquals(404, response.getCode());
    assertEquals(200, getValueJson(TABLE, ROW_1, COLUMN_1).getCode());

    response = checkAndDeleteJson(TABLE, ROW_1, "a:test", VALUE_1);
    assertEquals(304, response.getCode());
    assertEquals(200, getValueJson(TABLE, ROW_1, COLUMN_1).getCode());
  }

  @Test
  public void testDeleteXML() throws IOException, JAXBException {
    Response response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = putValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 200);
    response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = checkAndDeletePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);
    response = getValueXML(TABLE, ROW_1, COLUMN_2);
    assertEquals(response.getCode(), 404);

    //Delete a row in non existent table
    response = deleteValue("dummy", ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);

    //Delete non existent column
    response = deleteValue(TABLE, ROW_1, "dummy");
    assertEquals(response.getCode(), 404);
  }

}
