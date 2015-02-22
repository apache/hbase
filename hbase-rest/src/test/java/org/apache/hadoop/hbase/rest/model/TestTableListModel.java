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

import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestTableListModel extends TestModelBase<TableListModel> {
  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";
  private static final String TABLE3 = "table3";

  public TestTableListModel() throws Exception {
    super(TableListModel.class);
    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><TableList><table " +
          "name=\"table1\"/><table name=\"table2\"/><table name=\"table3\"/></TableList>";

    AS_PB = "CgZ0YWJsZTEKBnRhYmxlMgoGdGFibGUz";

    AS_JSON =
      "{\"table\":[{\"name\":\"table1\"},{\"name\":\"table2\"},{\"name\":\"table3\"}]}";
  }

  protected TableListModel buildTestModel() {
    TableListModel model = new TableListModel();
    model.add(new TableModel(TABLE1));
    model.add(new TableModel(TABLE2));
    model.add(new TableModel(TABLE3));
    return model;
  }

  protected void checkModel(TableListModel model) {
    Iterator<TableModel> tables = model.getTables().iterator();
    TableModel table = tables.next();
    assertEquals(table.getName(), TABLE1);
    table = tables.next();
    assertEquals(table.getName(), TABLE2);
    table = tables.next();
    assertEquals(table.getName(), TABLE3);
    assertFalse(tables.hasNext());
  }
}

