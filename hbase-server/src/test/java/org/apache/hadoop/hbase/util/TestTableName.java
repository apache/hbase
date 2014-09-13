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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Returns a {@code byte[]} containing the name of the currently running test method.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestTableName extends TestWatcher {
  private TableName tableName;

  /**
   * Invoked when a test is about to start
   */
  @Override
  protected void starting(Description description) {
    tableName = TableName.valueOf(description.getMethodName());
  }

  public TableName getTableName() {
    return tableName;
  }
  
  String emptyTableNames[] ={"", " "};
  String invalidNamespace[] = {":a", "%:a"};
  String legalTableNames[] = { "foo", "with-dash_under.dot", "_under_start_ok",
      "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02"
      , "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
      "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02"};
  String illegalTableNames[] = { ".dot_start_illegal", "-dash_start_illegal", "spaces not ok",
      "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
      "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2"};


  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespace() {
    for (String tn : invalidNamespace) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
      fail("invalid namespace " + tn + " should have failed with IllegalArgumentException for namespace");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTableName() {
    for (String tn : emptyTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
      fail("invalid tablename " + tn + " should have failed with IllegalArgumentException");
    }
  }

  @Test
  public void testLegalHTableNames() {
    for (String tn : legalTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
    }
  }

  @Test
  public void testIllegalHTableNames() {
    for (String tn : illegalTableNames) {
      try {
        TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
        fail("invalid tablename " + tn + " should have failed");
      } catch (Exception e) {
        // expected
      }
    }
  }
  
}
