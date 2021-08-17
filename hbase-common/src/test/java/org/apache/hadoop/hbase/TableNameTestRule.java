/*
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
package org.apache.hadoop.hbase;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Returns a {@code TableName} based on currently running test method name. Supports
 *  tests built on the {@link org.junit.runners.Parameterized} runner.
 */
public class TableNameTestRule extends TestWatcher {

  private TableName tableName;

  @Override
  protected void starting(Description description) {
    tableName = TableName.valueOf(cleanUpTestName(description.getMethodName()));
  }

  /**
   * Helper to handle parameterized method names. Unlike regular test methods, parameterized method
   * names look like 'foo[x]'. This is problematic for tests that use this name for HBase tables.
   * This helper strips out the parameter suffixes.
   * @return current test method name with out parameterized suffixes.
   */
  public static String cleanUpTestName(String methodName) {
    int index = methodName.indexOf('[');
    if (index == -1) {
      return methodName;
    }
    return methodName.substring(0, index);
  }

  public TableName getTableName() {
    return tableName;
  }
}
