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
package org.apache.hadoop.hbase.quotas.policies;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link DisableTableViolationPolicyEnforcement}.
 */
@Category(SmallTests.class)
public class TestDisableTableViolationPolicyEnforcement extends BaseViolationPolicyEnforcement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDisableTableViolationPolicyEnforcement.class);

  private DisableTableViolationPolicyEnforcement enforcement;

  @Before
  public void setup() {
    enforcement = new DisableTableViolationPolicyEnforcement();
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckPut() throws SpaceLimitingException {
    // If the policy is enacted, it will always throw an exception
    // to avoid constantly re-checking the table state.
    enforcement.check(PUT);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckAppend() throws SpaceLimitingException {
    enforcement.check(APPEND);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckDelete() throws SpaceLimitingException {
    enforcement.check(DELETE);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckIncrement() throws SpaceLimitingException {
    enforcement.check(INCREMENT);
  }
}
