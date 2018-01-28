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
 * Test class for {@link NoInsertsViolationPolicyEnforcement}.
 */
@Category(SmallTests.class)
public class TestNoInsertsViolationPolicyEnforcement extends BaseViolationPolicyEnforcement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNoInsertsViolationPolicyEnforcement.class);

  private NoInsertsViolationPolicyEnforcement enforcement;

  @Before
  public void setup() {
    enforcement = new NoInsertsViolationPolicyEnforcement();
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckAppend() throws Exception {
    enforcement.check(APPEND);
  }

  @Test
  public void testCheckDelete() throws Exception {
    enforcement.check(DELETE);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckIncrement() throws Exception {
    enforcement.check(INCREMENT);
  }

  @Test(expected = SpaceLimitingException.class)
  public void testCheckPut() throws Exception {
    enforcement.check(PUT);
  }
}
