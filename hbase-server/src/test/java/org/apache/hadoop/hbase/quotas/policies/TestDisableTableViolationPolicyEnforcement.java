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
package org.apache.hadoop.hbase.quotas.policies;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link DisableTableViolationPolicyEnforcement}.
 */
@Tag(SmallTests.TAG)
public class TestDisableTableViolationPolicyEnforcement extends BaseViolationPolicyEnforcement {

  private DisableTableViolationPolicyEnforcement enforcement;

  @BeforeEach
  public void setup() {
    enforcement = new DisableTableViolationPolicyEnforcement();
  }

  @Test
  public void testCheckPut() throws SpaceLimitingException {
    // If the policy is enacted, it will always throw an exception
    // to avoid constantly re-checking the table state.
    assertThrows(SpaceLimitingException.class, () -> enforcement.check(PUT));
  }

  @Test
  public void testCheckAppend() throws SpaceLimitingException {
    assertThrows(SpaceLimitingException.class, () -> enforcement.check(APPEND));
  }

  @Test
  public void testCheckDelete() throws SpaceLimitingException {
    assertThrows(SpaceLimitingException.class, () -> enforcement.check(DELETE));
  }

  @Test
  public void testCheckIncrement() throws SpaceLimitingException {
    assertThrows(SpaceLimitingException.class, () -> enforcement.check(INCREMENT));
  }
}
