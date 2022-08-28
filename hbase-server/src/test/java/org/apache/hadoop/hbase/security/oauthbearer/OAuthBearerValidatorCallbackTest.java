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
package org.apache.hadoop.hbase.security.oauthbearer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class OAuthBearerValidatorCallbackTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerValidatorCallbackTest.class);

  private static final OAuthBearerToken TOKEN = new OAuthBearerToken() {
    @Override
    public String value() {
      return "value";
    }

    @Override
    public String principalName() {
      return "principalName";
    }

    @Override
    public long lifetimeMs() {
      return 0;
    }
  };

  @Test
  public void testError() {
    String errorStatus = "errorStatus";
    String errorScope = "errorScope";
    String errorOpenIDConfiguration = "errorOpenIDConfiguration";
    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(TOKEN.value());
    callback.error(errorStatus, errorScope, errorOpenIDConfiguration);
    assertEquals(errorStatus, callback.errorStatus());
    assertEquals(errorScope, callback.errorScope());
    assertEquals(errorOpenIDConfiguration, callback.errorOpenIDConfiguration());
    assertNull(callback.token());
  }

  @Test
  public void testToken() {
    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(TOKEN.value());
    callback.token(TOKEN);
    assertSame(TOKEN, callback.token());
    assertNull(callback.errorStatus());
    assertNull(callback.errorScope());
    assertNull(callback.errorOpenIDConfiguration());
  }
}
