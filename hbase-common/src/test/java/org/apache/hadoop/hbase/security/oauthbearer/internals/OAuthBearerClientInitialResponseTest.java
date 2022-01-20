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
package org.apache.hadoop.hbase.security.oauthbearer.internals;

import static org.junit.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class OAuthBearerClientInitialResponseTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerClientInitialResponseTest.class);

  /*
      Test how a client would build a response
   */
  @Test
  public void testBuildClientResponseToBytes() {
    String expectedMesssage = "n,,\u0001auth=Bearer 123.345.567\u0001\u0001";

    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse("123.345.567");

    String message = new String(response.toBytes(), StandardCharsets.UTF_8);

    assertEquals(expectedMesssage, message);
  }

  @Test
  public void testBuildServerResponseToBytes() throws Exception {
    String serverMessage = "n,,\u0001auth=Bearer 123.345.567\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(serverMessage.getBytes(StandardCharsets.UTF_8));

    String message = new String(response.toBytes(), StandardCharsets.UTF_8);

    assertEquals(serverMessage, message);
  }

  @Test
  public void testToken() throws Exception {
    String message = "n,,\u0001auth=Bearer 123.345.567\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(message.getBytes(StandardCharsets.UTF_8));
    assertEquals("123.345.567", response.tokenValue());
    assertEquals("", response.authorizationId());
  }

  @Test
  public void testAuthorizationId() throws Exception {
    String message = "n,a=myuser,\u0001auth=Bearer 345\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(message.getBytes(StandardCharsets.UTF_8));
    assertEquals("345", response.tokenValue());
    assertEquals("myuser", response.authorizationId());
  }

  @Test
  public void testExtensions() throws Exception {
    String message =
      "n,,\u0001propA=valueA1, valueA2\u0001auth=Bearer 567\u0001propB=valueB\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(message.getBytes(StandardCharsets.UTF_8));
    assertEquals("567", response.tokenValue());
    assertEquals("", response.authorizationId());
  }

  // The example in the RFC uses `vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==` as the token
  // But since we use Base64Url encoding, padding is omitted. Hence this test verifies without '='.
  @Test
  public void testRfc7688Example() throws Exception {
    String message = "n,a=user@example.com,\u0001host=server.example.com\u0001port=143\u0001" +
      "auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(message.getBytes(StandardCharsets.UTF_8));
    assertEquals("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", response.tokenValue());
    assertEquals("user@example.com", response.authorizationId());
  }

  @Test
  public void testNoExtensionsFromByteArray() throws Exception {
    String message = "n,a=user@example.com,\u0001" +
      "auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001";
    OAuthBearerClientInitialResponse response =
      new OAuthBearerClientInitialResponse(message.getBytes(StandardCharsets.UTF_8));
    assertEquals("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", response.tokenValue());
    assertEquals("user@example.com", response.authorizationId());
  }
}
