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
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class OAuthBearerSaslClientTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerSaslClientTest.class);

  public static class ExtensionsCallbackHandler implements AuthenticateCallbackHandler {

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof OAuthBearerTokenCallback) {
          ((OAuthBearerTokenCallback) callback).token(new OAuthBearerToken() {
            @Override public String value() {
              return "";
            }

            @Override public long lifetimeMs() {
              return 100;
            }

            @Override public String principalName() {
              return "principalName";
            }
          });
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }

  @Test
  public void testAttachesExtensionsToFirstClientMessage() throws Exception {
    String expectedToken = new String(
      new OAuthBearerClientInitialResponse("").toBytes(), StandardCharsets.UTF_8);
    OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler());
    String message = new String(client.evaluateChallenge("".getBytes(StandardCharsets.UTF_8)),
      StandardCharsets.UTF_8);
    assertEquals(expectedToken, message);
  }

}
