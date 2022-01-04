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
import static org.junit.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.auth.SaslExtensions;
import org.apache.hadoop.hbase.security.auth.SaslExtensionsCallback;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.Test;

public class OAuthBearerSaslClientTest {
  private static final Map<String, String> TEST_PROPERTIES = new LinkedHashMap<String, String>() {
    {
      put("One", "1");
      put("Two", "2");
      put("Three", "3");
    }
  };
  private SaslExtensions testExtensions = new SaslExtensions(TEST_PROPERTIES);
  private final String errorMessage = "Error as expected!";

  public class ExtensionsCallbackHandler implements AuthenticateCallbackHandler {
    private boolean configured = false;
    private boolean toThrow;

    ExtensionsCallbackHandler(boolean toThrow) {
      this.toThrow = toThrow;
    }

    public boolean configured() {
      return configured;
    }

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
        } else if (callback instanceof SaslExtensionsCallback) {
          if (toThrow) {
            throw new RuntimeException(errorMessage);
          } else {
            ((SaslExtensionsCallback) callback).extensions(testExtensions);
          }
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }

  @Test
  public void testAttachesExtensionsToFirstClientMessage() throws Exception {
    String expectedToken = new String(
      new OAuthBearerClientInitialResponse("", testExtensions).toBytes(), StandardCharsets.UTF_8);
    OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));
    String message = new String(client.evaluateChallenge("".getBytes(StandardCharsets.UTF_8)),
      StandardCharsets.UTF_8);
    assertEquals(expectedToken, message);
  }

  @Test
  public void testNoExtensionsDoesNotAttachAnythingToFirstClientMessage() throws Exception {
    TEST_PROPERTIES.clear();
    testExtensions = new SaslExtensions(TEST_PROPERTIES);
    String expectedToken = new String(new OAuthBearerClientInitialResponse("",
      new SaslExtensions(TEST_PROPERTIES)).toBytes(), StandardCharsets.UTF_8);
    OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(false));

    String message = new String(client.evaluateChallenge("".getBytes(StandardCharsets.UTF_8)),
      StandardCharsets.UTF_8);

    assertEquals(expectedToken, message);
  }

  @Test
  public void testWrapsExtensionsCallbackHandlingErrorInSaslExceptionInFirstClientMessage() {
    OAuthBearerSaslClient client = new OAuthBearerSaslClient(new ExtensionsCallbackHandler(true));
    try {
      client.evaluateChallenge("".getBytes(StandardCharsets.UTF_8));
      fail("Should have failed with " + SaslException.class.getName());
    } catch (SaslException e) {
      // assert it has caught our expected exception
      assertEquals(RuntimeException.class, e.getCause().getClass());
      assertEquals(errorMessage, e.getCause().getMessage());
    }
  }
}
