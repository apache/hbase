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
package org.apache.hadoop.hbase.security.provider;

import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.OAUTHBEARER_MECHANISM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class OAuthBearerSaslClientCallbackHandlerTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerSaslClientCallbackHandlerTest.class);

  private static OAuthBearerToken createTokenWithLifetimeMillis(final long lifetimeMillis) {
    return new OAuthBearerToken() {
      @Override
      public String value() {
        return null;
      }

      @Override
      public String principalName() {
        return null;
      }

      @Override
      public long lifetimeMs() {
        return lifetimeMillis;
      }
    };
  }

  @Test
  public void testWithZeroTokens() {
    OAuthBearerSaslClientAuthenticationProvider.OAuthBearerSaslClientCallbackHandler handler =
      createCallbackHandler();
    PrivilegedActionException e =
      assertThrows(PrivilegedActionException.class, () -> Subject.doAs(new Subject(),
      (PrivilegedExceptionAction<Void>) () -> {
          OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
          handler.handle(new Callback[] {callback});
          return null;
        }
    ));
    assertEquals(IOException.class, e.getCause().getClass());
  }

  @Test
  public void testWithPotentiallyMultipleTokens() throws Exception {
    OAuthBearerSaslClientAuthenticationProvider.OAuthBearerSaslClientCallbackHandler handler =
      createCallbackHandler();
    Subject.doAs(new Subject(), (PrivilegedExceptionAction<Void>) () -> {
      final int maxTokens = 4;
      final Set<Object> privateCredentials = Subject.getSubject(AccessController.getContext())
        .getPrivateCredentials();
      privateCredentials.clear();
      for (int num = 1; num <= maxTokens; ++num) {
        privateCredentials.add(createTokenWithLifetimeMillis(num));
        privateCredentials.add(createTokenWithLifetimeMillis(-num));
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});
        assertEquals(num, callback.token().lifetimeMs());
      }
      return null;
    });
  }

  private static OAuthBearerSaslClientAuthenticationProvider.OAuthBearerSaslClientCallbackHandler
    createCallbackHandler() {
    OAuthBearerSaslClientAuthenticationProvider.OAuthBearerSaslClientCallbackHandler handler =
      new OAuthBearerSaslClientAuthenticationProvider.OAuthBearerSaslClientCallbackHandler();
    handler.configure(new Configuration(), OAUTHBEARER_MECHANISM, Collections.emptyMap());
    return handler;
  }
}
