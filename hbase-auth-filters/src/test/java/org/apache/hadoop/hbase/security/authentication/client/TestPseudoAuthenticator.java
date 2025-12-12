/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.hbase.security.authentication.client;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.hbase.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.hbase.security.authentication.server.PseudoAuthenticationHandler;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({ MiscTests.class, SmallTests.class })
public class TestPseudoAuthenticator {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPseudoAuthenticator.class);

  private Properties getAuthenticationHandlerConfiguration(boolean anonymousAllowed) {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, Boolean.toString(anonymousAllowed));
    return props;
  }

  @Test
  public void testGetUserName() throws Exception {
    PseudoAuthenticator authenticator = new PseudoAuthenticator();
    // TODO getUserName() has protected access
    // assertEquals(System.getProperty("user.name"), authenticator.getUserName());
  }

  @Test
  public void testAnonymousAllowed() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(true));
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      auth.stop();
    }
  }

  @Test
  public void testAnonymousDisallowed() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(false));
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      assertTrue(conn.getHeaderFields().containsKey("WWW-Authenticate"));
      //assertEquals("Authentication required", conn.getResponseMessage());
      // TODO: Was support to be "Authentication required"
      assertEquals("Unauthorized", conn.getResponseMessage());
    } finally {
      auth.stop();
    }
  }

  @Test
  public void testAuthenticationAnonymousAllowed() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(true));
    auth._testAuthentication(new PseudoAuthenticator(), false);
  }

  @Test
  public void testAuthenticationAnonymousDisallowed() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(false));
    auth._testAuthentication(new PseudoAuthenticator(), false);
  }

  @Test
  public void testAuthenticationAnonymousAllowedWithPost() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(true));
    auth._testAuthentication(new PseudoAuthenticator(), true);
  }

  @Test
  public void testAuthenticationAnonymousDisallowedWithPost() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
      getAuthenticationHandlerConfiguration(false));
    auth._testAuthentication(new PseudoAuthenticator(), true);
  }

}
