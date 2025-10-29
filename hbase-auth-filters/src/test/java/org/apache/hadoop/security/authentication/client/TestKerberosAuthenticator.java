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
package org.apache.hadoop.security.authentication.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.SCHEMES_PROPERTY;
import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.AUTH_HANDLER_PROPERTY;
import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.PRINCIPAL;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.KEYTAB;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.NAME_RULES;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.CharacterCodingException;
import javax.security.sasl.AuthenticationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Test class for {@link KerberosAuthenticator}.
 */
public class TestKerberosAuthenticator extends KerberosSecurityTestcase {

  public TestKerberosAuthenticator() {
  }

  @BeforeEach
  public void setup() throws Exception {
    // create keytab
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    String clientPrincipal = KerberosTestUtils.getClientPrincipal();
    String serverPrincipal = KerberosTestUtils.getServerPrincipal();
    clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
    serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private Properties getAuthenticationHandlerConfiguration() {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "kerberos");
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                      "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    props.setProperty(KerberosAuthenticationHandler.RULE_MECHANISM, "hadoop");
    return props;
  }

  private Properties getMultiAuthHandlerConfiguration() {
    Properties props = new Properties();
    props.setProperty(AUTH_TYPE, MultiSchemeAuthenticationHandler.TYPE);
    props.setProperty(SCHEMES_PROPERTY, "negotiate");
    props.setProperty(String.format(AUTH_HANDLER_PROPERTY, "negotiate"),
        "kerberos");
    props.setProperty(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(NAME_RULES,
        "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm() + ")s/@.*//\n");
    return props;
  }

  @Test
  @Timeout(value = 60)
  public void testFallbacktoPseudoAuthenticator() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
    AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
    auth._testAuthentication(new KerberosAuthenticator(), false);
  }

  @Test
  @Timeout(value = 60)
  public void testFallbacktoPseudoAuthenticatorAnonymous() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
    AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
    auth._testAuthentication(new KerberosAuthenticator(), false);
  }

  @Test
  @Timeout(value = 60)
  public void testNotAuthenticated() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      assertTrue(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
    } finally {
      auth.stop();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testAuthentication() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testAuthenticationPost() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testAuthenticationHttpClient() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testAuthenticationHttpClientPost() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testNotAuthenticatedWithMultiAuthHandler() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          conn.getResponseCode());
      assertTrue(conn
          .getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
    } finally {
      auth.stop();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testAuthenticationWithMultiAuthHandler() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testAuthenticationHttpClientPostWithMultiAuthHandler()
      throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test
  @Timeout(value = 60)
  public void testWrapExceptionWithMessage() {
    IOException ex;
    ex = new IOException("Induced exception");
    ex = KerberosAuthenticator.wrapExceptionWithMessage(ex, "Error while "
        + "authenticating with endpoint: localhost");
    assertEquals("Induced exception", ex.getCause().getMessage());
    assertEquals("Error while authenticating with endpoint: localhost",
        ex.getMessage());

    ex = new AuthenticationException("Auth exception");
    ex = KerberosAuthenticator.wrapExceptionWithMessage(ex, "Error while "
        + "authenticating with endpoint: localhost");
    assertEquals("Auth exception", ex.getCause().getMessage());
    assertEquals("Error while authenticating with endpoint: localhost",
        ex.getMessage());

    // Test for Exception with  no (String) constructor
    // redirect the LOG to and check log message
    ex = new CharacterCodingException();
    Exception ex2 = KerberosAuthenticator.wrapExceptionWithMessage(ex,
        "Error while authenticating with endpoint: localhost");
    assertTrue(ex instanceof CharacterCodingException);
    assertTrue(ex.equals(ex2));
  }

  @Test
  @Timeout(value = 60)
  public void testNegotiate() throws NoSuchMethodException, InvocationTargetException,
          IllegalAccessException, IOException {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();

    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE)).
            thenReturn(KerberosAuthenticator.NEGOTIATE);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    Method method = KerberosAuthenticator.class.getDeclaredMethod("isNegotiate",
            HttpURLConnection.class);
    method.setAccessible(true);

    assertTrue((boolean)method.invoke(kerberosAuthenticator, conn));
  }

  @Test
  @Timeout(value = 60)
  public void testNegotiateLowerCase() throws NoSuchMethodException, InvocationTargetException,
          IllegalAccessException, IOException {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();

    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getHeaderField("www-authenticate"))
            .thenReturn(KerberosAuthenticator.NEGOTIATE);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);

    Method method = KerberosAuthenticator.class.getDeclaredMethod("isNegotiate",
            HttpURLConnection.class);
    method.setAccessible(true);

    assertTrue((boolean)method.invoke(kerberosAuthenticator, conn));
  }

  @Test
  @Timeout(value = 60)
  public void testReadToken() throws NoSuchMethodException, IOException, IllegalAccessException,
          InvocationTargetException {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    FieldUtils.writeField(kerberosAuthenticator, "base64", new Base64(), true);

    Base64 base64 = new Base64();

    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);
    when(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE))
            .thenReturn(KerberosAuthenticator.NEGOTIATE + " " +
                    Arrays.toString(base64.encode("foobar".getBytes())));

    Method method = KerberosAuthenticator.class.getDeclaredMethod("readToken",
            HttpURLConnection.class);
    method.setAccessible(true);

    method.invoke(kerberosAuthenticator, conn); // expecting this not to throw an exception
  }

  @Test
  @Timeout(value = 60)
  public void testReadTokenLowerCase() throws NoSuchMethodException, IOException,
          IllegalAccessException, InvocationTargetException {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    FieldUtils.writeField(kerberosAuthenticator, "base64", new Base64(), true);

    Base64 base64 = new Base64();

    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAUTHORIZED);
    when(conn.getHeaderField("www-authenticate"))
            .thenReturn(KerberosAuthenticator.NEGOTIATE +
                    Arrays.toString(base64.encode("foobar".getBytes())));

    Method method = KerberosAuthenticator.class.getDeclaredMethod("readToken",
            HttpURLConnection.class);
    method.setAccessible(true);

    method.invoke(kerberosAuthenticator, conn); // expecting this not to throw an exception
  }
}
