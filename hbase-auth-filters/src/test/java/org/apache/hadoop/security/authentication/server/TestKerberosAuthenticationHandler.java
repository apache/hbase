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
package org.apache.hadoop.security.authentication.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.ietf.jgss.Oid;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Tests for Kerberos Authentication Handler.
 */
@Timeout(60)
public class TestKerberosAuthenticationHandler
    extends KerberosSecurityTestcase {

  protected KerberosAuthenticationHandler handler;

  protected KerberosAuthenticationHandler getNewAuthenticationHandler() {
    return new KerberosAuthenticationHandler();
  }

  protected String getExpectedType() {
    return KerberosAuthenticationHandler.TYPE;
  }

  protected Properties getDefaultProperties() {
    Properties props = new Properties();
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL,
            KerberosTestUtils.getServerPrincipal());
    props.setProperty(KerberosAuthenticationHandler.KEYTAB,
            KerberosTestUtils.getKeytabFile());
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
            "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    props.setProperty(KerberosAuthenticationHandler.RULE_MECHANISM,
            KerberosName.MECHANISM_HADOOP);
    return props;
  }

  @BeforeEach
  public void setup() throws Exception {
    // create keytab
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    String clientPrincipal = KerberosTestUtils.getClientPrincipal();
    String serverPrincipal = KerberosTestUtils.getServerPrincipal();
    clientPrincipal = clientPrincipal.substring(0,
        clientPrincipal.lastIndexOf("@"));
    serverPrincipal = serverPrincipal.substring(0,
        serverPrincipal.lastIndexOf("@"));
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
    // handler
    handler = getNewAuthenticationHandler();
    Properties props = getDefaultProperties();
    // Set whitelist for testing
    props.setProperty(KerberosAuthenticationHandler.ENDPOINT_WHITELIST,
        "/white,/white2,/white3");
    try {
      handler.init(props);
    } catch (Exception ex) {
      handler = null;
      throw ex;
    }
  }

  @Test
  public void testNameRulesHadoop() throws Exception {
    KerberosName kn = new KerberosName(KerberosTestUtils.getServerPrincipal());
    assertEquals(KerberosTestUtils.getRealm(), kn.getRealm());

    //destroy handler created in setUp()
    handler.destroy();
    handler = getNewAuthenticationHandler();

    Properties props = getDefaultProperties();
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
        "RULE:[1:$1@$0](.*@BAR)s/@.*//\nDEFAULT");

    try {
      handler.init(props);
    } catch (Exception ex) {
    }
    kn = new KerberosName("bar@BAR");
    assertEquals("bar", kn.getShortName());
    kn = new KerberosName("bar@FOO");
    try {
      kn.getShortName();
      fail();
    } catch (Exception ex) {
    }
  }

  @Test
  public void testNameRulesCompat() throws Exception {
    KerberosName kn = new KerberosName(KerberosTestUtils.getServerPrincipal());
    assertEquals(KerberosTestUtils.getRealm(), kn.getRealm());

    //destroy handler created in setUp()
    handler.destroy();
    handler = getNewAuthenticationHandler();

    Properties props = getDefaultProperties();
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES, "RULE:[1:$1@$0](.*@BAR)s/@.*//\nDEFAULT");
    props.setProperty(KerberosAuthenticationHandler.RULE_MECHANISM, KerberosName.MECHANISM_MIT);

    try {
      handler.init(props);
    } catch (Exception ex) {
    }
    kn = new KerberosName("bar@BAR");
    assertEquals("bar", kn.getShortName());
    kn = new KerberosName("bar@FOO");
    assertEquals("bar@FOO", kn.getShortName());
  }

  @Test
  public void testNullProperties() throws Exception {
    KerberosName kn = new KerberosName(KerberosTestUtils.getServerPrincipal());
    assertEquals(KerberosTestUtils.getRealm(), kn.getRealm());

    KerberosName.setRuleMechanism("MIT");
    KerberosName.setRules("DEFAULT");

    //destroy handler created in setUp()
    handler.destroy();
    handler = getNewAuthenticationHandler();

    Properties props = getDefaultProperties();
    props.remove(KerberosAuthenticationHandler.NAME_RULES);
    props.remove(KerberosAuthenticationHandler.RULE_MECHANISM);

    try {
      handler.init(props);
    } catch (Exception ex) {
    }

    assertEquals("MIT", KerberosName.getRuleMechanism());
    assertEquals("DEFAULT", KerberosName.getRules());
  }

  @Test
  public void testInit() throws Exception {
    assertEquals(KerberosTestUtils.getKeytabFile(), handler.getKeytab());
    Set<KerberosPrincipal> principals = handler.getPrincipals();
    Principal expectedPrincipal =
        new KerberosPrincipal(KerberosTestUtils.getServerPrincipal());
    assertTrue(principals.contains(expectedPrincipal));
    assertEquals(1, principals.size());
  }

  /**
   * Tests dynamic configuration of HTTP principals.
    * @throws Exception
   */
  @Test
  public void testDynamicPrincipalDiscovery() throws Exception {
    String[] keytabUsers = new String[]{
        "HTTP/host1", "HTTP/host2", "HTTP2/host1", "XHTTP/host"
    };
    String keytab = KerberosTestUtils.getKeytabFile();
    getKdc().createPrincipal(new File(keytab), keytabUsers);

    // destroy handler created in setUp()
    handler.destroy();
    Properties props = new Properties();
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, keytab);
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, "*");
    handler = getNewAuthenticationHandler();
    handler.init(props);

    assertEquals(KerberosTestUtils.getKeytabFile(),
        handler.getKeytab());
    
    Set<KerberosPrincipal> loginPrincipals = handler.getPrincipals();
    for (String user : keytabUsers) {
      Principal principal = new KerberosPrincipal(
          user + "@" + KerberosTestUtils.getRealm());
      boolean expected = user.startsWith("HTTP/");
      assertEquals(expected,
          loginPrincipals.contains(principal), "checking for " + user);
    }
  }

  /**
   * Tests dynamic principal discovery for missing principals.
   * @throws Exception
   */
  @Test
  public void testDynamicPrincipalDiscoveryMissingPrincipals()
      throws Exception {
    String[] keytabUsers = new String[]{"hdfs/localhost"};
    String keytab = KerberosTestUtils.getKeytabFile();
    getKdc().createPrincipal(new File(keytab), keytabUsers);

    // destroy handler created in setUp()
    handler.destroy();
    Properties props = new Properties();
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, keytab);
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, "*");
    handler = getNewAuthenticationHandler();
    try {
      handler.init(props);
      fail("init should have failed");
    } catch (ServletException ex) {
      assertEquals("Principals do not exist in the keytab",
          ex.getCause().getMessage());
    } catch (Throwable t) {
      fail("wrong exception: "+t);
    }
  }

  @Test
  public void testType() {
    assertEquals(getExpectedType(), handler.getType());
  }

  @Test
  public void testRequestWithoutAuthorization() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    assertNull(handler.authenticate(request, response));
    Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
        KerberosAuthenticator.NEGOTIATE);
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testRequestWithInvalidAuthorization() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
        .thenReturn("invalid");
    assertNull(handler.authenticate(request, response));
    Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
        KerberosAuthenticator.NEGOTIATE);
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testRequestWithIncompleteAuthorization() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
      .thenReturn(KerberosAuthenticator.NEGOTIATE);
    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void testRequestWithAuthorization() throws Exception {
    String token = KerberosTestUtils.doAsClient(new Callable<String>() {
      @Override
      public String call() throws Exception {
        GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        try {
          String servicePrincipal = KerberosTestUtils.getServerPrincipal();
          Oid oid = KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID;
          GSSName serviceName = gssManager.createName(servicePrincipal,
              oid);
          oid = KerberosUtil.GSS_KRB5_MECH_OID;
          gssContext = gssManager.createContext(serviceName, oid, null,
                                                  GSSContext.DEFAULT_LIFETIME);
          gssContext.requestCredDeleg(true);
          gssContext.requestMutualAuth(true);

          byte[] inToken = new byte[0];
          byte[] outToken =
              gssContext.initSecContext(inToken, 0, inToken.length);
          Base64 base64 = new Base64(0);
          return base64.encodeToString(outToken);

        } finally {
          if (gssContext != null) {
            gssContext.dispose();
          }
        }
      }
    });

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
      .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);
    Mockito.when(request.getServerName()).thenReturn("localhost");
    
    AuthenticationToken authToken = handler.authenticate(request, response);

    if (authToken != null) {
      Mockito.verify(response)
          .setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
              Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
      Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);

      assertEquals(KerberosTestUtils.getClientPrincipal(),
          authToken.getName());
      assertTrue(KerberosTestUtils.getClientPrincipal()
          .startsWith(authToken.getUserName()));
      assertEquals(getExpectedType(), authToken.getType());
    } else {
      Mockito.verify(response).setHeader(
          Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
          Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
      Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
  }

  @Test
  public void testRequestWithInvalidKerberosAuthorization() {

    String token = new Base64(0).encodeToString(new byte[]{0, 1, 2});

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
        .thenReturn(KerberosAuthenticator.NEGOTIATE + token);

    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void testRequestToWhitelist() throws Exception {
    final String token = new Base64(0).encodeToString(new byte[]{0, 1, 2});
    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse response =
        Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
        .thenReturn(KerberosAuthenticator.NEGOTIATE + token);
    Mockito.when(request.getServletPath()).thenReturn("/white");
    handler.authenticate(request, response);
    Mockito.when(request.getServletPath()).thenReturn("/white4");
    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (handler != null) {
      handler.destroy();
      handler = null;
    }
  }
}
