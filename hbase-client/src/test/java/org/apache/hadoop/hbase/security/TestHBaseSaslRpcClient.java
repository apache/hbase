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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider.DigestSaslClientCallbackHandler;
import org.apache.hadoop.hbase.security.provider.GssSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SimpleSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Strings;

@Category({ SecurityTests.class, SmallTests.class })
public class TestHBaseSaslRpcClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseSaslRpcClient.class);

  static {
    System.setProperty("java.security.krb5.realm", "DOMAIN.COM");
    System.setProperty("java.security.krb5.kdc", "DOMAIN.COM");
  }

  static final String DEFAULT_USER_NAME = "principal";
  static final String DEFAULT_USER_PASSWORD = "password";

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseSaslRpcClient.class);

  @Test
  public void testSaslClientUsesGivenRpcProtection() throws Exception {
    Token<? extends TokenIdentifier> token =
      createTokenMockWithCredentials(DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD);
    DigestSaslClientAuthenticationProvider provider = new DigestSaslClientAuthenticationProvider();
    for (SaslUtil.QualityOfProtection qop : SaslUtil.QualityOfProtection.values()) {
      String negotiatedQop = new HBaseSaslRpcClient(HBaseConfiguration.create(), provider, token,
        Mockito.mock(InetAddress.class), "", false, qop.name(), false) {
        public String getQop() {
          return saslProps.get(Sasl.QOP);
        }
      }.getQop();
      assertEquals(negotiatedQop, qop.getSaslQop());
    }
  }

  @Test
  public void testDigestSaslClientCallbackHandler() throws UnsupportedCallbackException {
    final Token<? extends TokenIdentifier> token = createTokenMock();
    when(token.getIdentifier()).thenReturn(Bytes.toBytes(DEFAULT_USER_NAME));
    when(token.getPassword()).thenReturn(Bytes.toBytes(DEFAULT_USER_PASSWORD));

    final NameCallback nameCallback = mock(NameCallback.class);
    final PasswordCallback passwordCallback = mock(PasswordCallback.class);
    final RealmCallback realmCallback = mock(RealmCallback.class);

    // We can provide a realmCallback, but HBase presently does nothing with it.
    Callback[] callbackArray = { nameCallback, passwordCallback, realmCallback };
    final DigestSaslClientCallbackHandler saslClCallbackHandler =
      new DigestSaslClientCallbackHandler(token);
    saslClCallbackHandler.handle(callbackArray);
    verify(nameCallback).setName(anyString());
    verify(passwordCallback).setPassword(any());
  }

  @Test
  public void testDigestSaslClientCallbackHandlerWithException() {
    final Token<? extends TokenIdentifier> token = createTokenMock();
    when(token.getIdentifier()).thenReturn(Bytes.toBytes(DEFAULT_USER_NAME));
    when(token.getPassword()).thenReturn(Bytes.toBytes(DEFAULT_USER_PASSWORD));
    final DigestSaslClientCallbackHandler saslClCallbackHandler =
      new DigestSaslClientCallbackHandler(token);
    try {
      saslClCallbackHandler.handle(new Callback[] { mock(TextOutputCallback.class) });
    } catch (UnsupportedCallbackException expEx) {
      // expected
    } catch (Exception ex) {
      fail("testDigestSaslClientCallbackHandlerWithException error : " + ex.getMessage());
    }
  }

  @Test
  public void testHBaseSaslRpcClientCreation() throws Exception {
    // creation kerberos principal check section
    // Note this is mocked in a way that doesn't care about principal names
    assertFalse(assertSuccessCreationKerberos());

    // creation digest principal check section
    assertFalse(assertSuccessCreationDigestPrincipal(null, null));
    assertFalse(assertSuccessCreationDigestPrincipal("", ""));
    assertFalse(assertSuccessCreationDigestPrincipal("", null));
    assertFalse(assertSuccessCreationDigestPrincipal(null, ""));
    assertTrue(assertSuccessCreationDigestPrincipal(DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD));

    // creation simple principal check section
    // Note this is mocked in a way that doesn't care about principal names
    assertFalse(assertSuccessCreationSimple());

    // exceptions check section
    assertTrue(assertIOExceptionThenSaslClientIsNull(DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD));
    assertTrue(
      assertIOExceptionWhenGetStreamsBeforeConnectCall(DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD));
  }

  @Test
  public void testAuthMethodReadWrite() throws IOException {
    DataInputBuffer in = new DataInputBuffer();
    DataOutputBuffer out = new DataOutputBuffer();

    assertAuthMethodRead(in, AuthMethod.SIMPLE);
    assertAuthMethodRead(in, AuthMethod.KERBEROS);
    assertAuthMethodRead(in, AuthMethod.DIGEST);

    assertAuthMethodWrite(out, AuthMethod.SIMPLE);
    assertAuthMethodWrite(out, AuthMethod.KERBEROS);
    assertAuthMethodWrite(out, AuthMethod.DIGEST);
  }

  private void assertAuthMethodRead(DataInputBuffer in, AuthMethod authMethod) throws IOException {
    in.reset(new byte[] { authMethod.code }, 1);
    assertEquals(authMethod, AuthMethod.read(in));
  }

  private void assertAuthMethodWrite(DataOutputBuffer out, AuthMethod authMethod)
    throws IOException {
    authMethod.write(out);
    assertEquals(authMethod.code, out.getData()[0]);
    out.reset();
  }

  private boolean assertIOExceptionWhenGetStreamsBeforeConnectCall(String principal,
    String password) throws IOException {
    boolean inState = false;
    boolean outState = false;

    DigestSaslClientAuthenticationProvider provider = new DigestSaslClientAuthenticationProvider() {
      @Override
      public SaslClient createClient(Configuration conf, InetAddress serverAddress,
        String serverPrincipal, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
        Map<String, String> saslProps) {
        return Mockito.mock(SaslClient.class);
      }
    };
    HBaseSaslRpcClient rpcClient = new HBaseSaslRpcClient(HBaseConfiguration.create(), provider,
      createTokenMockWithCredentials(principal, password), Mockito.mock(InetAddress.class), "",
      false);

    try {
      rpcClient.getInputStream();
    } catch (IOException ex) {
      // Sasl authentication exchange hasn't completed yet
      inState = true;
    }

    try {
      rpcClient.getOutputStream();
    } catch (IOException ex) {
      // Sasl authentication exchange hasn't completed yet
      outState = true;
    }

    return inState && outState;
  }

  private boolean assertIOExceptionThenSaslClientIsNull(String principal, String password) {
    try {
      DigestSaslClientAuthenticationProvider provider =
        new DigestSaslClientAuthenticationProvider() {
          @Override
          public SaslClient createClient(Configuration conf, InetAddress serverAddress,
            String serverPrincipal, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
            Map<String, String> saslProps) {
            return null;
          }
        };
      new HBaseSaslRpcClient(HBaseConfiguration.create(), provider,
        createTokenMockWithCredentials(principal, password), Mockito.mock(InetAddress.class), "",
        false);
      return false;
    } catch (IOException ex) {
      return true;
    }
  }

  private boolean assertSuccessCreationKerberos() {
    HBaseSaslRpcClient rpcClient = null;
    try {
      // createSaslRpcClientForKerberos is mocked in a way that doesn't care about principal names
      rpcClient = createSaslRpcClientForKerberos();
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
    return rpcClient != null;
  }

  private boolean assertSuccessCreationDigestPrincipal(String principal, String password) {
    HBaseSaslRpcClient rpcClient = null;
    try {
      rpcClient = new HBaseSaslRpcClient(HBaseConfiguration.create(),
        new DigestSaslClientAuthenticationProvider(),
        createTokenMockWithCredentials(principal, password), Mockito.mock(InetAddress.class), "",
        false);
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
    return rpcClient != null;
  }

  private boolean assertSuccessCreationSimple() {
    HBaseSaslRpcClient rpcClient = null;
    try {
      rpcClient = createSaslRpcClientSimple();
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
    return rpcClient != null;
  }

  private HBaseSaslRpcClient createSaslRpcClientForKerberos() throws IOException {
    return new HBaseSaslRpcClient(HBaseConfiguration.create(),
      new GssSaslClientAuthenticationProvider(), createTokenMock(), Mockito.mock(InetAddress.class),
      "", false);
  }

  private Token<? extends TokenIdentifier> createTokenMockWithCredentials(String principal,
    String password) throws IOException {
    Token<? extends TokenIdentifier> token = createTokenMock();
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(password)) {
      when(token.getIdentifier()).thenReturn(Bytes.toBytes(DEFAULT_USER_NAME));
      when(token.getPassword()).thenReturn(Bytes.toBytes(DEFAULT_USER_PASSWORD));
    }
    return token;
  }

  private HBaseSaslRpcClient createSaslRpcClientSimple() throws IOException {
    return new HBaseSaslRpcClient(HBaseConfiguration.create(),
      new SimpleSaslClientAuthenticationProvider(), createTokenMock(),
      Mockito.mock(InetAddress.class), "", false);
  }

  @SuppressWarnings("unchecked")
  private Token<? extends TokenIdentifier> createTokenMock() {
    return mock(Token.class);
  }
}
