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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class, SecurityTests.class})
public class TestSaslServerAuthenticationProviders {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSaslServerAuthenticationProviders.class);

  @Before
  public void reset() {
    // Clear out any potentially bogus state from the providers class
    SaslServerAuthenticationProviders.reset();
  }

  @Test
  public void testCannotAddTheSameProviderTwice() {
    HashMap<Byte,SaslServerAuthenticationProvider> registeredProviders = new HashMap<>();
    SimpleSaslServerAuthenticationProvider p1 = new SimpleSaslServerAuthenticationProvider();
    SimpleSaslServerAuthenticationProvider p2 = new SimpleSaslServerAuthenticationProvider();

    SaslServerAuthenticationProviders.addProviderIfNotExists(p1, registeredProviders);
    assertEquals(1, registeredProviders.size());

    try {
      SaslServerAuthenticationProviders.addProviderIfNotExists(p2, registeredProviders);
    } catch (RuntimeException e) {}

    assertSame("Expected the original provider to be present", p1,
        registeredProviders.entrySet().iterator().next().getValue());
  }

  @Test
  public void testInstanceIsCached() {
    Configuration conf = HBaseConfiguration.create();
    SaslServerAuthenticationProviders providers1 =
        SaslServerAuthenticationProviders.getInstance(conf);
    SaslServerAuthenticationProviders providers2 =
        SaslServerAuthenticationProviders.getInstance(conf);
    assertSame(providers1, providers2);

    SaslServerAuthenticationProviders.reset();

    SaslServerAuthenticationProviders providers3 =
        SaslServerAuthenticationProviders.getInstance(conf);
    assertNotSame(providers1, providers3);
    assertEquals(providers1.getNumRegisteredProviders(), providers3.getNumRegisteredProviders());
  }

  @Test
  public void instancesAreInitialized() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(SaslServerAuthenticationProviders.EXTRA_PROVIDERS_KEY,
        InitCheckingSaslServerAuthenticationProvider.class.getName());

    SaslServerAuthenticationProviders providers =
        SaslServerAuthenticationProviders.getInstance(conf);

    SaslServerAuthenticationProvider provider =
        providers.selectProvider(InitCheckingSaslServerAuthenticationProvider.ID);
    assertEquals(InitCheckingSaslServerAuthenticationProvider.class, provider.getClass());

    assertTrue("Provider was not inititalized",
        ((InitCheckingSaslServerAuthenticationProvider) provider).isInitialized());
  }

  public static class InitCheckingSaslServerAuthenticationProvider
      implements SaslServerAuthenticationProvider {
    public static final byte ID = (byte)88;
    private boolean initialized = false;

    public synchronized void init(Configuration conf) {
      this.initialized = true;
    }

    public synchronized boolean isInitialized() {
      return initialized;
    }

    @Override
    public SaslAuthMethod getSaslAuthMethod() {
      return new SaslAuthMethod("INIT_CHECKING", ID, "DIGEST-MD5", AuthenticationMethod.TOKEN);
    }

    @Override
    public String getTokenKind() {
      return "INIT_CHECKING_TOKEN";
    }

    @Override
    public AttemptingUserProvidingSaslServer createServer(
        SecretManager<TokenIdentifier> secretManager,
        Map<String, String> saslProps) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsProtocolAuthentication() {
      return false;
    }

    @Override
    public UserGroupInformation getAuthorizedUgi(
        String authzId, SecretManager<TokenIdentifier> secretManager)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
