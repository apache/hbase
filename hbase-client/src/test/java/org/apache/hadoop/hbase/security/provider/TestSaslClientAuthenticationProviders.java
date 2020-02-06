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

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@Category({SmallTests.class, SecurityTests.class})
public class TestSaslClientAuthenticationProviders {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSaslClientAuthenticationProviders.class);

  @Test
  public void testCannotAddTheSameProviderTwice() {
    HashMap<Byte,SaslClientAuthenticationProvider> registeredProviders = new HashMap<>();
    SaslClientAuthenticationProvider p1 = new SimpleSaslClientAuthenticationProvider();
    SaslClientAuthenticationProvider p2 = new SimpleSaslClientAuthenticationProvider();

    SaslClientAuthenticationProviders.addProviderIfNotExists(p1, registeredProviders);
    assertEquals(1, registeredProviders.size());

    try {
      SaslClientAuthenticationProviders.addProviderIfNotExists(p2, registeredProviders);
    } catch (RuntimeException e) {}

    assertSame("Expected the original provider to be present", p1,
        registeredProviders.entrySet().iterator().next().getValue());
  }

  @Test
  public void testInstanceIsCached() {
    Configuration conf = HBaseConfiguration.create();
    SaslClientAuthenticationProviders providers1 =
        SaslClientAuthenticationProviders.getInstance(conf);
    SaslClientAuthenticationProviders providers2 =
        SaslClientAuthenticationProviders.getInstance(conf);
    assertSame(providers1, providers2);

    SaslClientAuthenticationProviders.reset();

    SaslClientAuthenticationProviders providers3 =
        SaslClientAuthenticationProviders.getInstance(conf);
    assertNotSame(providers1, providers3);
    assertEquals(providers1.getNumRegisteredProviders(), providers3.getNumRegisteredProviders());
  }

  @Test(expected = RuntimeException.class)
  public void testDifferentConflictingImplementationsFail() {
    Configuration conf = HBaseConfiguration.create();
    conf.setStrings(SaslClientAuthenticationProviders.EXTRA_PROVIDERS_KEY,
        ConflictingProvider1.class.getName(), ConflictingProvider2.class.getName());
    SaslClientAuthenticationProviders.getInstance(conf);
  }

  static class ConflictingProvider1 implements SaslClientAuthenticationProvider {
    static final SaslAuthMethod METHOD1 = new SaslAuthMethod(
        "FOO", (byte)12, "DIGEST-MD5", AuthenticationMethod.SIMPLE);

    public ConflictingProvider1() {
    }

    @Override public SaslAuthMethod getSaslAuthMethod() {
      return METHOD1;
    }

    @Override public String getTokenKind() {
      return null;
    }

    @Override public SaslClient createClient(Configuration conf, InetAddress serverAddr,
        SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
        Map<String, String> saslProps) throws IOException {
      return null;
    }

    @Override public UserInformation getUserInfo(User user) {
      return null;
    }
  }

  static class ConflictingProvider2 implements SaslClientAuthenticationProvider {
    static final SaslAuthMethod METHOD2 = new SaslAuthMethod(
        "BAR", (byte)12, "DIGEST-MD5", AuthenticationMethod.SIMPLE);

    public ConflictingProvider2() {
    }

    @Override public SaslAuthMethod getSaslAuthMethod() {
      return METHOD2;
    }

    @Override public String getTokenKind() {
      return null;
    }

    @Override public SaslClient createClient(Configuration conf, InetAddress serverAddr,
        SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
        Map<String, String> saslProps) throws IOException {
      return null;
    }

    @Override public UserInformation getUserInfo(User user) {
      return null;
    }
  }
}
