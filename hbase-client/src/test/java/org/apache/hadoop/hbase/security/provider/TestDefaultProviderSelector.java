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

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultProviderSelector {

  DefaultProviderSelector selector;
  @Before
  public void setup() {
    selector = new DefaultProviderSelector();
  }

  @Test(expected = IllegalStateException.class)
  public void testExceptionOnMissingProviders() {
    selector.configure(new Configuration(false), Collections.emptyMap());
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfiguration() {
    selector.configure(null, Collections.emptyMap());
  }

  @Test(expected = NullPointerException.class)
  public void testNullProviderMap() {
    selector.configure(new Configuration(false), null);
  }

  @Test(expected = IllegalStateException.class)
  public void testDuplicateProviders() {
    Map<Byte,SaslClientAuthenticationProvider> providers = new HashMap<>();
    providers.put((byte) 1, new SimpleSaslClientAuthenticationProvider());
    providers.put((byte) 2, new SimpleSaslClientAuthenticationProvider());
    selector.configure(new Configuration(false), providers);
  }

  @Test
  public void testExpectedProviders() {
    Map<Byte,SaslClientAuthenticationProvider> providers = new HashMap<>();

    for (SaslClientAuthenticationProvider provider : Arrays.asList(
        new SimpleSaslClientAuthenticationProvider(), new GssSaslClientAuthenticationProvider(),
        new DigestSaslClientAuthenticationProvider())) {
      providers.put(provider.getSaslAuthMethod().getCode(), provider);
    }

    selector.configure(new Configuration(false), providers);

    assertNotNull("Simple provider was null", selector.simpleAuth);
    assertNotNull("Kerberos provider was null", selector.krbAuth);
    assertNotNull("Digest provider was null", selector.digestAuth);
  }
}
