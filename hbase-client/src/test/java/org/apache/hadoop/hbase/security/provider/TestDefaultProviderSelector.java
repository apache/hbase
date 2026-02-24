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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestDefaultProviderSelector {

  BuiltInProviderSelector selector;

  @BeforeEach
  public void setup() {
    selector = new BuiltInProviderSelector();
  }

  @Test
  public void testExceptionOnMissingProviders() {
    assertThrows(IllegalStateException.class,
      () -> selector.configure(new Configuration(false), Collections.emptySet()));
  }

  @Test
  public void testNullConfiguration() {
    assertThrows(NullPointerException.class,
      () -> selector.configure(null, Collections.emptySet()));
  }

  @Test
  public void testNullProviderMap() {
    assertThrows(NullPointerException.class, () -> selector.configure(new Configuration(), null));
  }

  @Test
  public void testDuplicateProviders() {
    Set<SaslClientAuthenticationProvider> providers = new HashSet<>();
    providers.add(new SimpleSaslClientAuthenticationProvider());
    providers.add(new SimpleSaslClientAuthenticationProvider());
    assertThrows(IllegalStateException.class,
      () -> selector.configure(new Configuration(false), providers));
  }

  @Test
  public void testExpectedProviders() {
    HashSet<SaslClientAuthenticationProvider> providers =
      new HashSet<>(Arrays.asList(new SimpleSaslClientAuthenticationProvider(),
        new GssSaslClientAuthenticationProvider(), new DigestSaslClientAuthenticationProvider()));

    selector.configure(new Configuration(false), providers);

    assertNotNull(selector.simpleAuth, "Simple provider was null");
    assertNotNull(selector.krbAuth, "Kerberos provider was null");
    assertNotNull(selector.digestAuth, "Digest provider was null");
  }
}
