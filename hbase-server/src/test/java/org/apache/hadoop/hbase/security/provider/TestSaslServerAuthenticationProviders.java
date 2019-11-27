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

import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

public class TestSaslServerAuthenticationProviders {

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
    SaslServerAuthenticationProviders providers1 = SaslServerAuthenticationProviders.getInstance(conf);
    SaslServerAuthenticationProviders providers2 = SaslServerAuthenticationProviders.getInstance(conf);
    assertSame(providers1, providers2);

    SaslServerAuthenticationProviders.reset();

    SaslServerAuthenticationProviders providers3 = SaslServerAuthenticationProviders.getInstance(conf);
    assertNotSame(providers1, providers3);
    assertEquals(providers1.getNumRegisteredProviders(), providers3.getNumRegisteredProviders());
  }
}
