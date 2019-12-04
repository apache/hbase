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

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
}
