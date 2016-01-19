/**
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

import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import javax.security.sasl.Sasl;
import java.util.Map;

@Category({SecurityTests.class, SmallTests.class})
public class TestSaslUtil {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testInitSaslProperties() {
    Map<String, String> props;

    props = SaslUtil.initSaslProperties("integrity");
    assertEquals(props.get(Sasl.QOP), "auth-int");

    props = SaslUtil.initSaslProperties("privacy,authentication");
    assertEquals(props.get(Sasl.QOP), "auth-conf,auth");

    props = SaslUtil.initSaslProperties("integrity,authentication,privacy");
    assertEquals(props.get(Sasl.QOP), "auth-int,auth,auth-conf");

    exception.expect(IllegalArgumentException.class);
    props = SaslUtil.initSaslProperties("xyz");
    assertEquals(props.get(Sasl.QOP), "auth");

    exception.expect(IllegalArgumentException.class);
    props = SaslUtil.initSaslProperties("");
    assertEquals(props.get(Sasl.QOP), "auth");
  }
}
