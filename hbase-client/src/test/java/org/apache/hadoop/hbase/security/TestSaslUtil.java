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
import static org.junit.Assert.fail;

import java.util.Map;
import javax.security.sasl.Sasl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({ SecurityTests.class, SmallTests.class })
public class TestSaslUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSaslUtil.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testInitSaslProperties() {
    Map<String, String> props;

    props = SaslUtil.initSaslProperties("integrity");
    assertEquals("auth-int", props.get(Sasl.QOP));

    props = SaslUtil.initSaslProperties("privacy,authentication");
    assertEquals("auth-conf,auth", props.get(Sasl.QOP));

    props = SaslUtil.initSaslProperties("integrity,authentication,privacy");
    assertEquals("auth-int,auth,auth-conf", props.get(Sasl.QOP));

    exception.expect(IllegalArgumentException.class);
    props = SaslUtil.initSaslProperties("xyz");
    assertEquals("auth", props.get(Sasl.QOP));

    exception.expect(IllegalArgumentException.class);
    props = SaslUtil.initSaslProperties("");
    assertEquals("auth", props.get(Sasl.QOP));
  }

  @Test
  public void testVerifyQop() {
    String nullQop = null;
    String authentication = "auth";
    String integrity = "auth-int";
    String confidentality = "auth-conf";
    String anyQop = "auth-conf,auth-int,auth";

    // Empty requested, got empty
    try {
      SaslUtil.verifyNegotiatedQop(nullQop, nullQop);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Auth requested, got null
    try {
      SaslUtil.verifyNegotiatedQop(authentication, nullQop);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Auth requested, got auth
    try {
      SaslUtil.verifyNegotiatedQop(authentication, authentication);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Auth requested, got confidentality.
    try {
      SaslUtil.verifyNegotiatedQop(authentication, confidentality);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Integrity requested requested, got null
    try {
      SaslUtil.verifyNegotiatedQop(integrity, nullQop);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Integrity requested requested, got auth
    try {
      SaslUtil.verifyNegotiatedQop(integrity, authentication);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Integrity requested requested, got conf
    try {
      SaslUtil.verifyNegotiatedQop(integrity, authentication);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Confidentality requested requested, got null
    try {
      SaslUtil.verifyNegotiatedQop(confidentality, nullQop);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Confidentality requested requested, got auth
    try {
      SaslUtil.verifyNegotiatedQop(confidentality, authentication);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Confidentality requested requested, got integrity
    try {
      SaslUtil.verifyNegotiatedQop(confidentality, integrity);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Confidentality requested requested, got confidentality
    try {
      SaslUtil.verifyNegotiatedQop(confidentality, integrity);
      fail("Should have thrown exception");
    } catch (Exception e) {
    }

    // Any requested, got null
    try {
      SaslUtil.verifyNegotiatedQop(anyQop, null);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Any requested, got auth
    try {
      SaslUtil.verifyNegotiatedQop(anyQop, authentication);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Any requested, got integrity
    try {
      SaslUtil.verifyNegotiatedQop(anyQop, integrity);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }

    // Any requested, got confidentality
    try {
      SaslUtil.verifyNegotiatedQop(anyQop, confidentality);
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }
  }
}
