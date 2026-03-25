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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Map;
import javax.security.sasl.Sasl;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestSaslUtil {

  @Test
  public void testInitSaslProperties() {
    Map<String, String> props;

    props = SaslUtil.initSaslProperties("integrity");
    assertEquals("auth-int", props.get(Sasl.QOP));

    props = SaslUtil.initSaslProperties("privacy,authentication");
    assertEquals("auth-conf,auth", props.get(Sasl.QOP));

    props = SaslUtil.initSaslProperties("integrity,authentication,privacy");
    assertEquals("auth-int,auth,auth-conf", props.get(Sasl.QOP));

    assertThrows(IllegalArgumentException.class, () -> {
      Map<String, String> invalidProps = SaslUtil.initSaslProperties("xyz");
      assertEquals("auth", invalidProps.get(Sasl.QOP));
    });

    props = SaslUtil.initSaslProperties("");
    assertEquals("auth", props.get(Sasl.QOP));
  }

  @Test
  public void testVerifyQop() throws IOException {
    String nullQop = null;
    String authentication = "auth";
    String integrity = "auth-int";
    String confidentality = "auth-conf";
    String anyQop = "auth-conf,auth-int,auth";

    // Empty requested, got empty
    SaslUtil.verifyNegotiatedQop(nullQop, nullQop);

    // Auth requested, got null
    SaslUtil.verifyNegotiatedQop(authentication, nullQop);

    // Auth requested, got auth
    SaslUtil.verifyNegotiatedQop(authentication, authentication);

    // Auth requested, got confidentiality.
    assertThrows(IOException.class,
      () -> SaslUtil.verifyNegotiatedQop(authentication, confidentality));

    // Integrity requested requested, got null
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(integrity, nullQop));

    // Integrity requested requested, got auth
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(integrity, authentication));

    // Integrity requested requested, got conf
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(integrity, authentication));

    // Confidentiality requested requested, got null
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(confidentality, nullQop));

    // Confidentiality requested requested, got auth
    assertThrows(IOException.class,
      () -> SaslUtil.verifyNegotiatedQop(confidentality, authentication));

    // Confidentiality requested requested, got integrity
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(confidentality, integrity));

    // Confidentiality requested requested, got confidentiality
    assertThrows(IOException.class, () -> SaslUtil.verifyNegotiatedQop(confidentality, integrity));

    // Any requested, got null
    SaslUtil.verifyNegotiatedQop(anyQop, null);

    // Any requested, got auth
    SaslUtil.verifyNegotiatedQop(anyQop, authentication);

    // Any requested, got integrity
    SaslUtil.verifyNegotiatedQop(anyQop, integrity);

    // Any requested, got confidentiality
    SaslUtil.verifyNegotiatedQop(anyQop, confidentality);
  }
}
