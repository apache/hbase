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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Comprehensively tests all permutations of ClientAuth modes and host verification
 * enabled/disabled. Tests each permutation of that against each relevant value of
 * {@link CertConfig}, i.e. passing no cert, a bad cert, etc. See inline comments in {@link #data()}
 * below for what the expectations are
 */
@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestMutualTlsServerSide extends AbstractTestMutualTls {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMutualTlsServerSide.class);
  @Parameterized.Parameter(6)
  public X509Util.ClientAuth clientAuthMode;

  @Parameterized.Parameters(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, "
    + "validateClientHostnames={3}, testCase={4}, clientAuthMode={5}")
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (String keyPassword : new String[] { "", "pa$$w0rd" }) {
          // we want to run with and without validating hostnames. we encode the expected success
          // criteria
          // in the TestCase config. See below.
          for (boolean validateClientHostnames : new Boolean[] { true, false }) {
            // ClientAuth.NONE should succeed in all cases, because it never requests the
            // certificate for verification
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, true,
              validateClientHostnames, CertConfig.NO_CLIENT_CERT, X509Util.ClientAuth.NONE });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, true,
              validateClientHostnames, CertConfig.NON_VERIFIABLE_CERT, X509Util.ClientAuth.NONE });
            params.add(
              new Object[] { caKeyType, certKeyType, keyPassword, true, validateClientHostnames,
                CertConfig.VERIFIABLE_CERT_WITH_BAD_HOST, X509Util.ClientAuth.NONE });

            // ClientAuth.WANT should succeed if no cert, but if the cert is provided it is
            // validated. So should fail on bad cert or good cert with bad host when host
            // verification is enabled
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, true,
              validateClientHostnames, CertConfig.NO_CLIENT_CERT, X509Util.ClientAuth.WANT });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, false,
              validateClientHostnames, CertConfig.NON_VERIFIABLE_CERT, X509Util.ClientAuth.WANT });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, !validateClientHostnames,
              validateClientHostnames, CertConfig.VERIFIABLE_CERT_WITH_BAD_HOST,
              X509Util.ClientAuth.WANT });

            // ClientAuth.NEED is most restrictive, failing in all cases except "good cert/bad host"
            // when host verification is disabled
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, false,
              validateClientHostnames, CertConfig.NO_CLIENT_CERT, X509Util.ClientAuth.NEED });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, false,
              validateClientHostnames, CertConfig.NON_VERIFIABLE_CERT, X509Util.ClientAuth.NEED });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, !validateClientHostnames,
              validateClientHostnames, CertConfig.VERIFIABLE_CERT_WITH_BAD_HOST,
              X509Util.ClientAuth.NEED });

            // additionally ensure that all modes succeed when a good cert is presented
            for (X509Util.ClientAuth mode : X509Util.ClientAuth.values()) {
              params.add(new Object[] { caKeyType, certKeyType, keyPassword, true,
                validateClientHostnames, CertConfig.GOOD_CERT, mode });
            }
          }
        }
      }
    }
    return params;
  }

  @Override
  protected void initialize(Configuration serverConf, Configuration clientConf)
    throws IOException, GeneralSecurityException, OperatorCreationException {
    // server enables client auth mode and verifies client host names
    // inject bad certs into client side
    serverConf.set(X509Util.HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE, clientAuthMode.name());
    serverConf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_VERIFY_CLIENT_HOSTNAME,
      validateHostnames);
    handleCertConfig(clientConf);
  }
}
