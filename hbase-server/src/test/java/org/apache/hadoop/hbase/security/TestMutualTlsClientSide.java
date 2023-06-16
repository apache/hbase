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
 * Comprehensively tests all permutations of certificate and host verification on the client side.
 * Tests each permutation of that against each value of {@link CertConfig}, i.e. passing a bad cert,
 * etc. See inline comments in {@link #data()} below for what the expectations are
 */
@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestMutualTlsClientSide extends AbstractTestMutualTls {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMutualTlsClientSide.class);

  @Parameterized.Parameters(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, "
    + "validateServerHostnames={3}, testCase={4}")
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (String keyPassword : new String[] { "", "pa$$w0rd" }) {
          // we want to run with and without validating hostnames. we encode the expected success
          // criteria in the TestCase config. See below.
          for (boolean validateServerHostnames : new Boolean[] { true, false }) {
            // fail for non-verifiable certs or certs with bad hostnames when validateServerHostname
            // is true. otherwise succeed.
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, false,
              validateServerHostnames, CertConfig.NON_VERIFIABLE_CERT });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, !validateServerHostnames,
              validateServerHostnames, CertConfig.VERIFIABLE_CERT_WITH_BAD_HOST });
            params.add(new Object[] { caKeyType, certKeyType, keyPassword, true,
              validateServerHostnames, CertConfig.GOOD_CERT });
          }
        }
      }
    }
    return params;
  }

  @Override
  protected void initialize(Configuration serverConf, Configuration clientConf)
    throws IOException, GeneralSecurityException, OperatorCreationException {
    // client verifies server hostname, and injects bad certs into server conf
    clientConf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_VERIFY_SERVER_HOSTNAME,
      validateHostnames);
    handleCertConfig(serverConf);
  }
}
