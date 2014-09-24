/**
 *
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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

@InterfaceAudience.Private
public class SaslUtil {
  public static final String SASL_DEFAULT_REALM = "default";
  public static final Map<String, String> SASL_PROPS =
      new TreeMap<String, String>();
  public static final int SWITCH_TO_SIMPLE_AUTH = -88;

  public static enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");

    public final String saslQop;

    private QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }

    public String getSaslQop() {
      return saslQop;
    }
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static String[] splitKerberosName(String fullName) {
    return fullName.split("[/@]");
  }

  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password)).toCharArray();
  }

  static void initSaslProperties(String rpcProtection) {
    QualityOfProtection saslQOP = QualityOfProtection.AUTHENTICATION;
    if (QualityOfProtection.INTEGRITY.name().toLowerCase()
        .equals(rpcProtection)) {
      saslQOP = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase().equals(
        rpcProtection)) {
      saslQOP = QualityOfProtection.PRIVACY;
    }

    SaslUtil.SASL_PROPS.put(Sasl.QOP, saslQOP.getSaslQop());
    SaslUtil.SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
  }
}
