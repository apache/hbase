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

import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class SaslUtil {
  private static final Log log = LogFactory.getLog(SaslUtil.class);
  public static final String SASL_DEFAULT_REALM = "default";
  public static final int SWITCH_TO_SIMPLE_AUTH = -88;

  public enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");

    private final String saslQop;

    QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }

    public String getSaslQop() {
      return saslQop;
    }

    public boolean matches(String stringQop) {
      if (saslQop.equals(stringQop)) {
        log.warn("Use authentication/integrity/privacy as value for rpc protection "
            + "configurations instead of auth/auth-int/auth-conf.");
        return true;
      }
      return name().equalsIgnoreCase(stringQop);
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

  /**
   * Returns {@link org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection}
   * corresponding to the given {@code stringQop} value.
   * @throws IllegalArgumentException If stringQop doesn't match any QOP.
   */
  public static QualityOfProtection getQop(String stringQop) {
    for (QualityOfProtection qop : QualityOfProtection.values()) {
      if (qop.matches(stringQop)) {
        return qop;
      }
    }
    throw new IllegalArgumentException("Invalid qop: " +  stringQop
        + ". It must be one of 'authentication', 'integrity', 'privacy'.");
  }

  /**
   * @param rpcProtection Value of 'hbase.rpc.protection' configuration.
   * @return Map with values for SASL properties.
   */
  static Map<String, String> initSaslProperties(String rpcProtection) {
    String saslQop;
    if (rpcProtection.isEmpty()) {
      saslQop = QualityOfProtection.AUTHENTICATION.getSaslQop();
    } else {
      String[] qops = rpcProtection.split(",");
      StringBuilder saslQopBuilder = new StringBuilder();
      for (int i = 0; i < qops.length; ++i) {
        QualityOfProtection qop = getQop(qops[i]);
        saslQopBuilder.append(",").append(qop.getSaslQop());
      }
      saslQop = saslQopBuilder.substring(1);  // remove first ','
    }
    Map<String, String> saslProps = new TreeMap<>();
    saslProps.put(Sasl.QOP, saslQop);
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }
}
