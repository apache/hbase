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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

@InterfaceAudience.Private
public class SaslUtil {
  private static final Log log = LogFactory.getLog(SaslUtil.class);
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

  /**
   * Returns {@link org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection}
   * corresponding to the given {@code stringQop} value. Returns null if value is
   * invalid.
   */
  public static QualityOfProtection getQop(String stringQop) {
    QualityOfProtection qop = null;
    if (QualityOfProtection.AUTHENTICATION.name().toLowerCase(Locale.ROOT).equals(stringQop)
        || QualityOfProtection.AUTHENTICATION.saslQop.equals(stringQop)) {
      qop = QualityOfProtection.AUTHENTICATION;
    } else if (QualityOfProtection.INTEGRITY.name().toLowerCase(Locale.ROOT).equals(stringQop)
        || QualityOfProtection.INTEGRITY.saslQop.equals(stringQop)) {
      qop = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase(Locale.ROOT).equals(stringQop)
        || QualityOfProtection.PRIVACY.saslQop.equals(stringQop)) {
      qop = QualityOfProtection.PRIVACY;
    }
    if (qop == null) {
      throw new IllegalArgumentException("Invalid qop: " +  stringQop
          + ". It must be one of 'authentication', 'integrity', 'privacy'.");
    }
    if (QualityOfProtection.AUTHENTICATION.saslQop.equals(stringQop)
        || QualityOfProtection.INTEGRITY.saslQop.equals(stringQop)
        || QualityOfProtection.PRIVACY.saslQop.equals(stringQop)) {
      log.warn("Use authentication/integrity/privacy as value for rpc protection "
          + "configurations instead of auth/auth-int/auth-conf.");
    }
    return qop;
  }

  static void initSaslProperties(String rpcProtection) {
    QualityOfProtection saslQOP = getQop(rpcProtection);
    if (saslQOP == null) {
      saslQOP = QualityOfProtection.AUTHENTICATION;
    }
    SaslUtil.SASL_PROPS.put(Sasl.QOP, saslQOP.getSaslQop());
    SaslUtil.SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
  }
}
