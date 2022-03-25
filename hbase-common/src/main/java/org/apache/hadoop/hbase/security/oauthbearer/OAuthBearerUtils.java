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
package org.apache.hadoop.hbase.security.oauthbearer;

import java.util.HashMap;
import java.util.Map;
import javax.security.sasl.Sasl;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class OAuthBearerUtils {
  public static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";
  public static final String TOKEN_KIND = "HBASE_JWT_TOKEN";

  /**
   * Verifies configuration for OAuth Bearer authentication mechanism.
   * Throws RuntimeException if PlainText is not allowed.
   */
  public static String[] mechanismNamesCompatibleWithPolicy(Map<String, ?> props) {
    if (props != null && "true".equals(String.valueOf(props.get(Sasl.POLICY_NOPLAINTEXT)))) {
      throw new RuntimeException("OAuth Bearer authentication mech cannot be used if plaintext is "
        + "disallowed.");
    }
    return new String[] { OAUTHBEARER_MECHANISM };
  }

  /**
   *  Converts an extensions string into a {@code Map<String, String>}.
   *
   *  Example:
   *      {@code parseMap("key=hey,keyTwo=hi,keyThree=hello", "=", ",") =>
   *      { key: "hey", keyTwo: "hi", keyThree: "hello" }}
   *
   */
  public static Map<String, String> parseMap(String mapStr,
    String keyValueSeparator, String elementSeparator) {
    Map<String, String> map = new HashMap<>();

    if (!mapStr.isEmpty()) {
      String[] attrvals = mapStr.split(elementSeparator);
      for (String attrval : attrvals) {
        String[] array = attrval.split(keyValueSeparator, 2);
        map.put(array[0], array[1]);
      }
    }
    return map;
  }

  private OAuthBearerUtils() {
    // empty
  }
}
