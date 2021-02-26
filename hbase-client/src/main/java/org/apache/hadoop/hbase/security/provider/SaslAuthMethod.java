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
package org.apache.hadoop.hbase.security.provider;

import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Describes the way in which some {@link SaslClientAuthenticationProvider} authenticates over SASL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class SaslAuthMethod {

  private final String name;
  private final byte code;
  private final String saslMech;
  private final AuthenticationMethod method;

  public SaslAuthMethod(String name, byte code, String saslMech, AuthenticationMethod method) {
    this.name = name;
    this.code = code;
    this.saslMech = saslMech;
    this.method = method;
  }

  /**
   * Returns the unique name to identify this authentication method among other HBase auth methods.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the unique value to identify this authentication method among other HBase auth methods.
   */
  public byte getCode() {
    return code;
  }

  /**
   * Returns the SASL mechanism used by this authentication method.
   */
  public String getSaslMechanism() {
    return saslMech;
  }

  /**
   * Returns the Hadoop {@link AuthenticationMethod} for this method.
   */
  public AuthenticationMethod getAuthMethod() {
    return method;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SaslAuthMethod)) {
      return false;
    }
    SaslAuthMethod other = (SaslAuthMethod) o;
    return Objects.equals(name, other.name) &&
        code == other.code &&
        Objects.equals(saslMech, other.saslMech) &&
        Objects.equals(method, other.method);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(name)
        .append(code)
        .append(saslMech)
        .append(method)
        .toHashCode();
  }
}
