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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;

/** Authentication method */
@InterfaceAudience.Private
public enum AuthMethod {
  SIMPLE((byte) 80, "", UserGroupInformation.AuthenticationMethod.SIMPLE),
  KERBEROS((byte) 81, "GSSAPI", UserGroupInformation.AuthenticationMethod.KERBEROS),
  DIGEST((byte) 82, "DIGEST-MD5", UserGroupInformation.AuthenticationMethod.TOKEN);

  /** The code for this method. */
  public final byte code;
  public final String mechanismName;
  public final UserGroupInformation.AuthenticationMethod authenticationMethod;

  AuthMethod(byte code, String mechanismName,
             UserGroupInformation.AuthenticationMethod authMethod) {
    this.code = code;
    this.mechanismName = mechanismName;
    this.authenticationMethod = authMethod;
  }

  private static final int FIRST_CODE = values()[0].code;

  /** Return the object represented by the code. */
  public static AuthMethod valueOf(byte code) {
    final int i = (code & 0xff) - FIRST_CODE;
    return i < 0 || i >= values().length ? null : values()[i];
  }

  /** Return the SASL mechanism name */
  public String getMechanismName() {
    return mechanismName;
  }

  /** Read from in */
  public static AuthMethod read(DataInput in) throws IOException {
    return valueOf(in.readByte());
  }

  /** Write to out */
  public void write(DataOutput out) throws IOException {
    out.write(code);
  }
}
