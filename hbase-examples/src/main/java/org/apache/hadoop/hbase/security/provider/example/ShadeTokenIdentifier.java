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
package org.apache.hadoop.hbase.security.provider.example;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ShadeTokenIdentifier extends TokenIdentifier {
  private static final Text TEXT_TOKEN_KIND = new Text(ShadeSaslAuthenticationProvider.TOKEN_KIND);
  private String username;

  public ShadeTokenIdentifier() {
    // for ServiceLoader
  }

  public ShadeTokenIdentifier(String username) {
    this.username = requireNonNull(username);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(username);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    username = in.readUTF();
  }

  @Override
  public Text getKind() {
    return TEXT_TOKEN_KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    if (username == null || "".equals(username)) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(username);
  }
}
