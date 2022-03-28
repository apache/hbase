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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to acquire tokens for the ShadeSaslAuthenticationProvider.
 */
@InterfaceAudience.Private
public final class ShadeClientTokenUtil {

  private ShadeClientTokenUtil() {}

  public static Token<? extends TokenIdentifier> obtainToken(
      Connection conn, String username, char[] password) throws IOException {
    ShadeTokenIdentifier identifier = new ShadeTokenIdentifier(username);
    return new Token<>(identifier.getBytes(), Bytes.toBytes(new String(password)),
        new Text(ShadeSaslAuthenticationProvider.TOKEN_KIND),
        new Text(conn.getAdmin().getClusterMetrics().getClusterId()));
  }
}
