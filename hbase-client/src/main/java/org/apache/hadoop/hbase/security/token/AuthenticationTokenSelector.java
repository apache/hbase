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

package org.apache.hadoop.hbase.security.token;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

import java.util.Collection;

@InterfaceAudience.Private
public class AuthenticationTokenSelector
    implements TokenSelector<AuthenticationTokenIdentifier> {
  private static Log LOG = LogFactory.getLog(AuthenticationTokenSelector.class);

  public AuthenticationTokenSelector() {
  }

  @Override
  public Token<AuthenticationTokenIdentifier> selectToken(Text serviceName,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    if (serviceName != null) {
      for (Token ident : tokens) {
        if (serviceName.equals(ident.getService()) &&
            AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE.equals(ident.getKind())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Returning token "+ident);
          }
          return (Token<AuthenticationTokenIdentifier>)ident;
        }
      }
    }
    LOG.debug("No matching token found");
    return null;
  }
}
