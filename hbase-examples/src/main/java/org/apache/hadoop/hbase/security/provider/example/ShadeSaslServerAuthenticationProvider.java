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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.AttemptingUserProvidingSaslServer;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

public class ShadeSaslServerAuthenticationProvider extends ShadeSaslAuthenticationProvider
    implements SaslServerAuthenticationProvider {

  private AtomicReference<UserGroupInformation> attemptingUser = new AtomicReference<>(null);

  @Override
  public AttemptingUserProvidingSaslServer createServer(Configuration conf,
      SecretManager<TokenIdentifier> secretManager, Map<String, String> saslProps)
          throws IOException {
    return new AttemptingUserProvidingSaslServer(
        new SaslPlainServer(
            new ShadeSaslServerCallbackHandler(attemptingUser)), () -> attemptingUser.get());
  }

  @Override
  public boolean supportsProtocolAuthentication() {
    return false;
  }

  @Override
  public UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
    return UserGroupInformation.createRemoteUser(authzId);
  }

  static class ShadeSaslServerCallbackHandler implements CallbackHandler {
    private final AtomicReference<UserGroupInformation> attemptingUser;

    public ShadeSaslServerCallbackHandler(AtomicReference<UserGroupInformation> attemptingUser) {
      this.attemptingUser = attemptingUser;
    }

    @Override public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL PLAIN Callback");
        }
      }

      if (nc != null && pc != null) {
        String username = nc.getName();
        UserGroupInformation ugi = createUgiForRemoteUser(username);
        char[] password = pc.getPassword();

        attemptingUser.set(ugi);

        // TODO validate
      }

      if (ac != null) {
        String authenticatedUserId = ac.getAuthenticationID();
        String userRequestedToExecuteAs = ac.getAuthorizationID();
        if (authenticatedUserId.equals(userRequestedToExecuteAs)) {
          ac.setAuthorized(true);
          ac.setAuthorizedID(userRequestedToExecuteAs);
        } else {
          ac.setAuthorized(false);
        }
      }
    }

    UserGroupInformation createUgiForRemoteUser(String username) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
      ugi.setAuthenticationMethod(ShadeSaslAuthenticationProvider.METHOD.getAuthMethod());
      return ugi;
    }
  }

}
