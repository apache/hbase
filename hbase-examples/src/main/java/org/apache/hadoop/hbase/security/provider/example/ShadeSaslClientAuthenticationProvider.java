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
import java.net.InetAddress;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@InterfaceAudience.Private
public class ShadeSaslClientAuthenticationProvider extends ShadeSaslAuthenticationProvider
    implements SaslClientAuthenticationProvider {

  @Override
  public SaslClient createClient(Configuration conf, InetAddress serverAddr,
      SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
    return Sasl.createSaslClient(new String[] { getSaslAuthMethod().getSaslMechanism()}, null, null,
        SaslUtil.SASL_DEFAULT_REALM, saslProps, new ShadeSaslClientCallbackHandler(token));
  }

  @Override
  public UserInformation getUserInfo(User user) {
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    userInfoPB.setEffectiveUser(user.getUGI().getUserName());
    return userInfoPB.build();
  }

  static class ShadeSaslClientCallbackHandler implements CallbackHandler {
    private final String username;
    private final char[] password;
    public ShadeSaslClientCallbackHandler(
        Token<? extends TokenIdentifier> token) throws IOException {
      TokenIdentifier id = token.decodeIdentifier();
      if (id == null) {
        // Something is wrong with the environment if we can't get our Identifier back out.
        throw new IllegalStateException("Could not extract Identifier from Token");
      }
      this.username = id.getUser().getUserName();
      this.password = Bytes.toString(token.getPassword()).toCharArray();
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        nc.setName(username);
      }
      if (pc != null) {
        pc.setPassword(password);
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
