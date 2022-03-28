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
package org.apache.hadoop.hbase.security.provider;

import java.util.Optional;
import java.util.function.Supplier;

import javax.security.sasl.SaslServer;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Wrapper around a SaslServer which provides the last user attempting to authenticate via SASL,
 * if the server/mechanism allow figuring that out.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class AttemptingUserProvidingSaslServer {
  private final Supplier<UserGroupInformation> producer;
  private final SaslServer saslServer;

  public AttemptingUserProvidingSaslServer(
      SaslServer saslServer, Supplier<UserGroupInformation> producer) {
    this.saslServer = saslServer;
    this.producer = producer;
  }

  public SaslServer getServer() {
    return saslServer;
  }

  public Optional<UserGroupInformation> getAttemptingUser() {
    return Optional.ofNullable(producer.get());
  }
}
