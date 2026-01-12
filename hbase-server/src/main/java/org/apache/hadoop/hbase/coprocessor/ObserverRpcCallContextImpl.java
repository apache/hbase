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
package org.apache.hadoop.hbase.coprocessor;

import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ObserverRpcCallContextImpl implements ObserverRpcCallContext {
  private final User user;
  private final Map<String, byte[]> attributes;

  public ObserverRpcCallContextImpl(User user, Map<String, byte[]> attributes) {
    this.user = Objects.requireNonNull(user, "user must not be null.");
    this.attributes = Objects.requireNonNull(attributes, "attributes must not be null.");
  }

  @Override
  public User getUser() {
    return user;
  }

  @Override
  public Map<String, byte[]> getAttributes() {
    return attributes;
  }
}
