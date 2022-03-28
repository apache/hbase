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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for all Apache HBase, built-in {@link SaslAuthenticationProvider}'s to extend.
 *
 * HBase users should take care to note that this class (and its sub-classes) are marked with the
 * {@code InterfaceAudience.Private} annotation. These implementations are available for users to
 * read, copy, and modify, but should not be extended or re-used in binary form. There are no
 * compatibility guarantees provided for implementations of this class.
 */
@InterfaceAudience.Private
public abstract class BuiltInSaslAuthenticationProvider implements SaslAuthenticationProvider {

  public static final String AUTH_TOKEN_TYPE = "HBASE_AUTH_TOKEN";

  @Override
  public String getTokenKind() {
    return AUTH_TOKEN_TYPE;
  }
}
