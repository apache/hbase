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
package org.apache.hadoop.hbase.security.oauthbearer;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * The <code>b64token</code> value as defined in
 * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a> along with the
 * token's specific scope and lifetime and principal name.
 * <p/>
 * A network request would be required to re-hydrate an opaque token, and that could result in (for
 * example) an {@code IOException}, but retrievers for various attributes ({@link #lifetimeMs()},
 * etc.) declare no exceptions. Therefore, if a network request is required for any of these
 * retriever methods, that request could be performed at construction time so that the various
 * attributes can be reliably provided thereafter. For example, a constructor might declare
 * {@code throws IOException} in such a case. Alternatively, the retrievers could throw unchecked
 * exceptions.
 * <p/>
 * @see <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749 Section 1.4</a> and
 *      <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface OAuthBearerToken {
  /**
   * The <code>b64token</code> value as defined in
   * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
   * <p/>
   * @return <code>b64token</code> value as defined in
   *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
   */
  String value();

  /**
   * The token's lifetime, expressed as the number of milliseconds since the epoch, as per
   * <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749 Section 1.4</a>
   * <p/>
   * @return the token'slifetime, expressed as the number of milliseconds since the epoch, as per
   *         <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749 Section 1.4</a>.
   */
  long lifetimeMs();

  /**
   * The name of the principal to which this credential applies
   * <p/>
   * @return the always non-null/non-empty principal name
   */
  String principalName();
}
