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
package org.apache.hadoop.hbase.security.oauthbearer.internals.knox;

import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Exception thrown when token validation fails due to a problem with the token
 * itself (as opposed to a missing remote resource or a configuration problem)
 */
@InterfaceAudience.Public
public class OAuthBearerIllegalTokenException extends RuntimeException {
  private static final long serialVersionUID = -5275276640051316350L;
  private final OAuthBearerValidationResult reason;

  /**
   * Constructor
   *
   * @param reason
   *            the mandatory reason for the validation failure; it must indicate
   *            failure
   */
  public OAuthBearerIllegalTokenException(OAuthBearerValidationResult reason) {
    super(Objects.requireNonNull(reason, "Reason cannot be null").failureDescription());
    if (reason.success()) {
      throw new IllegalArgumentException(
        "The reason indicates success; it must instead indicate failure");
    }
    this.reason = reason;
  }

  public OAuthBearerIllegalTokenException(OAuthBearerValidationResult reason, Throwable t) {
    super(Objects.requireNonNull(reason, "Reason cannot be null").failureDescription(), t);
    if (reason.success()) {
      throw new IllegalArgumentException(
        "The reason indicates success; it must instead indicate failure");
    }
    this.reason = reason;
  }

  /**
   * Return the (always non-null) reason for the validation failure
   *
   * @return the reason for the validation failure
   */
  public OAuthBearerValidationResult reason() {
    return reason;
  }
}
