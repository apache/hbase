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
package org.apache.hadoop.hbase.security.auth;

import java.util.Objects;
import javax.security.auth.callback.Callback;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Optional callback used for SASL mechanisms if any extensions need to be set
 * in the SASL exchange.
 */
@InterfaceAudience.Public
public class SaslExtensionsCallback implements Callback {
  private SaslExtensions extensions = SaslExtensions.NO_SASL_EXTENSIONS;

  /**
   * Returns always non-null {@link SaslExtensions} consisting of the extension
   * names and values that are sent by the client to the server in the initial
   * client SASL authentication message. The default value is
   * {@link SaslExtensions#NO_SASL_EXTENSIONS} so that if this callback is
   * unhandled the client will see a non-null value.
   */
  public SaslExtensions extensions() {
    return extensions;
  }

  /**
   * Sets the SASL extensions on this callback.
   *
   * @param extensions
   *            the mandatory extensions to set
   */
  public void extensions(SaslExtensions extensions) {
    this.extensions = Objects.requireNonNull(extensions, "extensions must not be null");
  }
}
