/**
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Query extends OperationWithAttributes {

  /**
   * Sets the authorizations to be used by this Query
   * @param authorizations
   */
  public void setAuthorizations(Authorizations authorizations) {
    this.setAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY, ProtobufUtil
        .toAuthorizations(authorizations).toByteArray());
  }

  /**
   * @return The authorizations this Query is associated with.
   * @throws DeserializationException
   */
  public Authorizations getAuthorizations() throws DeserializationException {
    byte[] authorizationsBytes = this.getAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY);
    if (authorizationsBytes == null) return null;
    return ProtobufUtil.toAuthorizations(authorizationsBytes);
  }
}
