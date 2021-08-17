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

package org.apache.hadoop.hbase.filter;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;


/**
 * This filter was deprecated in 2.0.0 and should be removed in 3.0.0. We keep the code here
 * to prevent the proto serialization exceptions puzzle those users who use older version clients
 * to communicate with newer version servers.
 *
 * @deprecated Deprecated in 2.0.0 and will be removed in 3.0.0.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-13347">HBASE-13347</a>
 */
@InterfaceAudience.Public
@Deprecated
public class FirstKeyValueMatchingQualifiersFilter extends FirstKeyOnlyFilter {

  /**
   * @param pbBytes A pb serialized {@link FirstKeyValueMatchingQualifiersFilter} instance
   * @return An instance of {@link FirstKeyValueMatchingQualifiersFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static FirstKeyValueMatchingQualifiersFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    throw new DeserializationException(
      "Stop using FirstKeyValueMatchingQualifiersFilter, which has been permanently removed");
  }
}
