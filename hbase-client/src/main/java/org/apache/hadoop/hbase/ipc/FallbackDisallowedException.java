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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Indicate that the rpc server tells client to fallback to simple auth but client is disabled to do
 * so.
 */
@InterfaceAudience.Public
public class FallbackDisallowedException extends HBaseIOException {

  private static final long serialVersionUID = -6942845066279358253L;

  public FallbackDisallowedException() {
    super("Server asks us to fall back to SIMPLE auth, "
        + "but this client is configured to only allow secure connections.");
  }
}
