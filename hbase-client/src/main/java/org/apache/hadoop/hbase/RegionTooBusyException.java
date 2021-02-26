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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown by a region server if it will block and wait to serve a request.
 * For example, the client wants to insert something to a region while the
 * region is compacting. Keep variance in the passed 'msg' low because its msg is used as a key
 * over in {@link org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException}
 * grouping failure types.
 */
@InterfaceAudience.Public
public class RegionTooBusyException extends IOException {
  private static final long serialVersionUID = 1728345723728342L;

  // Be careful. Keep variance in the passed 'msg' low because its msg is used as a key over in
  // RetriesExhaustedWithDetailsException grouping failure types.
  public RegionTooBusyException(final String msg) {
    super(msg);
  }
}
