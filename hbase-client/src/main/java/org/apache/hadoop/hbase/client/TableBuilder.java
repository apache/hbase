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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * For creating {@link Table} instance.
 * <p>
 * The implementation should have default configurations set before returning the builder to user.
 * So users are free to only set the configurations they care about to create a new
 * Table instance.
 */
@InterfaceAudience.Public
public interface TableBuilder {

  /**
   * Set timeout for a whole operation such as get, put or delete. Notice that scan will not be
   * effected by this value, see scanTimeoutNs.
   * <p>
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   */
  TableBuilder setOperationTimeout(int timeout);

  /**
   * Set timeout for each rpc request.
   * <p>
   * Notice that this will <strong>NOT</strong> change the rpc timeout for read(get, scan) request
   * and write request(put, delete).
   */
  TableBuilder setRpcTimeout(int timeout);

  /**
   * Set timeout for each read(get, scan) rpc request.
   */
  TableBuilder setReadRpcTimeout(int timeout);

  /**
   * Set timeout for each write(put, delete) rpc request.
   */
  TableBuilder setWriteRpcTimeout(int timeout);

  /**
   * Create the {@link Table} instance.
   */
  Table build();
}
