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

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Function to figure priority of incoming request.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public interface PriorityFunction {
  /**
   * Returns the 'priority type' of the specified request.
   * The returned value is mainly used to select the dispatch queue.
   * @param header
   * @param param
   * @return Priority of this request.
   */
  int getPriority(RequestHeader header, Message param);

  /**
   * Returns the deadline of the specified request.
   * The returned value is used to sort the dispatch queue.
   * @param header
   * @param param
   * @return Deadline of this request. 0 now, otherwise msec of 'delay'
   */
  long getDeadline(RequestHeader header, Message param);
}
