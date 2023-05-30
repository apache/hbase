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
package org.apache.hadoop.hbase.ipc;

import java.util.Comparator;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class StealRpcJobQueue extends StealJobQueue<CallRunner> {

  public final static RpcComparator RPCCOMPARATOR = new RpcComparator();

  public StealRpcJobQueue(int initCapacity, int stealFromQueueInitCapacity) {
    super(initCapacity, stealFromQueueInitCapacity, RPCCOMPARATOR);
  }

  public static class RpcComparator implements Comparator<CallRunner> {
    public RpcComparator() {
      super();
    }

    @Override
    public int compare(CallRunner o1, CallRunner o2) {
      long diff = o1.getRpcCall().getReceiveTime() - o2.getRpcCall().getReceiveTime();
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
