package org.apache.hadoop.hbase.consensus.quorum;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;

public class AppendResponseCallBack implements FutureCallback<AppendResponse>{
  private static Logger LOG = LoggerFactory.getLogger(AppendResponseCallBack.class);
  private ConsensusSession session;

  public AppendResponseCallBack(ConsensusSession session) {
    this.session = session;
  }

  public void onSuccess(AppendResponse response) {

  }

  @Override
  public void onFailure(Throwable arg0) {
  }
}


