package org.apache.hadoop.hbase.consensus.protocol;

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


import com.lmax.disruptor.EventFactory;

/**
 * Class which contains information about the transaction to commit. This will
 * be used by the Disruptor Producer.
 */
public class DataStoreCommitEvent {

  private Payload value;
  private long commitIndex;

  public DataStoreCommitEvent() {
    this.value = null;
  }

  public Payload getValue() {
    return value;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setValue(long commitIndex, Payload value) {
    this.commitIndex = commitIndex;
    this.value = value;
  }

  public final static EventFactory<DataStoreCommitEvent> EVENT_FACTORY =
    new EventFactory<DataStoreCommitEvent>() {
    public DataStoreCommitEvent newInstance() {
      return new DataStoreCommitEvent();
    }
  };
}
