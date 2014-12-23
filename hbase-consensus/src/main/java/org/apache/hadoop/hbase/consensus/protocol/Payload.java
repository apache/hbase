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


import com.google.common.util.concurrent.SettableFuture;
import java.nio.ByteBuffer;

/**
 * Wrap the data to commit into a object along with the result.
 */
public class Payload {
  final ByteBuffer entries;
  final SettableFuture<Long> result;

  public Payload(ByteBuffer entries, SettableFuture<Long> result) {
    this.entries = entries;
    this.result = result;
  }

  public ByteBuffer getEntries() {
    return entries;
  }

  public SettableFuture<Long> getResult() {
    return result;
  }
}
