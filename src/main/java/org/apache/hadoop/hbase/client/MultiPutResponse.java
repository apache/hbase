/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response class for MultiPut.
 */
@ThriftStruct
public class MultiPutResponse implements Writable {

  protected MultiPut request; // used in client code ONLY

  protected Map<byte[], Integer> answers = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

  public MultiPutResponse() {}

  @ThriftConstructor
  public MultiPutResponse(@ThriftField(1) final Map<byte[], Integer> answers) {
    // Adding it to the existing TreeMap, because we want to use the
    // BYTES_COMPARATOR.
    for (Map.Entry<byte[], Integer> e : answers.entrySet()) {
      this.answers.put(e.getKey(), e.getValue());
    }
  }

  @ThriftField(1)
  public Map<byte[], Integer> getAnswers() {
    return answers;
  }

  public void addResult(byte[] regionName, int result) {
    answers.put(regionName, result);
  }

  public Integer getAnswer(byte[] region) {
    return answers.get(region);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(answers.size());
    for( Map.Entry<byte[],Integer> e : answers.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      out.writeInt(e.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    answers.clear();

    int mapSize = in.readInt();
    for( int i = 0 ; i < mapSize ; i++ ) {
      byte[] key = Bytes.readByteArray(in);
      int value = in.readInt();

      answers.put(key, value);
    }
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MultiPutResponse other = (MultiPutResponse)obj;
    if ((other.answers == null) != (this.answers == null)) {
      return false;
    }
    if (this.answers != null) {
      // If the answers map is not null, they should be of the same size, and
      // have the same entries.
      if (!((this.answers.size() == other.answers.size()) &&
             this.answers.entrySet().containsAll(other.answers.entrySet()))) {
        return false;
      }
    }
    return true;
  }

  public int hashCode() {
    return answers.hashCode();
  }
}
