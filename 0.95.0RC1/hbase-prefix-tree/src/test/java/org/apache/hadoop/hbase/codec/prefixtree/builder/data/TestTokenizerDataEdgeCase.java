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

package org.apache.hadoop.hbase.codec.prefixtree.builder.data;

import java.util.List;

import org.apache.hadoop.hbase.codec.prefixtree.builder.TestTokenizerData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestTokenizerDataEdgeCase implements TestTokenizerData {

  static List<byte[]> d = Lists.newArrayList();
  static {
    /*
     * tricky little combination because the acegi token will partially match abdfi, but when you
     * descend into abdfi, it will not fully match
     */
    List<String> s = Lists.newArrayList();
    s.add("abdfh");
    s.add("abdfi");
    s.add("acegi");
    d = Bytes.getUtf8ByteArrays(s);
  }

  @Override
  public List<byte[]> getInputs() {
    return d;
  }

  @Override
  public List<byte[]> getOutputs() {
    return d;
  }

}
