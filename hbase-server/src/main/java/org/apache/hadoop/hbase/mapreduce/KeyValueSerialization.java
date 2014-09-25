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
package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class KeyValueSerialization implements Serialization<KeyValue> {
  @Override
  public boolean accept(Class<?> c) {
    return KeyValue.class.isAssignableFrom(c);
  }

  @Override
  public KeyValueDeserializer getDeserializer(Class<KeyValue> t) {
    return new KeyValueDeserializer();
  }

  @Override
  public KeyValueSerializer getSerializer(Class<KeyValue> c) {
    return new KeyValueSerializer();
  }

  public static class KeyValueDeserializer implements Deserializer<KeyValue> {
    private DataInputStream dis;

    @Override
    public void close() throws IOException {
      this.dis.close();
    }

    @Override
    public KeyValue deserialize(KeyValue ignore) throws IOException {
      // I can't overwrite the passed in KV, not from a proto kv, not just yet.  TODO
      return KeyValue.create(this.dis);
    }

    @Override
    public void open(InputStream is) throws IOException {
      this.dis = new DataInputStream(is);
    }
  }

  public static class KeyValueSerializer implements Serializer<KeyValue> {
    private DataOutputStream dos;

    @Override
    public void close() throws IOException {
      this.dos.close();
    }

    @Override
    public void open(OutputStream os) throws IOException {
      this.dos = new DataOutputStream(os);
    }

    @Override
    public void serialize(KeyValue kv) throws IOException {
      KeyValue.write(kv, this.dos);
    }
  }
}
