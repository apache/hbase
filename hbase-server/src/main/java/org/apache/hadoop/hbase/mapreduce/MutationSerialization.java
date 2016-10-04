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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutationSerialization implements Serialization<Mutation> {
  @Override
  public boolean accept(Class<?> c) {
    return Mutation.class.isAssignableFrom(c);
  }

  @Override
  public Deserializer<Mutation> getDeserializer(Class<Mutation> c) {
    return new MutationDeserializer();
  }

  @Override
  public Serializer<Mutation> getSerializer(Class<Mutation> c) {
    return new MutationSerializer();
  }

  private static class MutationDeserializer implements Deserializer<Mutation> {
    private InputStream in;

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Mutation deserialize(Mutation mutation) throws IOException {
      MutationProto proto = MutationProto.parseDelimitedFrom(in);
      return ProtobufUtil.toMutation(proto);
    }

    @Override
    public void open(InputStream in) throws IOException {
      this.in = in;
    }
    
  }
  private static class MutationSerializer implements Serializer<Mutation> {
    private OutputStream out;

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public void open(OutputStream out) throws IOException {
      this.out = out;
    }

    @Override
    public void serialize(Mutation mutation) throws IOException {
      MutationType type;
      if (mutation instanceof Put) {
        type = MutationType.PUT;
      } else if (mutation instanceof Delete) {
        type = MutationType.DELETE;
      } else {
        throw new IllegalArgumentException("Only Put and Delete are supported");
      }
      ProtobufUtil.toMutation(type, mutation).writeDelimitedTo(out);
    }
  }
}
