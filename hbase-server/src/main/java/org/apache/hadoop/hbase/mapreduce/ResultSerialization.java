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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class ResultSerialization implements Serialization<Result> {
  @Override
  public boolean accept(Class<?> c) {
    return Result.class.isAssignableFrom(c);
  }

  @Override
  public Deserializer<Result> getDeserializer(Class<Result> c) {
    return new ResultDeserializer();
  }

  @Override
  public Serializer<Result> getSerializer(Class<Result> c) {
    return new ResultSerializer();
  }

  private static class ResultDeserializer implements Deserializer<Result> {
    private InputStream in;

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Result deserialize(Result mutation) throws IOException {
      ClientProtos.Result proto =
          ClientProtos.Result.parseDelimitedFrom(in);
      return ProtobufUtil.toResult(proto);
    }

    @Override
    public void open(InputStream in) throws IOException {
      this.in = in;
    }
    
  }
  private static class ResultSerializer implements Serializer<Result> {
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
    public void serialize(Result result) throws IOException {
      ProtobufUtil.toResult(result).writeDelimitedTo(out);
    }
  }
}
