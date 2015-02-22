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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class ResultSerialization extends Configured implements Serialization<Result> {
  private static final Log LOG = LogFactory.getLog(ResultSerialization.class);
  // The following configuration property indicates import file format version.
  public static final String IMPORT_FORMAT_VER = "hbase.import.version";

  @Override
  public boolean accept(Class<?> c) {
    return Result.class.isAssignableFrom(c);
  }

  @Override
  public Deserializer<Result> getDeserializer(Class<Result> c) {
    // check input format version
    Configuration conf = getConf();
    if (conf != null) {
      String inputVersion = conf.get(IMPORT_FORMAT_VER);
      if (inputVersion != null && inputVersion.equals("0.94")) {
        LOG.info("Load exported file using deserializer for HBase 0.94 format");
        return new Result94Deserializer();
      }
    }

    return new ResultDeserializer();
  }

  @Override
  public Serializer<Result> getSerializer(Class<Result> c) {
    return new ResultSerializer();
  }

  /**
   * The following deserializer class is used to load exported file of 0.94
   */
  private static class Result94Deserializer implements Deserializer<Result> {
    private DataInputStream in;

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Result deserialize(Result mutation) throws IOException {
      int totalBuffer = in.readInt();
      if (totalBuffer == 0) {
        return Result.EMPTY_RESULT;
      }
      byte[] buf = new byte[totalBuffer];
      readChunked(in, buf, 0, totalBuffer);
      List<Cell> kvs = new ArrayList<Cell>();
      int offset = 0;
      while (offset < totalBuffer) {
        int keyLength = Bytes.toInt(buf, offset);
        offset += Bytes.SIZEOF_INT;
        kvs.add(new KeyValue(buf, offset, keyLength));
        offset += keyLength;
      }
      return Result.create(kvs);
    }

    @Override
    public void open(InputStream in) throws IOException {
      if (!(in instanceof DataInputStream)) {
        throw new IOException("Wrong input stream instance passed in");
      }
      this.in = (DataInputStream) in;
    }

    private void readChunked(final DataInput in, byte[] dest, int ofs, int len) throws IOException {
      int maxRead = 8192;

      for (; ofs < len; ofs += maxRead)
        in.readFully(dest, ofs, Math.min(len - ofs, maxRead));
    }
  }

  private static class ResultDeserializer implements Deserializer<Result> {
    private InputStream in;

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Result deserialize(Result mutation) throws IOException {
      ClientProtos.Result proto = ClientProtos.Result.parseDelimitedFrom(in);
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
