/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.thrift;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.ExceptionUtils;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

/**
 * A serializer for use with TableInputFormat. Serializes results as TResult.
 */
public class ThriftResultSerializer implements Serializer<Object>, Configurable {

  private static final Log LOG = LogFactory.getLog(ThriftResultSerializer.class);

  public static final String PROTOCOL_CONF_KEY =
      "hbase.thrift.result.serializer.protocol.class";

  private OutputStream out;

  private Class<? extends TProtocol> protocolClass = TCompactProtocol.class;

  private TProtocol prot;
  private DataOutput dataOut;
  private TTransport transport;

  private Configuration conf;

  @Override
  public void open(OutputStream out) throws IOException {
    this.out = out;
    transport = new TIOStreamTransport(out);

    LOG.info("Using Thrift protocol: " + protocolClass.getName());

    try {
      Constructor<? extends TProtocol> constructor =
          protocolClass.getConstructor(TTransport.class);
      prot = constructor.newInstance(transport);
    } catch (Exception ex) {
      throw ExceptionUtils.toIOException(ex);
    }

    if (out instanceof DataOutput) {
      dataOut = (DataOutput) out;
    } else {
      dataOut = new DataOutputStream(out);
    }
  }

  @Override
  public void serialize(Object t) throws IOException {
    Class<?> klass = t.getClass();
    if (klass == Result.class) {
      Result result = (Result) t;
      TRowResult tResult = ThriftUtilities.oneRowResult(result);
      try {
        tResult.write(prot);
      } catch (TException e) {
        throw new IOException(e);
      }
    } else if (klass == ImmutableBytesWritable.class) {
      // This is used for the row.
      ImmutableBytesWritable imb = (ImmutableBytesWritable) t;
      imb.write(dataOut);
    } else {
      throw new IOException("Cannot serialize class " + klass.getName());
    }
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    String protoclCLassName =
        conf.get(PROTOCOL_CONF_KEY, protocolClass.getName());
    try {
      protocolClass =
          (Class<? extends TProtocol>) Class.forName(protoclCLassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Classloader of " + protocolClass.getName() + " is "
        + protocolClass.getClassLoader());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
