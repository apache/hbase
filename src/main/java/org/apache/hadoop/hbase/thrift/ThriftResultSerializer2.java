/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.thrift;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * A wrapper class which loads <code>ThriftResultSerializer</code> class with a
 * <code>CuttingClassLoader</code>.
 *
 * Using PROTOCOL_CLASSPATH_KEY in configuration to specify paths to cut in
 * queue.
 */
public class ThriftResultSerializer2 implements Serializer<Object>,
    Configurable {
  ThriftResultSerializer instance;
  /**
   * Key to the configuration of classpath to cut
   */
  public static final String CUTTING_CLASSPATH_KEY =
      "hbase.thrift.result.serializer.cutting.classpath";

  @Override
  public void setConf(Configuration conf) {
    String classPath = conf.get(CUTTING_CLASSPATH_KEY, "");
    try {
      // make the URL array of cutting paths
      String[] parts = classPath.split(File.pathSeparator);
      URL[] urls = new URL[parts.length];
      for (int i = 0; i < parts.length; i++) {
        urls[i] = new File(parts[i]).toURI().toURL();
      }

      // Create the ClassLoader
      ClassLoader classLoader = new CuttingClassLoader(urls,
 this.getClass().getClassLoader());
      // and load the class.
      Class<?> cls = Class.forName(ThriftResultSerializer.class.getName(),
          true, classLoader);
      // Then create the new instance.
      instance = (ThriftResultSerializer) cls.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    instance.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return instance.getConf();
  }

  @Override
  public void open(OutputStream out) throws IOException {
    instance.open(out);
  }

  @Override
  public void serialize(Object t) throws IOException {
    instance.serialize(t);
  }

  @Override
  public void close() throws IOException {
    instance.close();
  }
}
