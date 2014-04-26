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
import java.lang.reflect.Method;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ExceptionUtils;
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
  private static Log LOG = LogFactory.getLog(ThriftResultSerializer2.class);
  // ThriftResultSerializer instance;
  Object instance;
  /**
   * Key to the configuration of classpath to cut
   */
  public static final String CUTTING_CLASSPATH_KEY =
      "hbase.thrift.result.serializer.cutting.classpath";

  private Method getConfMethod;
  private Method openMethod;
  private Method serializeMethod;
  private Method closeMethod;

  @SuppressWarnings("resource")
  @Override
  public void setConf(Configuration conf) {
    String classPath = conf.get(CUTTING_CLASSPATH_KEY, "");
    LOG.info(CUTTING_CLASSPATH_KEY + " is set to " + classPath);
    try {
      // make the URL array of cutting paths
      String[] parts = classPath.split(File.pathSeparator);
      URL[] urls = new URL[parts.length];
      for (int i = 0; i < parts.length; i++) {
        urls[i] = new File(parts[i]).toURI().toURL();
      }

      // Create the ClassLoader
      // Skip all classes in the interface so that both side can use it.
      ClassLoader classLoader = new CuttingClassLoader(urls,
              ThriftResultSerializer.class.getClassLoader(),
              Result.class.getName(), ImmutableBytesWritable.class.getName(),
              KeyValue.class.getName(), Configuration.class.getName());
      // and load the class.
      Class<?> cls =
          classLoader.loadClass(ThriftResultSerializer.class.getName());
      // Then create the new instance.
      instance = cls.newInstance();
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }

    try {
      getConfMethod = instance.getClass().getMethod("getConf");
      openMethod = instance.getClass().getMethod("open", OutputStream.class);
      serializeMethod =
          instance.getClass().getMethod("serialize", Object.class);
      closeMethod = instance.getClass().getMethod("close");

      Method setConfMethod =
          instance.getClass().getMethod("setConf", Configuration.class);
      setConfMethod.invoke(instance, conf);
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    try {
      return (Configuration) getConfMethod.invoke(instance);
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }
  }

  @Override
  public void open(OutputStream out) throws IOException {
    try {
      openMethod.invoke(instance, out);
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }
  }

  @Override
  public void serialize(Object t) throws IOException {
    try {
      serializeMethod.invoke(instance, t);
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      closeMethod.invoke(instance);
    } catch (Exception e) {
      throw ExceptionUtils.toRuntimeException(e);
    }
  }

}
