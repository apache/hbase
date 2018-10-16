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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.ipc.RemoteException;

/**
 * A {@link RemoteException} with some extra information.  If source exception
 * was a {@link org.apache.hadoop.hbase.DoNotRetryIOException}, 
 * {@link #isDoNotRetry()} will return true.
 * <p>A {@link RemoteException} hosts exceptions we got from the server.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class RemoteWithExtrasException extends RemoteException {
  private final String hostname;
  private final int port;
  private final boolean doNotRetry;

  /**
   * Dynamic class loader to load filter/comparators
   */
  private final static class ClassLoaderHolder {
    private final static ClassLoader CLASS_LOADER;

    static {
      ClassLoader parent = RemoteWithExtrasException.class.getClassLoader();
      Configuration conf = HBaseConfiguration.create();
      CLASS_LOADER = AccessController.doPrivileged((PrivilegedAction<ClassLoader>)
        () -> new DynamicClassLoader(conf, parent)
      );
    }
  }

  public RemoteWithExtrasException(String className, String msg, final boolean doNotRetry) {
    this(className, msg, null, -1, doNotRetry);
  }

  public RemoteWithExtrasException(String className, String msg, final String hostname,
      final int port, final boolean doNotRetry) {
    super(className, msg);
    this.hostname = hostname;
    this.port = port;
    this.doNotRetry = doNotRetry;
  }

  @Override
  public IOException unwrapRemoteException() {
    Class<?> realClass;
    try {
      // try to load a exception class from where the HBase classes are loaded or from Dynamic
      // classloader.
      realClass = Class.forName(getClassName(), false, ClassLoaderHolder.CLASS_LOADER);
    } catch (ClassNotFoundException cnfe) {
      try {
        // cause could be a hadoop exception, try to load from hadoop classpath
        realClass = Class.forName(getClassName(), false, super.getClass().getClassLoader());
      } catch (ClassNotFoundException e) {
        return new DoNotRetryIOException(
            "Unable to load exception received from server:" + e.getMessage(), this);
      }
    }
    try {
      return instantiateException(realClass.asSubclass(IOException.class));
    } catch (Exception e) {
      return new DoNotRetryIOException(
          "Unable to instantiate exception received from server:" + e.getMessage(), this);
    }
  }

  private IOException instantiateException(Class<? extends IOException> cls) throws Exception {
    Constructor<? extends IOException> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    IOException ex = cn.newInstance(this.getMessage());
    ex.initCause(this);
    return ex;
  }

  /**
   * @return null if not set
   */
  public String getHostname() {
    return this.hostname;
  }

  /**
   * @return -1 if not set
   */
  public int getPort() {
    return this.port;
  }

  /**
   * @return True if origin exception was a do not retry type.
   */
  public boolean isDoNotRetry() {
    return this.doNotRetry;
  }
}
