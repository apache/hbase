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

import java.net.URL;
import java.net.URLClassLoader;

/**
 * The classloader allows the searching firstly in some path, if not found
 * search in parent.
 */
public class CuttingClassLoader extends URLClassLoader {
  public CuttingClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    // Find in loaded classes
    Class<?> clazz = findLoadedClass(name);
    if (clazz != null) {
      return clazz;
    }

    try {
      // Find in cutting paths
      return findClass(name);
    } catch (ClassNotFoundException e) {
    }

    // Find in parent
    return super.loadClass(name, resolve);
  }
}
