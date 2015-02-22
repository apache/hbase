/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.zip.Checksum;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Utility class that is used to generate a Checksum object.
 * The Checksum implementation is pluggable and an application
 * can specify their own class that implements their own
 * Checksum algorithm.
 */
@InterfaceAudience.Private
public class ChecksumFactory {

  static private final Class<?>[] EMPTY_ARRAY = new Class[]{};

  /**
   * Create a new instance of a Checksum object.
   * @return The newly created Checksum object
   */
  static public Checksum newInstance(String className) throws IOException {
    try {
      Class<?> clazz = getClassByName(className);
      return (Checksum)newInstance(clazz);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns a Constructor that can be used to create a Checksum object.
   * @param className classname for which an constructor is created
   * @return a new Constructor object
   */
  static public Constructor<?> newConstructor(String className)
    throws IOException {
    try {
      Class<?> clazz = getClassByName(className);
      Constructor<?> ctor = clazz.getDeclaredConstructor(EMPTY_ARRAY);
      ctor.setAccessible(true);
      return ctor;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (java.lang.NoSuchMethodException e) {
      throw new IOException(e);
    }
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @return a new object
   */
  static private <T> T newInstance(Class<T> theClass) {
    T result;
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(EMPTY_ARRAY);
      ctor.setAccessible(true);
      result = ctor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Load a class by name.
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  static private Class<?> getClassByName(String name)
    throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return Class.forName(name, true, classLoader);
  }
}
