/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.io.WritableFactories;

/**
 * Utilities for class manipulation.
 */
public class Classes {

  /**
   * Dynamic class loader to load filter/comparators
   */
  private final static ClassLoader CLASS_LOADER;

  static {
    ClassLoader parent = Classes.class.getClassLoader();
    Configuration conf = HBaseConfiguration.create();
    CLASS_LOADER = new DynamicClassLoader(conf, parent);
  }

  /**
   * Equivalent of {@link Class#forName(String)} which also returns classes for
   * primitives like <code>boolean</code>, etc.
   * 
   * @param className
   *          The name of the class to retrieve. Can be either a normal class or
   *          a primitive class.
   * @return The class specified by <code>className</code>
   * @throws ClassNotFoundException
   *           If the requested class can not be found.
   */
  public static Class<?> extendedForName(String className)
      throws ClassNotFoundException {
    Class<?> valueType;
    if (className.equals("boolean")) {
      valueType = boolean.class;
    } else if (className.equals("byte")) {
      valueType = byte.class;
    } else if (className.equals("short")) {
      valueType = short.class;
    } else if (className.equals("int")) {
      valueType = int.class;
    } else if (className.equals("long")) {
      valueType = long.class;
    } else if (className.equals("float")) {
      valueType = float.class;
    } else if (className.equals("double")) {
      valueType = double.class;
    } else if (className.equals("char")) {
      valueType = char.class;
    } else {
      valueType = Class.forName(className);
    }
    return valueType;
  }

  @SuppressWarnings("rawtypes")
  public static String stringify(Class[] classes) {
    StringBuilder buf = new StringBuilder();
    if (classes != null) {
      for (Class c : classes) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(c.getName());
      }
    } else {
      buf.append("NULL");
    }
    return buf.toString();
  }

  /**
   * Used to dynamically load a filter class.
   *
   * @param className the filter class name
   * @return a filter class
   * @throws ClassNotFoundException if couldn't find the class
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends Filter> getFilterClassByName(
      String className) throws ClassNotFoundException {
    return (Class<? extends Filter>) Class.forName(className, true, CLASS_LOADER);
  }

  /**
   * Used to dynamically load a filter class, and create a Writable filter.
   * This filter class most likely extends Configurable.
   *
   * @param className the filter class name.
   * @return a filter
   */
  public static Filter createWritableForName(String className) {
    try {
      Class<? extends Filter> clazz = getFilterClassByName(className);
      return (Filter)WritableFactories.newInstance(clazz, new Configuration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    }
  }

  /**
   * This method is almost the same as #createWritableForName, except
   * that this one doesn't expect the filter class to extends Configurable.
   *
   * @param className the filter class name.
   * @return a filter
   */
  public static Filter createForName(String className) {
    try {
      Class<? extends Filter> clazz = getFilterClassByName(className);
      return (Filter)clazz.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    } catch (InstantiationException e) {
      throw new RuntimeException("Couldn't instantiate " + className, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("No access to " + className, e);
    }
  }
}
