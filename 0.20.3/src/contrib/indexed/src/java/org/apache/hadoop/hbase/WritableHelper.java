/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A collection of writable utils.
 */
public class WritableHelper {
  private WritableHelper() {
  }

  /**
   * Helper method to instantiate an expression instance using the provided
   * className.
   * @param className the class name
   * @param baseClass the base class type (the class must be or inherit from
   *                  this type)
   * @return the instance
   */
  @SuppressWarnings("unchecked")
  public static <T extends Writable> T instanceForName(String className, Class<T> baseClass) {
    try {
      Class<T> clazz = (Class<T>) Class.forName(className);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Can't find or instantiate class " + className, e);
    }
  }

  /**
   * Reads an instance of provided clazz (or one of it's subclasses) from the
   * provided data input.
   * @param in    the data into
   * @param clazz the class that the instance will be or extend from
   * @param <T>   the type
   * @return the instance
   * @throws IOException if an io error occurs
   */
  public static <T extends Writable> T readInstance(DataInput in, Class<T> clazz) throws IOException {
    String className = Bytes.toString(Bytes.readByteArray(in));
    T instance = instanceForName(className, clazz);
    instance.readFields(in);
    return instance;
  }

  /**
   * Reads an instance of provided clazz (or one of it's subclasses) from the
   * provided data input.
   * <p/>
   * <p>Note: It's assumed that the {@link #writeInstanceNullable(java.io.DataOutput,
   * org.apache.hadoop.io.Writable)} method was used to write out the instance.
   * @param in    the data into
   * @param clazz the class that the instance will be or extend from
   * @param <T>   the type
   * @return the instance (or null)
   * @throws IOException if an io error occurs
   */
  public static <T extends Writable> T readInstanceNullable(DataInput in, Class<T> clazz) throws IOException {
    if (in.readBoolean()) {
      return readInstance(in, clazz);
    } else {
      return null;
    }
  }

  /**
   * Writes out the provided writable instance to the data outout.
   * @param out      the data output
   * @param writable the writable isntance (must not be null)
   * @throws IOException if an io error occurs
   */
  public static void writeInstance(DataOutput out, Writable writable) throws IOException {
    if (writable == null) {
      throw new IllegalArgumentException("The writable instance must not be null");
    }
    Bytes.writeByteArray(out, Bytes.toBytes(writable.getClass().getName()));
    writable.write(out);
  }

  /**
   * Writes out the provided writable instance to the data outout.
   * @param out      the data output
   * @param writable the writable isntance (can be null)
   * @throws IOException if an io error occurs
   */
  public static void writeInstanceNullable(DataOutput out, Writable writable) throws IOException {
    if (writable == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      writeInstance(out, writable);
    }
  }
}
