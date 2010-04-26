/*
 * Copyright 2009 The Apache Software Foundation
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

import java.io.Serializable;
import java.util.Arrays;

/**
 * A generic class for immutable pairs.
 * @param <T1>
 * @param <T2>
 */
public final class Pair<T1, T2> implements Serializable
{
  private static final long serialVersionUID = -3986244606585552569L;
  protected T1 first = null;
  protected T2 second = null;
  private int hashcode;


  /**
   * Constructor
   * @param a
   * @param b
   */
  public Pair(T1 a, T2 b)
  {
    this.first = a;
    this.second = b;

    // generate a hash code
    hashcode = first != null ? generateHashCode(first) : 0;
    hashcode = 31 * hashcode + (second != null ? generateHashCode(second) : 0);
  }

  private static int generateHashCode(Object o) {
    if (o.getClass().isArray()) {
      if (o instanceof long[]) {
          return Arrays.hashCode((long[]) o);
      } else if (o instanceof int[]) {
          return Arrays.hashCode((int[]) o);
      } else if (o instanceof short[]) {
          return Arrays.hashCode((short[]) o);
      } else if (o instanceof char[]) {
          return Arrays.hashCode((char[]) o);
      } else if (o instanceof byte[]) {
          return Arrays.hashCode((byte[]) o);
      } else if (o instanceof double[]) {
          return Arrays.hashCode((double[]) o);
      } else if (o instanceof float[]) {
          return Arrays.hashCode((float[]) o);
      } else if (o instanceof boolean[]) {
          return Arrays.hashCode((boolean[]) o);
      } else {
          // Not an array of primitives
          return Arrays.hashCode((Object[]) o);
      }
    } else {
      // Standard comparison
      return o.hashCode();
    }
  }

  /**
   * Return the first element stored in the pair.
   * @return T1
   */
  public T1 getFirst()
  {
    return first;
  }

  /**
   * Return the second element stored in the pair.
   * @return T2
   */
  public T2 getSecond()
  {
    return second;
  }

  /**
   * Creates a new instance of the pair encapsulating the supplied values.
   *
   * @param one  the first value
   * @param two  the second value
   * @param <T1> the type of the first element.
   * @param <T2> the type of the second element.
   * @return the new instance
   */
  public static <T1, T2> Pair<T1, T2> of(T1 one, T2 two)
  {
    return new Pair<T1, T2>(one, two);
  }

  private static boolean equals(Object x, Object y)
  {
    // Null safe compare first
    if (x == null || y == null) {
      return x == y;
    }

    Class clazz = x.getClass();
    // If they are both the same type of array
    if (clazz.isArray() && clazz == y.getClass()) {
      // NOTE: this section is borrowed from EqualsBuilder in commons-lang
      if (x instanceof long[]) {
          return Arrays.equals((long[]) x, (long[]) y);
      } else if (x instanceof int[]) {
          return Arrays.equals((int[]) x, (int[]) y);
      } else if (x instanceof short[]) {
          return Arrays.equals((short[]) x, (short[]) y);
      } else if (x instanceof char[]) {
          return Arrays.equals((char[]) x, (char[]) y);
      } else if (x instanceof byte[]) {
          return Arrays.equals((byte[]) x, (byte[]) y);
      } else if (x instanceof double[]) {
          return Arrays.equals((double[]) x, (double[]) y);
      } else if (x instanceof float[]) {
          return Arrays.equals((float[]) x, (float[]) y);
      } else if (x instanceof boolean[]) {
          return Arrays.equals((boolean[]) x, (boolean[]) y);
      } else {
          // Not an array of primitives
          return Arrays.deepEquals((Object[]) x, (Object[]) y);
      }
    } else {
      // Standard comparison
      return x.equals(y);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other)
  {
    return other instanceof Pair && equals(first, ((Pair)other).first) &&
      equals(second, ((Pair)other).second);
  }

  @Override
  public int hashCode()
  {
    return hashcode;
  }

  @Override
  public String toString()
  {
    return "{" + getFirst() + "," + getSecond() + "}";
  }
}