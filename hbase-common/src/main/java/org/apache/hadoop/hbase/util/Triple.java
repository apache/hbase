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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Utility class to manage a triple.
 */
@InterfaceAudience.Private
public class Triple<A, B, C> {
  private A first;
  private B second;
  private C third;
  // default constructor
  public Triple() {

  }

  public Triple(A first, B second, C third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  // ctor cannot infer types w/o warning but a method can.
  public static <A, B, C> Triple<A, B, C> create(A first, B second, C third) {
    return new Triple<A, B, C>(first, second, third);
  }

  public int hashCode() {
    int hashFirst = (first != null ? first.hashCode() : 0);
    int hashSecond = (second != null ? second.hashCode() : 0);
    int hashThird = (third != null ? third.hashCode() : 0);

    return (hashFirst >> 1) ^ hashSecond ^ (hashThird << 1);
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof Triple)) {
      return false;
    }

    Triple<?, ?, ?> otherTriple = (Triple<?, ?, ?>) obj;

    if (first != otherTriple.first && (first != null && !(first.equals(otherTriple.first))))
      return false;
    if (second != otherTriple.second && (second != null && !(second.equals(otherTriple.second))))
      return false;
    if (third != otherTriple.third && (third != null && !(third.equals(otherTriple.third))))
      return false;

    return true;
  }

  public String toString() {
    return "(" + first + ", " + second + "," + third + " )";
  }

  public A getFirst() {
    return first;
  }

  public void setFirst(A first) {
    this.first = first;
  }

  public B getSecond() {
    return second;
  }

  public void setSecond(B second) {
    this.second = second;
  }

  public C getThird() {
    return third;
  }

  public void setThird(C third) {
    this.third = third;
  }
}



