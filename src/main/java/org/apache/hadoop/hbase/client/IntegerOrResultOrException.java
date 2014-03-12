/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

/**
 * This is used in the containers in {@link TMultiResponse}. Previously we used
 * Object as value, but since we are moving to Thrift serialization, we need to
 * serialize the containers. The Type specifies which value this wrapper is
 * holding: Integer, List<Result> or MultiResponseException. Previously Object
 * could represent either Integer, Exception or Result[].
 *
 */
@ThriftStruct
public class IntegerOrResultOrException {

  public enum Type {
    INTEGER, LIST_OF_RESULTS, EXCEPTION
  }

  private Integer integer;
  private ThriftHBaseException ex = null;
  private List<Result> results = null;
  private Type type = null;

  public IntegerOrResultOrException() {}

  @ThriftConstructor
  public IntegerOrResultOrException(@ThriftField(1) Integer integer,
      @ThriftField(2) ThriftHBaseException ex,
      @ThriftField(3) List<Result> results,
      @ThriftField(4) Type type) {
    this.type = type;
    if (type == Type.INTEGER) {
      this.integer = integer;
    } else if (type == Type.LIST_OF_RESULTS) {
      this.results = results;
    } else if (type == Type.EXCEPTION) {
      this.ex = ex;
    }
  }

  public IntegerOrResultOrException(Object obj) {
    if (obj instanceof Integer) {
      this.integer = (Integer) obj;
      this.type = Type.INTEGER;
    } else if (obj instanceof Exception) {
      this.ex = new ThriftHBaseException((Exception) obj);
      this.type = Type.EXCEPTION;
    } else if (obj instanceof Result[]) {
      Result[] resultArray = (Result[]) obj;
      this.results = new ArrayList<>();
      Collections.addAll(results, resultArray);
      this.type = Type.LIST_OF_RESULTS;
    } else {
      throw new IllegalArgumentException(obj.getClass().getCanonicalName()
          + " is not a supported type");
    }
  }

  public IntegerOrResultOrException(Integer integer) {
    this.integer = integer;
    this.type = Type.INTEGER;
  }

  public IntegerOrResultOrException(ThriftHBaseException ex) {
    this.ex = ex;
    this.type = Type.EXCEPTION;
  }

  public IntegerOrResultOrException(List<Result> results) {
    this.results = results;
    this.type = Type.LIST_OF_RESULTS;
  }

  @ThriftField(1)
  public Integer getInteger() {
    return integer;
  }

  @ThriftField(2)
  public ThriftHBaseException getEx() {
    return ex;
  }

  @ThriftField(3)
  public List<Result> getResults() {
    return results;
  }

  @ThriftField(4)
  public Type getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(integer, results, ex, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IntegerOrResultOrException other = (IntegerOrResultOrException) obj;
    if (ex == null) {
      if (other.ex != null)
        return false;
    } else if (!ex.equals(other.ex))
      return false;
    if (integer == null) {
      if (other.integer != null)
        return false;
    } else if (!integer.equals(other.integer))
      return false;
    if (results == null) {
      if (other.results != null)
        return false;
    } else if (!results.equals(other.results))
      return false;
    if (type != other.type)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "IntegerOrResultOrException [integer=" + integer + ", ex=" + ex
        + ", results=" + results + ", type=" + type + "]";
  }

  /**
   * Create IntegerOrResultOrException from Object
   * @param obj
   * @return
   */
  public static IntegerOrResultOrException createFromObject(Object obj) {
    return new IntegerOrResultOrException(obj);
  }

  /**
   * This is used when transforming TMultiResponse to MultiResponse
   *
   * @param ioroe
   */
  public static Object createObjectFromIntegerOrResultOrException(
      IntegerOrResultOrException ioroe) {
    if (ioroe.getType().equals(Type.INTEGER)) {
       return ioroe.getInteger();
    } else if (ioroe.getType().equals(Type.LIST_OF_RESULTS)) {
      List<Result> list = ioroe.getResults();
      return list.toArray(new Result[list.size()]);
    } else if (ioroe.getType().equals(Type.EXCEPTION)) {
      return ioroe.getEx().getServerJavaException();
    }
    return null;
  }
}
