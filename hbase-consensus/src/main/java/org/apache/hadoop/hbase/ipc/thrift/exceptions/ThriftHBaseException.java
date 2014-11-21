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
package org.apache.hadoop.hbase.ipc.thrift.exceptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

@ThriftStruct
public final class ThriftHBaseException extends Exception {

  private static final long serialVersionUID = -1294763299320737511L;

  public static final Log LOG = LogFactory.getLog(ThriftHBaseException.class);

  private String message;
  private String exceptionClass;
  private byte[] serializedServerJavaEx;

  /**
   * Swift Contructor used for serialization
   *
   * @param message - the message of the exception
   * @param exceptionClass - the class of which instance the exception is
   * @param serializedServerJavaEx - serialized java exception
   */
  @ThriftConstructor
  public ThriftHBaseException(@ThriftField(1) String message,
      @ThriftField(2) String exceptionClass,
      @ThriftField(3) byte[] serializedServerJavaEx) {
    this.message = message;
    this.exceptionClass = exceptionClass;
    this.serializedServerJavaEx = serializedServerJavaEx;
  }

  public ThriftHBaseException(){}

  public ThriftHBaseException(Exception serverJavaException) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oout = new ObjectOutputStream(out);
      oout.writeObject(serverJavaException);
      this.message = serverJavaException.getMessage();
      this.exceptionClass = serverJavaException.getClass().getCanonicalName();
      serializedServerJavaEx = out.toByteArray();
    } catch (IOException e) {
      // Should never happen in reality
      LOG.error("Exception happened during serialization of java server exception");
    }
  }

  public Exception getServerJavaException() {
    Exception ex = null;
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(serializedServerJavaEx);
      ObjectInputStream oin = new ObjectInputStream(in);
      ex = (Exception) oin.readObject();
    } catch (Exception e) {
      // Should never happen in reality
      LOG.error("Exception happened during serialization of java server exception");
    }
    return ex;
  }

  @ThriftField(1)
  public String getMessage() {
    return message;
  }

  @ThriftField(2)
  public String getExceptionClass() {
    return exceptionClass;
  }

  @ThriftField(3)
  public byte[] getSerializedServerJavaEx() {
    return serializedServerJavaEx;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(message, exceptionClass, serializedServerJavaEx);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (other == this) {
      return true;
    }

    if (other.getClass() != this.getClass()) {
      return false;
    }

    ThriftHBaseException otherException = (ThriftHBaseException) other;
    if (!this.getMessage().equals(otherException.getMessage())) {
      return false;
    }

    if (!this.getExceptionClass().equals(otherException.getExceptionClass())) {
      return false;
    }

    if (!Bytes.equals(this.getSerializedServerJavaEx(),
                      otherException.getSerializedServerJavaEx())) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return getClass() + " [message=" + message + ", exceptionClass="
        + exceptionClass + ", serializedServerJavaEx Hash="
        + Arrays.toString(serializedServerJavaEx).hashCode() + "]";
  }
}

