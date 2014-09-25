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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

import com.google.protobuf.BlockingService;

import org.apache.hadoop.hbase.util.Bytes;
import org.cloudera.htrace.Trace;

import java.net.InetAddress;

/**
 * Represents client information (authenticated username, remote address, protocol)
 * for the currently executing request.  If called outside the context of a RPC request, all values
 * will be <code>null</code>. The {@link CallRunner} class before it a call and then on
 * its way out, it will clear the thread local.
 */
@InterfaceAudience.Private
public class RequestContext {
  private static ThreadLocal<RequestContext> instance =
      new ThreadLocal<RequestContext>() {
        protected RequestContext initialValue() {
          return new RequestContext(null, null, null);
        }
      };

  public static RequestContext get() {
    return instance.get();
  }


  /**
   * Returns the user credentials associated with the current RPC request or
   * <code>null</code> if no credentials were provided.
   * @return A User
   */
  public static User getRequestUser() {
    RequestContext ctx = instance.get();
    if (ctx != null) {
      return ctx.getUser();
    }
    return null;
  }

  /**
   * Returns the username for any user associated with the current RPC
   * request or <code>null</code> if no user is set.
   */
  public static String getRequestUserName() {
    User user = getRequestUser();
    if (user != null) {
      return user.getShortName();
    }
    return null;
  }

  /**
   * Indicates whether or not the current thread is within scope of executing
   * an RPC request.
   */
  public static boolean isInRequestContext() {
    RequestContext ctx = instance.get();
    if (ctx != null) {
      return ctx.isInRequest();
    }
    return false;
  }

  /**
   * Initializes the client credentials for the current request.
   * @param user
   * @param remoteAddress
   * @param service
   */
  public static void set(User user,
      InetAddress remoteAddress, BlockingService service) {
    RequestContext ctx = instance.get();
    ctx.user = user;
    ctx.remoteAddress = remoteAddress;
    ctx.service = service;
    ctx.inRequest = true;
    if (Trace.isTracing()) {
      if (user != null) {
        Trace.currentSpan().addKVAnnotation(Bytes.toBytes("user"), Bytes.toBytes(user.getName()));
      }
      if (remoteAddress != null) {
        Trace.currentSpan().addKVAnnotation(
            Bytes.toBytes("remoteAddress"),
            Bytes.toBytes(remoteAddress.getHostAddress()));
      }
    }
  }

  /**
   * Clears out the client credentials for a given request.
   */
  public static void clear() {
    RequestContext ctx = instance.get();
    ctx.user = null;
    ctx.remoteAddress = null;
    ctx.service = null;
    ctx.inRequest = false;
  }

  private User user;
  private InetAddress remoteAddress;
  private BlockingService service;
  // indicates we're within a RPC request invocation
  private boolean inRequest;

  private RequestContext(User user, InetAddress remoteAddr, BlockingService service) {
    this.user = user;
    this.remoteAddress = remoteAddr;
    this.service = service;
  }

  public User getUser() {
    return user;
  }

  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  public BlockingService getService() {
    return this.service;
  }

  boolean isInRequest() {
    return inRequest;
  }
}
