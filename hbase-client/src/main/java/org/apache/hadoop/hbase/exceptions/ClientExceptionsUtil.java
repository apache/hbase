/*
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

package org.apache.hadoop.hbase.exceptions;

import java.io.EOFException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.ipc.RemoteException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ClientExceptionsUtil {

  private ClientExceptionsUtil() {}

  public static boolean isMetaClearingException(Throwable cur) {
    cur = findException(cur);

    if (cur == null) {
      return true;
    }
    return !isMetaCachePreservingException(cur);
  }

  public static boolean isRegionServerOverloadedException(Throwable t) {
    t = findException(t);
    return isInstanceOfRegionServerOverloadedException(t);
  }

  private static boolean isInstanceOfRegionServerOverloadedException(Throwable t) {
    return t instanceof CallQueueTooBigException || t instanceof CallDroppedException;
  }

  private static boolean isMetaCachePreservingException(Throwable t) {
    return t instanceof RegionOpeningException || t instanceof RegionTooBusyException
        || t instanceof RpcThrottlingException || t instanceof RetryImmediatelyException
        || t instanceof RequestTooBigException;
  }

  private static boolean isExceptionWeCare(Throwable t) {
    return isMetaCachePreservingException(t) || isInstanceOfRegionServerOverloadedException(t)
        || t instanceof NotServingRegionException;
  }

  /**
   * Look for an exception we know in the remote exception:
   * - hadoop.ipc wrapped exceptions
   * - nested exceptions
   *
   * Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException /
   *            RpcThrottlingException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (isExceptionWeCare(cur)) {
        return cur;
      }
      if (cur instanceof RemoteException) {
        RemoteException re = (RemoteException) cur;
        cur = re.unwrapRemoteException();

        // unwrapRemoteException can return the exception given as a parameter when it cannot
        // unwrap it. In this case, there is no need to look further
        // noinspection ObjectEquality
        if (cur == re) {
          return cur;
        }

        // When we receive RemoteException which wraps IOException which has a cause as
        // RemoteException we can get into infinite loop here; so if the cause of the exception
        // is RemoteException, we shouldn't look further.
      } else if (cur.getCause() != null && !(cur.getCause() instanceof RemoteException)) {
        cur = cur.getCause();
      } else {
        return cur;
      }
    }

    return null;
  }

  /**
   * Checks if the exception is CallQueueTooBig exception (maybe wrapped
   * into some RemoteException).
   * @param t exception to check
   * @return true if it's a CQTBE, false otherwise
   */
  public static boolean isCallQueueTooBigException(Throwable t) {
    t = findException(t);
    return (t instanceof CallQueueTooBigException);
  }

  /**
   * Checks if the exception is CallDroppedException (maybe wrapped
   * into some RemoteException).
   * @param t exception to check
   * @return true if it's a CQTBE, false otherwise
   */
  public static boolean isCallDroppedException(Throwable t) {
    t = findException(t);
    return (t instanceof CallDroppedException);
  }

  // This list covers most connectivity exceptions but not all.
  // For example, in SocketOutputStream a plain IOException is thrown at times when the channel is
  // closed.
  private static final ImmutableSet<Class<? extends Throwable>> CONNECTION_EXCEPTION_TYPES =
    ImmutableSet.of(SocketTimeoutException.class, ConnectException.class,
      ClosedChannelException.class, SyncFailedException.class, EOFException.class,
      TimeoutException.class, TimeoutIOException.class, CallTimeoutException.class,
      ConnectionClosingException.class, FailedServerException.class,
      ConnectionClosedException.class);

  /**
   * For test only. Usually you should use the {@link #isConnectionException(Throwable)} method
   * below.
   */
  @VisibleForTesting
  public static Set<Class<? extends Throwable>> getConnectionExceptionTypes() {
    return CONNECTION_EXCEPTION_TYPES;
  }

  /**
   * Check if the exception is something that indicates that we cannot contact/communicate with the
   * server.
   * @param e exception to check
   * @return true when exception indicates that the client wasn't able to make contact with server
   */
  public static boolean isConnectionException(Throwable e) {
    if (e == null) {
      return false;
    }
    for (Class<? extends Throwable> clazz : CONNECTION_EXCEPTION_TYPES) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Translates exception for preemptive fast fail checks.
   * @param t exception to check
   * @return translated exception
   * @throws IOException
   */
  public static Throwable translatePFFE(Throwable t) throws IOException {
    if (t instanceof NoSuchMethodError) {
      // We probably can't recover from this exception by retrying.
      throw (NoSuchMethodError) t;
    }

    if (t instanceof NullPointerException) {
      // The same here. This is probably a bug.
      throw (NullPointerException) t;
    }

    if (t instanceof UndeclaredThrowableException) {
      t = t.getCause();
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    return t;
  }
}
