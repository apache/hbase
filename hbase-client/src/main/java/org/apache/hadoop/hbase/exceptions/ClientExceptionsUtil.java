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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
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
    return !isSpecialException(cur) || (cur instanceof RegionMovedException)
        || cur instanceof NotServingRegionException;
  }

  public static boolean isSpecialException(Throwable cur) {
    return (cur instanceof RegionMovedException || cur instanceof RegionOpeningException
        || cur instanceof RegionTooBusyException || cur instanceof ThrottlingException
        || cur instanceof MultiActionResultTooLarge || cur instanceof RetryImmediatelyException
        || cur instanceof CallQueueTooBigException || cur instanceof CallDroppedException
        || cur instanceof NotServingRegionException || cur instanceof RequestTooBigException);
  }


  /**
   * Look for an exception we know in the remote exception:
   * - hadoop.ipc wrapped exceptions
   * - nested exceptions
   *
   * Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException /
   *            ThrottlingException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (isSpecialException(cur)) {
        return cur;
      }
      if (cur instanceof RemoteException) {
        RemoteException re = (RemoteException) cur;
        cur = re.unwrapRemoteException();

        // unwrapRemoteException can return the exception given as a parameter when it cannot
        //  unwrap it. In this case, there is no need to look further
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

  /**
   * Check if the exception is something that indicates that we cannot
   * contact/communicate with the server.
   *
   * @param e exception to check
   * @return true when exception indicates that the client wasn't able to make contact with server
   */
  public static boolean isConnectionException(Throwable e) {
    if (e == null) {
      return false;
    }
    // This list covers most connectivity exceptions but not all.
    // For example, in SocketOutputStream a plain IOException is thrown
    // at times when the channel is closed.
    return (e instanceof SocketTimeoutException || e instanceof ConnectException
      || e instanceof ClosedChannelException || e instanceof SyncFailedException
      || e instanceof EOFException || e instanceof TimeoutException
      || e instanceof CallTimeoutException || e instanceof ConnectionClosingException
      || e instanceof FailedServerException);
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
