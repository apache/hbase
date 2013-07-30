/*
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
package org.apache.hadoop.hbase.exceptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Subclass if the server knows the region is now on another server.
 * This allows the client to call the new region server without calling the master.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionOpeningException extends NotServingRegionException {
  private static final Log LOG = LogFactory.getLog(RegionOpeningException.class);
  private static final long serialVersionUID = -7232903522310558395L;

  public RegionOpeningException(String message) {
    super(message);
  }

  /**
   * Look for a RegionOpeningException in the exception:
   *  - hadoop.ipc wrapped exceptions
   *  - nested exceptions
   * Returns null if we didn't find the exception.
   * TODO: this code is mostly C/Ped from RegionMovedExecption. Due to the limitations of
   *       generics it's not amenable to generalizing without adding parameters/isAssignableFrom.
   *       Might make general if used in more places.
   */
  public static RegionOpeningException find(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    RegionOpeningException res = null;
    Throwable cur = (Throwable)exception;
    while (res == null && cur != null) {
      if (cur instanceof RegionOpeningException) {
        res = (RegionOpeningException) cur;
      } else {
        if (cur instanceof RemoteException) {
          RemoteException re = (RemoteException) cur;
          Exception e = re.unwrapRemoteException(RegionOpeningException.class);
          if (e == null) {
            e = re.unwrapRemoteException();
          }
          // unwrapRemoteException can return the exception given as a parameter when it cannot
          //  unwrap it. In this case, there is no need to look further
          // noinspection ObjectEquality
          if (e != re) {
            res = find(e);
          }
        }
        cur = cur.getCause();
      }
    }
    return res;
  }
}