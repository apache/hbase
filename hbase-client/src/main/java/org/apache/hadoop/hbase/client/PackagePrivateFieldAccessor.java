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
package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A helper class used to access the package private field in o.a.h.h.client package.
 * <p>
 * This is because we share some data structures between client and server and the data structures
 * are marked as {@code InterfaceAudience.Public}, but we do not want to expose some of the fields
 * to end user.
 * <p>
 * TODO: A better solution is to separate the data structures used in client and server.
 */
@InterfaceAudience.Private
public class PackagePrivateFieldAccessor {

  public static void setMvccReadPoint(Scan scan, long mvccReadPoint) {
    scan.setMvccReadPoint(mvccReadPoint);
  }

  public static long getMvccReadPoint(Scan scan) {
    return scan.getMvccReadPoint();
  }
}
