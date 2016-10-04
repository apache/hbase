/**
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;


/**
 * Represents a coprocessor service method execution against a single region.  While coprocessor
 * service calls are performed against a region, this class implements {@link Row} in order to
 * make use of the AsyncProcess framework for batching multi-region calls per region server.
 *
 * <p><b>Note:</b> This class should not be instantiated directly.  Use
 * HTable#batchCoprocessorService instead.</p>
 */
@InterfaceAudience.Private
public class RegionCoprocessorServiceExec implements Row {

  /*
   * This duplicates region name in MultiAction, but allows us to easily access the region name in
   * the AsyncProcessCallback context.
   */
  private final byte[] region;
  private final byte[] startKey;
  private final MethodDescriptor method;
  private final Message request;

  public RegionCoprocessorServiceExec(byte[] region, byte[] startKey,
      MethodDescriptor method, Message request) {
    this.region = region;
    this.startKey = startKey;
    this.method = method;
    this.request = request;
  }

  @Override
  public byte[] getRow() {
    return startKey;
  }

  public byte[] getRegion() {
    return region;
  }

  public MethodDescriptor getMethod() {
    return method;
  }

  public Message getRequest() {
    return request;
  }

  @Override
  public int compareTo(Row o) {
    int res = Bytes.compareTo(this.getRow(), o.getRow());
    if ((o instanceof RegionCoprocessorServiceExec) && res == 0) {
      RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) o;
      res = method.getFullName().compareTo(exec.getMethod().getFullName());
      if (res == 0) {
        res = Bytes.compareTo(request.toByteArray(), exec.getRequest().toByteArray());
      }
    }
    return res;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(Bytes.hashCode(this.getRow()), method.getFullName(), request);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    return compareTo(other) == 0;
  }
}
