/*
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * SlowLog params object that contains detailed info as params and region name : to be used for
 * filter purpose
 */
@InterfaceAudience.Private
public class SlowLogParams {

  private final String regionName;
  private final String params;
  private final ClientProtos.Scan scan;
  private final ClientProtos.MultiRequest multi;
  private final ClientProtos.Get get;
  private final ClientProtos.MutationProto mutate;

  public SlowLogParams(String regionName, String params, ClientProtos.Scan scan) {
    this.regionName = regionName;
    this.params = params;
    this.scan = scan;
    this.multi = null;
    this.get = null;
    this.mutate = null;
  }

  public SlowLogParams(String regionName, String params, ClientProtos.MultiRequest multi) {
    this.regionName = regionName;
    this.params = params;
    this.scan = null;
    this.multi = multi;
    this.get = null;
    this.mutate = null;
  }

  public SlowLogParams(String regionName, String params, ClientProtos.Get get) {
    this.regionName = regionName;
    this.params = params;
    this.scan = null;
    this.multi = null;
    this.get = get;
    this.mutate = null;
  }

  public SlowLogParams(String regionName, String params, ClientProtos.MutationProto mutate) {
    this.regionName = regionName;
    this.params = params;
    this.scan = null;
    this.multi = null;
    this.get = null;
    this.mutate = mutate;
  }

  public SlowLogParams(String regionName, String params) {
    this.regionName = regionName;
    this.params = params;
    this.scan = null;
    this.multi = null;
    this.get = null;
    this.mutate = null;
  }

  public SlowLogParams(String params) {
    this.regionName = StringUtils.EMPTY;
    this.params = params;
    this.scan = null;
    this.multi = null;
    this.get = null;
    this.mutate = null;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getParams() {
    return params;
  }

  public ClientProtos.Scan getScan() {
    return scan;
  }

  public ClientProtos.MultiRequest getMulti() {
    return multi;
  }

  public ClientProtos.Get getGet() {
    return get;
  }

  public ClientProtos.MutationProto getMutate() {
    return mutate;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("regionName", regionName).append("params", params)
      .append("scan", scan).append("multi", multi).append("get", get).append("mutate", mutate)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SlowLogParams)) {
      return false;
    }
    SlowLogParams that = (SlowLogParams) o;
    return new EqualsBuilder().append(regionName, that.regionName).append(params, that.params)
      .append(scan, that.scan).append(multi, that.multi).append(get, that.get)
      .append(mutate, that.mutate).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(regionName).append(params).append(scan).append(multi)
      .append(get).append(mutate).toHashCode();
  }
}
