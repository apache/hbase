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

package org.apache.hadoop.hbase.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * SlowLog params object that contains detailed info as params and region name : to be used
 * for filter purpose
 */
@InterfaceAudience.Private
public class SlowLogParams {

  private final String regionName;
  private final String params;

  public SlowLogParams(String regionName, String params) {
    this.regionName = regionName;
    this.params = params;
  }

  public SlowLogParams(String params) {
    this.regionName = StringUtils.EMPTY;
    this.params = params;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getParams() {
    return params;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("regionName", regionName)
      .append("params", params)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SlowLogParams that = (SlowLogParams) o;

    return new EqualsBuilder()
      .append(regionName, that.regionName)
      .append(params, that.params)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(regionName)
      .append(params)
      .toHashCode();
  }
}
