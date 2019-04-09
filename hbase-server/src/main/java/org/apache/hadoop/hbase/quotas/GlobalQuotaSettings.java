/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.List;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest.Builder;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;

/**
 * An object which captures all quotas types (throttle or space) for a subject (user, table, or
 * namespace). This is used inside of the HBase RegionServer to act as an analogy to the
 * ProtocolBuffer class {@link Quotas}.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC})
@InterfaceStability.Evolving
public abstract class GlobalQuotaSettings extends QuotaSettings {

  protected GlobalQuotaSettings(String userName, TableName tableName, String namespace,
      String regionServer) {
    super(userName, tableName, namespace, regionServer);
  }

  /**
   * Computes a list of QuotaSettings that present the complete quota state of the combination of
   * this user, table, and/or namespace. Beware in calling this method repeatedly as the
   * implementation of it may be costly.
   */
  public abstract List<QuotaSettings> getQuotaSettings();

  @Override
  public QuotaType getQuotaType() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void setupSetQuotaRequest(Builder builder) {
    // ThrottleSettings should be used instead for setting a throttle quota.
    throw new UnsupportedOperationException(
        "This class should not be used to generate a SetQuotaRequest.");
  }
}
