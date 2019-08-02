/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.ClusterSchema;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for RSGroup implementation
 */
@InterfaceAudience.Private
final class RSGroupUtil {

  private static final Logger LOG = LoggerFactory.getLogger(RSGroupUtil.class);

  private RSGroupUtil() {
  }

  @FunctionalInterface
  private interface GetRSGroup {
    RSGroupInfo get(String groupName) throws IOException;
  }

  private static Optional<RSGroupInfo> getRSGroupInfo(MasterServices master, GetRSGroup getter,
      TableName tableName) throws IOException {
    TableDescriptor td = master.getTableDescriptors().get(tableName);
    if (td == null) {
      return Optional.empty();
    }
    Optional<String> optGroupNameOfTable = td.getRegionServerGroup();
    if (optGroupNameOfTable.isPresent()) {
      RSGroupInfo group = getter.get(optGroupNameOfTable.get());
      if (group != null) {
        return Optional.of(group);
      }
    }
    ClusterSchema clusterSchema = master.getClusterSchema();
    if (clusterSchema == null) {
      if (TableName.isMetaTableName(tableName)) {
        LOG.info("Can not get the namespace rs group config for meta table, since the" +
            " meta table is not online yet, will use default group to assign meta first");
      } else {
        LOG.warn("ClusterSchema is null, can only use default rsgroup, should not happen?");
      }
      return Optional.empty();
    }
    NamespaceDescriptor nd = clusterSchema.getNamespace(tableName.getNamespaceAsString());
    String groupNameOfNs = nd.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
    if (groupNameOfNs == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(getter.get(groupNameOfNs));
  }

  /**
   * Will try to get the rsgroup from {@link TableDescriptor} first, and then try to get the rsgroup
   * from the {@link NamespaceDescriptor}. If still not present, return empty.
   */
  static Optional<RSGroupInfo> getRSGroupInfo(MasterServices master, RSGroupInfoManager manager,
      TableName tableName) throws IOException {
    return getRSGroupInfo(master, manager::getRSGroup, tableName);
  }

  /**
   * Will try to get the rsgroup from {@link TableDescriptor} first, and then try to get the rsgroup
   * from the {@link NamespaceDescriptor}. If still not present, return empty.
   */
  static Optional<RSGroupInfo> getRSGroupInfo(MasterServices master, RSGroupAdmin admin,
      TableName tableName) throws IOException {
    return getRSGroupInfo(master, admin::getRSGroupInfo, tableName);
  }

  /**
   * Fill the tables field for {@link RSGroupInfo}, for backward compatibility.
   */
  @SuppressWarnings("deprecation")
  static RSGroupInfo fillTables(RSGroupInfo rsGroupInfo, Collection<TableDescriptor> tds) {
    RSGroupInfo newRsGroupInfo = new RSGroupInfo(rsGroupInfo);
    Predicate<TableDescriptor> filter;
    if (rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
      filter = td -> {
        Optional<String> optGroupName = td.getRegionServerGroup();
        return !optGroupName.isPresent() || optGroupName.get().equals(RSGroupInfo.DEFAULT_GROUP);
      };
    } else {
      filter = td -> {
        Optional<String> optGroupName = td.getRegionServerGroup();
        return optGroupName.isPresent() && optGroupName.get().equals(newRsGroupInfo.getName());
      };
    }
    tds.stream().filter(filter).map(TableDescriptor::getTableName)
        .forEach(newRsGroupInfo::addTable);
    return newRsGroupInfo;
  }
}
