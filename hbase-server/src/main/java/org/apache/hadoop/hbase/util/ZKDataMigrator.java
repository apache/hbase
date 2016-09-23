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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * utlity method to migrate zookeeper data across HBase versions.
 */
@InterfaceAudience.Private
public class ZKDataMigrator {

  private static final Log LOG = LogFactory.getLog(ZKDataMigrator.class);

  /**
   * Method for table states migration.
   * Used when upgrading from pre-2.0 to 2.0
   * Reading state from zk, applying them to internal state
   * and delete.
   * Used by master to clean migration from zk based states to
   * table descriptor based states.
   */
  @Deprecated
  public static Map<TableName, TableState.State> queryForTableStates(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    Map<TableName, TableState.State> rv = new HashMap<>();
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, zkw.znodePaths.tableZNode);
    if (children == null)
      return rv;
    for (String child: children) {
      TableName tableName = TableName.valueOf(child);
      ZooKeeperProtos.DeprecatedTableState.State state = getTableState(zkw, tableName);
      TableState.State newState = TableState.State.ENABLED;
      if (state != null) {
        switch (state) {
        case ENABLED:
          newState = TableState.State.ENABLED;
          break;
        case DISABLED:
          newState = TableState.State.DISABLED;
          break;
        case DISABLING:
          newState = TableState.State.DISABLING;
          break;
        case ENABLING:
          newState = TableState.State.ENABLING;
          break;
        default:
        }
      }
      rv.put(tableName, newState);
    }
    return rv;
  }

  /**
   * Gets table state from ZK.
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return Null or {@link ZooKeeperProtos.DeprecatedTableState.State} found in znode.
   * @throws KeeperException
   */
  @Deprecated
  private static  ZooKeeperProtos.DeprecatedTableState.State getTableState(
      final ZooKeeperWatcher zkw, final TableName tableName)
      throws KeeperException, InterruptedException {
    String znode = ZKUtil.joinZNode(zkw.znodePaths.tableZNode, tableName.getNameAsString());
    byte [] data = ZKUtil.getData(zkw, znode);
    if (data == null || data.length <= 0) return null;
    try {
      ProtobufUtil.expectPBMagicPrefix(data);
      ZooKeeperProtos.DeprecatedTableState.Builder builder =
          ZooKeeperProtos.DeprecatedTableState.newBuilder();
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
      return builder.getState();
    } catch (IOException e) {
      KeeperException ke = new KeeperException.DataInconsistencyException();
      ke.initCause(e);
      throw ke;
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }

}
