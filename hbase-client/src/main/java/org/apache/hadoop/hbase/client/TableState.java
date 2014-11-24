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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

/**
 * Represents table state.
 */
@InterfaceAudience.Private
public class TableState {

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static enum State {
    ENABLED,
    DISABLED,
    DISABLING,
    ENABLING;

    /**
     * Covert from PB version of State
     *
     * @param state convert from
     * @return POJO
     */
    public static State convert(HBaseProtos.TableState.State state) {
      State ret;
      switch (state) {
      case ENABLED:
        ret = State.ENABLED;
        break;
      case DISABLED:
        ret = State.DISABLED;
        break;
      case DISABLING:
        ret = State.DISABLING;
        break;
      case ENABLING:
        ret = State.ENABLING;
        break;
      default:
        throw new IllegalStateException(state.toString());
      }
      return ret;
    }

    /**
     * Covert to PB version of State
     *
     * @return PB
     */
    public HBaseProtos.TableState.State convert() {
      HBaseProtos.TableState.State state;
      switch (this) {
      case ENABLED:
        state = HBaseProtos.TableState.State.ENABLED;
        break;
      case DISABLED:
        state = HBaseProtos.TableState.State.DISABLED;
        break;
      case DISABLING:
        state = HBaseProtos.TableState.State.DISABLING;
        break;
      case ENABLING:
        state = HBaseProtos.TableState.State.ENABLING;
        break;
      default:
        throw new IllegalStateException(this.toString());
      }
      return state;
    }

  }

  private final long timestamp;
  private final TableName tableName;
  private final State state;

  /**
   * Create instance of TableState.
   * @param state table state
   */
  public TableState(TableName tableName, State state, long timestamp) {
    this.tableName = tableName;
    this.state = state;
    this.timestamp = timestamp;
  }

  /**
   * Create instance of TableState with current timestamp
   *
   * @param tableName table for which state is created
   * @param state     state of the table
   */
  public TableState(TableName tableName, State state) {
    this(tableName, state, System.currentTimeMillis());
  }

  /**
   * @return table state
   */
  public State getState() {
    return state;
  }

  /**
   * Timestamp of table state
   *
   * @return milliseconds
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Table name for state
   *
   * @return milliseconds
   */
  public TableName getTableName() {
    return tableName;
  }

  /**
   * Check that table in given states
   * @param state state
   * @return true if satisfies
   */
  public boolean inStates(State state) {
    return this.state.equals(state);
  }

  /**
   * Check that table in given states
   * @param states state list
   * @return true if satisfies
   */
  public boolean inStates(State... states) {
    for (State s : states) {
      if (s.equals(this.state))
        return true;
    }
    return false;
  }


  /**
   * Covert to PB version of TableState
   * @return PB
   */
  public HBaseProtos.TableState convert() {
    return HBaseProtos.TableState.newBuilder()
        .setState(this.state.convert())
        .setTable(ProtobufUtil.toProtoTableName(this.tableName))
        .setTimestamp(this.timestamp)
            .build();
  }

  /**
   * Covert from PB version of TableState
   * @param tableState convert from
   * @return POJO
   */
  public static TableState convert(HBaseProtos.TableState tableState) {
    TableState.State state = State.convert(tableState.getState());
    return new TableState(ProtobufUtil.toTableName(tableState.getTable()),
        state, tableState.getTimestamp());
  }

  /**
   * Static version of state checker
   * @param state desired
   * @param target equals to any of
   * @return true if satisfies
   */
  public static boolean isInStates(State state, State... target) {
    for (State tableState : target) {
      if (state.equals(tableState))
        return true;
    }
    return false;
  }
}
