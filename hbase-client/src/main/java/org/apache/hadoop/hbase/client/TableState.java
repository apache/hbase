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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

/**
 * Represents table state.
 */
@InterfaceAudience.Private
public class TableState {

  @InterfaceAudience.Private
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

  private final TableName tableName;
  private final State state;

  /**
   * Create instance of TableState.
   * @param tableName name of the table
   * @param state table state
   */
  public TableState(TableName tableName, State state) {
    this.tableName = tableName;
    this.state = state;
  }

  /**
   * @return table state
   */
  public State getState() {
    return state;
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
        .setState(this.state.convert()).build();
  }

  /**
   * Covert from PB version of TableState
   *
   * @param tableName table this state of
   * @param tableState convert from
   * @return POJO
   */
  public static TableState convert(TableName tableName, HBaseProtos.TableState tableState) {
    TableState.State state = State.convert(tableState.getState());
    return new TableState(tableName, state);
  }

  public static TableState parseFrom(TableName tableName, byte[] bytes)
      throws DeserializationException {
    try {
      return convert(tableName, HBaseProtos.TableState.parseFrom(bytes));
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableState that = (TableState) o;

    if (state != that.state) return false;
    if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (state != null ? state.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TableState{" +
        ", tableName=" + tableName +
        ", state=" + state +
        '}';
  }
}
