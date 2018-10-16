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
package org.apache.hadoop.hbase.client.procedure;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hbase.thirdparty.com.google.protobuf.StringValue;

public class ShellTestProcedure extends Procedure<Object> implements TableProcedureInterface {
  private String tableNameString;

  public ShellTestProcedure() {
  }

  public ShellTestProcedure(String tableNameString) {
    setTableNameString(tableNameString);
  }

  public String getTableNameString() {
    return tableNameString;
  }

  public void setTableNameString(String tableNameString) {
    this.tableNameString = tableNameString;
  }

  @Override
  public TableName getTableName() {
    return TableName.valueOf(tableNameString);
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  protected Procedure<Object>[] execute(Object env)
      throws ProcedureYieldException, ProcedureSuspendedException,
      InterruptedException {
    return null;
  }

  @Override
  protected void rollback(Object env) throws IOException, InterruptedException {
  }

  @Override
  protected boolean abort(Object env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    StringValue message = StringValue.newBuilder().setValue(tableNameString).build();
    serializer.serialize(message);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    StringValue message = serializer.deserialize(StringValue.class);
    tableNameString = message.getValue();
  }
}
