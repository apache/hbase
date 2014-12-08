/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMiniBatchOperationInProgress {

  @Test
  public void testMiniBatchOperationInProgressMethods() {
    Pair<Mutation, Integer>[] operations = new Pair[10];
    OperationStatus[] retCodeDetails = new OperationStatus[10];
    WALEdit[] walEditsFromCoprocessors = new WALEdit[10];
    for (int i = 0; i < 10; i++) {
      operations[i] = new Pair<Mutation, Integer>(new Put(Bytes.toBytes(i)), null);
    }
    MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatch = 
      new MiniBatchOperationInProgress<Pair<Mutation, Integer>>(operations, retCodeDetails, 
      walEditsFromCoprocessors, 0, 5);

    assertEquals(5, miniBatch.size());
    assertTrue(Bytes.equals(Bytes.toBytes(0), miniBatch.getOperation(0).getFirst().getRow()));
    assertTrue(Bytes.equals(Bytes.toBytes(2), miniBatch.getOperation(2).getFirst().getRow()));
    assertTrue(Bytes.equals(Bytes.toBytes(4), miniBatch.getOperation(4).getFirst().getRow()));
    try {
      miniBatch.getOperation(5);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    miniBatch.setOperationStatus(1, OperationStatus.FAILURE);
    assertEquals(OperationStatus.FAILURE, retCodeDetails[1]);
    try {
      miniBatch.setOperationStatus(6, OperationStatus.FAILURE);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    try {
      miniBatch.setWalEdit(5, new WALEdit());
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }

    miniBatch = new MiniBatchOperationInProgress<Pair<Mutation, Integer>>(operations,
        retCodeDetails, walEditsFromCoprocessors, 7, 10);
    try {
      miniBatch.setWalEdit(-1, new WALEdit());
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    try {
      miniBatch.getOperation(-1);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    try {
      miniBatch.getOperation(3);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    try {
      miniBatch.getOperationStatus(9);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    try {
      miniBatch.setOperationStatus(3, OperationStatus.FAILURE);
      fail("Should throw Exception while accessing out of range");
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    assertTrue(Bytes.equals(Bytes.toBytes(7), miniBatch.getOperation(0).getFirst().getRow()));
    assertTrue(Bytes.equals(Bytes.toBytes(9), miniBatch.getOperation(2).getFirst().getRow()));
    miniBatch.setOperationStatus(1, OperationStatus.SUCCESS);
    assertEquals(OperationStatus.SUCCESS, retCodeDetails[8]);
    WALEdit wal = new WALEdit();
    miniBatch.setWalEdit(0, wal);
    assertEquals(wal, walEditsFromCoprocessors[7]);
  }
}
