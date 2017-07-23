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

package org.apache.hadoop.hbase.chaos.actions;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.util.Random;

/**
 * Action that changes the compression algorithm on a column family from a list of tables.
 */
public class ChangeCompressionAction extends Action {
  private final TableName tableName;
  private final Random random;

  public ChangeCompressionAction(TableName tableName) {
    this.tableName = tableName;
    this.random = new Random();
  }

  @Override
  public void perform() throws IOException {
    // Possible compression algorithms. If an algorithm is not supported,
    // modifyTable will fail, so there is no harm.
    Algorithm[] possibleAlgos = Algorithm.values();

    // Since not every compression algorithm is supported,
    // let's use the same algorithm for all column families.

    // If an unsupported compression algorithm is chosen, pick a different one.
    // This is to work around the issue that modifyTable() does not throw remote
    // exception.
    Algorithm algo;
    do {
      algo = possibleAlgos[random.nextInt(possibleAlgos.length)];

      try {
        Compressor c = algo.getCompressor();

        // call returnCompressor() to release the Compressor
        algo.returnCompressor(c);
        break;
      } catch (Throwable t) {
        LOG.info("Performing action: Changing compression algorithms to " + algo +
            " is not supported, pick another one");
      }
    } while (true);

    final Algorithm chosenAlgo = algo; // for use in lambda
    LOG.debug("Performing action: Changing compression algorithms on "
      + tableName.getNameAsString() + " to " + chosenAlgo);
    modifyAllTableColumns(tableName, columnFamilyDescriptorBuilder -> {
      if (random.nextBoolean()) {
        columnFamilyDescriptorBuilder.setCompactionCompressionType(chosenAlgo);
      } else {
        columnFamilyDescriptorBuilder.setCompressionType(chosenAlgo);
      }
    });
  }
}
