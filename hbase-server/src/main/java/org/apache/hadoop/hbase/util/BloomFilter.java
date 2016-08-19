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
package org.apache.hadoop.hbase.util;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 *
 * Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.
 * <p>
 * The Bloom filter is a data structure that was introduced in 1970 and that has
 * been adopted by the networking research community in the past decade thanks
 * to the bandwidth efficiencies that it offers for the transmission of set
 * membership information between networked hosts. A sender encodes the
 * information into a bit vector, the Bloom filter, that is more compact than a
 * conventional representation. Computation and space costs for construction are
 * linear in the number of elements. The receiver uses the filter to test
 * whether various elements are members of the set. Though the filter will
 * occasionally return a false positive, it will never return a false negative.
 * When creating the filter, the sender can choose its desired point in a
 * trade-off between the false positive rate and the size.
 *
 * <p>
 * Originally inspired by <a href="http://www.one-lab.org/">European Commission
 * One-Lab Project 034819</a>.
 *
 * Bloom filters are very sensitive to the number of elements inserted into
 * them. For HBase, the number of entries depends on the size of the data stored
 * in the column. Currently the default region size is 256MB, so entry count ~=
 * 256MB / (average value size for column). Despite this rule of thumb, there is
 * no efficient way to calculate the entry count after compactions. Therefore,
 * it is often easier to use a dynamic bloom filter that will add extra space
 * instead of allowing the error rate to grow.
 *
 * ( http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/BloomFilterSurvey
 * .pdf )
 *
 * m denotes the number of bits in the Bloom filter (bitSize) n denotes the
 * number of elements inserted into the Bloom filter (maxKeys) k represents the
 * number of hash functions used (nbHash) e represents the desired false
 * positive rate for the bloom (err)
 *
 * If we fix the error rate (e) and know the number of entries, then the optimal
 * bloom size m = -(n * ln(err) / (ln(2)^2) ~= n * ln(err) / ln(0.6185)
 *
 * The probability of false positives is minimized when k = m/n ln(2).
 *
 * @see BloomFilter The general behavior of a filter
 *
 * @see <a
 *      href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">
 *      Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
 *
 * @see BloomFilterWriter for the ability to add elements to a Bloom filter
 */
@InterfaceAudience.Private
public interface BloomFilter extends BloomFilterBase {

  /**
   * Check if the specified key is contained in the bloom filter.
   * @param keyCell the key to check for the existence of
   * @param bloom bloom filter data to search. This can be null if auto-loading
   *        is supported.
   * @param type The type of Bloom ROW/ ROW_COL
   * @return true if matched by bloom, false if not
   */
  boolean contains(Cell keyCell, ByteBuff bloom, BloomType type);

  /**
   * Check if the specified key is contained in the bloom filter.
   * @param buf data to check for existence of
   * @param offset offset into the data
   * @param length length of the data
   * @param bloom bloom filter data to search. This can be null if auto-loading
   *        is supported.
   * @return true if matched by bloom, false if not
   */
  boolean contains(byte[] buf, int offset, int length, ByteBuff bloom);

  /**
   * @return true if this Bloom filter can automatically load its data
   *         and thus allows a null byte buffer to be passed to contains()
   */
  boolean supportsAutoLoading();
}
