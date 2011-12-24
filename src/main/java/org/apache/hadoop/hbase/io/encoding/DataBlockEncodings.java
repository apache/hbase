/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provide access to all data block encoding algorithms.
 */
public class DataBlockEncodings {

  /** Constructor. This class cannot be instantiated. */
  private DataBlockEncodings() {
  }

  /**
   * Algorithm type. All of the algorithms are required to have unique id which
   * should _NEVER_ be changed. If you want to add a new algorithm/version,
   * assign it a new id. Announce the new id in the HBase mailing list to
   * prevent collisions.
   */
  public static enum Algorithm {
    /**
     * Disable data block encoding.
     */
    NONE(0, null),
    BITSET(1, new BitsetKeyDeltaEncoder()),
    PREFIX(2, new PrefixKeyDeltaEncoder()),
    DIFF(3, new DiffKeyDeltaEncoder()),
    FAST_DIFF(4, new FastDiffDeltaEncoder());

    private final short id;
    private final byte[] idInBytes;
    private final DataBlockEncoder encoder;

    private Algorithm(int id, DataBlockEncoder encoder) {
      if (id < Short.MIN_VALUE || id > Short.MAX_VALUE) {
        throw new AssertionError(
            "Data block encoding algorithm id is out of range: " + id);
      }
      this.id = (short) id;
      this.idInBytes = Bytes.toBytes(this.id);
      if (idInBytes.length != HFileBlock.DATA_BLOCK_ENCODER_ID_SIZE) {
        // White this may seem redundant, if we accidentally serialize
        // the id as e.g. an int instead of a short, all encoders will break.
        throw new RuntimeException("Unexpected length of encoder ID byte " +
            "representation: " + Bytes.toStringBinary(idInBytes));
      }
      this.encoder = encoder;
    }

    /**
     * @return Name converted to bytes.
     */
    public byte[] getNameInBytes() {
      return Bytes.toBytes(toString());
    }

    /**
     * @return The id of a data block encoder.
     */
    public short getId() {
      return id;
    }

    /**
     * Writes id in bytes.
     * @param stream Where it should be written.
     */
    public void writeIdInBytes(OutputStream stream) throws IOException {
      stream.write(idInBytes);
    }

    /**
     * Return new data block encoder for given algorithm type.
     * @return data block encoder if algorithm is specified, null if none is
     *         selected.
     */
    public DataBlockEncoder getEncoder() {
      return encoder;
    }
  }

  /**
   * Maps encoding algorithm ids to algorithm instances for all algorithms in
   * the {@link Algorithm} enum.
   */
  private static final Map<Short, Algorithm> idToAlgorithm =
      new HashMap<Short, Algorithm>();

  /** Size of a delta encoding algorithm id */
  public static final int ID_SIZE = Bytes.SIZEOF_SHORT;

  static {
    for (Algorithm algo : Algorithm.values()) {
      if (idToAlgorithm.containsKey(algo.getId())) {
        throw new RuntimeException(String.format(
            "Two data block encoder algorithms '%s' and '%s' has same id '%d",
            idToAlgorithm.get(algo.getId()).toString(), algo.toString(),
            (int) algo.getId()));
      }
      idToAlgorithm.put(algo.getId(), algo);
    }
  }

  /**
   * Provide access to all data block encoders, even those which are not
   * exposed in the enum. Useful for testing and benchmarking.
   * @return list of all data block encoders.
   */
  public static List<DataBlockEncoder> getAllEncoders() {
    ArrayList<DataBlockEncoder> encoders = new ArrayList<DataBlockEncoder>();
    for (Algorithm algo : Algorithm.values()) {
      DataBlockEncoder encoder = algo.getEncoder();
      if (encoder != null) {
        encoders.add(encoder);
      }
    }

    // Add encoders that are only used in testing.
    encoders.add(new CopyKeyDataBlockEncoder());
    return encoders;
  }

  /**
   * Find and create data block encoder for given id;
   * @param encoderId id of data block encoder.
   * @return Newly created data block encoder.
   */
  public static DataBlockEncoder getDataBlockEncoderFromId(short encoderId) {
    if (!idToAlgorithm.containsKey(encoderId)) {
      throw new IllegalArgumentException(String.format(
          "There is no data block encoder for given id '%d'",
          (int) encoderId));
    }

    return idToAlgorithm.get(encoderId).getEncoder();
  }

  /**
   * Find and return name of data block encoder for given id.
   * @param encoderId id of data block encoder
   * @return name, same as used in options in column family
   */
  public static String getNameFromId(short encoderId) {
    return idToAlgorithm.get(encoderId).toString();
  }

  /**
   * Check if given encoder has this id.
   * @param encoder encoder which id will be checked
   * @param encoderId id which we except
   * @return true if id is right for given encoder, false otherwise
   * @exception IllegalArgumentException
   *            thrown when there is no matching data block encoder
   */
  public static boolean isCorrectEncoder(DataBlockEncoder encoder,
      short encoderId) {
    if (!idToAlgorithm.containsKey(encoderId)) {
      throw new IllegalArgumentException(String.format(
          "There is no data block encoder for given id '%d'",
          (int) encoderId));
    }

    Algorithm algorithm = idToAlgorithm.get(encoderId);
    return algorithm.getClass().equals(encoder.getClass());
  }

}
