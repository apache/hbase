/*
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
import java.lang.reflect.Constructor;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Checksum types. The Checksum type is a one byte number
 * that stores a representation of the checksum algorithm
 * used to encode a hfile. The ordinal of these cannot 
 * change or else you risk breaking all existing HFiles out there.
 */
public enum ChecksumType {

  NULL((byte)0) {
    @Override
    public String getName() {
      return "NULL";
    }
    @Override
    public void initialize() {
      // do nothing
    }
    @Override
    public Checksum getChecksumObject() throws IOException {
      return null; // checksums not used
    }
  },

  CRC32((byte)1) {
    private transient Constructor<?> ctor;

    @Override
    public String getName() {
      return "CRC32";
    }

    @Override
    public void initialize() {
      final String PURECRC32 = "org.apache.hadoop.util.PureJavaCrc32";
      final String JDKCRC = "java.util.zip.CRC32";
      LOG = LogFactory.getLog(ChecksumType.class);

      // check if hadoop library is available
      try {
        ctor = ChecksumFactory.newConstructor(PURECRC32);
        LOG.info("Checksum using " + PURECRC32);
      } catch (Exception e) {
        LOG.trace(PURECRC32 + " not available.");
      }
      try {
        // The default checksum class name is java.util.zip.CRC32. 
        // This is available on all JVMs.
        if (ctor == null) {
          ctor = ChecksumFactory.newConstructor(JDKCRC);
          LOG.info("Checksum can use " + JDKCRC);
        }
      } catch (Exception e) {
        LOG.trace(JDKCRC + " not available.");
      }
    }

    @Override
    public Checksum getChecksumObject() throws IOException {
      if (ctor == null) {
        throw new IOException("Bad constructor for " + getName());
      }
      try {
        return (Checksum)ctor.newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  },

  CRC32C((byte)2) {
    private transient Constructor<?> ctor;

    @Override
    public String getName() {
      return "CRC32C";
    }

    @Override
    public void initialize() {
      final String PURECRC32C = "org.apache.hadoop.util.PureJavaCrc32C";
      LOG = LogFactory.getLog(ChecksumType.class);
      try {
        ctor = ChecksumFactory.newConstructor(PURECRC32C);
        LOG.info("Checksum can use " + PURECRC32C);
      } catch (Exception e) {
        LOG.trace(PURECRC32C + " not available.");
      }
    }

    @Override
    public Checksum getChecksumObject() throws IOException {
      if (ctor == null) {
        throw new IOException("Bad constructor for " + getName());
      }
      try {
        return (Checksum)ctor.newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  };

  private final byte code;
  protected Log LOG;

  /** initializes the relevant checksum class object */
  abstract void initialize();

  /** returns the name of this checksum type */
  public abstract String getName();

  private ChecksumType(final byte c) {
    this.code = c;
    initialize();
  }

  /** returns a object that can be used to generate/validate checksums */
  public abstract Checksum getChecksumObject() throws IOException;

  public byte getCode() {
    return this.code;
  }

  /**
   * Cannot rely on enum ordinals . They change if item is removed or moved.
   * Do our own codes.
   * @param b
   * @return Type associated with passed code.
   */
  public static ChecksumType codeToType(final byte b) {
    for (ChecksumType t : ChecksumType.values()) {
      if (t.getCode() == b) {
        return t;
      }
    }
    throw new RuntimeException("Unknown checksum type code " + b);
  }

  /**
   * Map a checksum name to a specific type.
   * Do our own names.
   * @param name
   * @return Type associated with passed code.
   */
  public static ChecksumType nameToType(final String name) {
    for (ChecksumType t : ChecksumType.values()) {
      if (t.getName().equals(name)) {
        return t;
      }
    }
    throw new RuntimeException("Unknown checksum type name " + name);
  }
}
