/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Codec for endpoint that convert between types and byte arrays.
 *
 * TODO add testcase for this class.
 */
public class EndpointBytesCodec {

  /**
   * The interface of decoder.
   */
  public interface IBytesDecoder {
    /**
     * Decodes a byte array into an object.
     */
    Object decode(byte[] bytes);
  }

  /**
   * The interface of encoder.
   */
  public interface IBytesEncoder {
    /**
     * Encode an object into bytes.
     */
    public byte[] encode(Object obj);
  }

  /**
   * Mapping from class types to IBytesDecoders.
   */
  private static Map<Class<?>, IBytesDecoder> decoders = new HashMap<>();

  /**
   * Mapping from class types to IBytesEncoders.
   */
  private static Map<Class<?>, IBytesEncoder> encoders = new HashMap<>();

  /**
   * Connects an encoder and a decoder with some classes.
   *
   * @param classes All classes in this array will be connected to the codecs.
   */
  private static void addCodec(Class<?>[] classes, IBytesEncoder enc,
      IBytesDecoder dec) {
    for (Class<?> cls : classes) {
      encoders.put(cls, enc);
      decoders.put(cls, dec);
    }
  }

  private static final byte[] BYTES_FALSE = { 0 };
  private static final byte[] BYTES_TRUE = { 1 };

  static {
    addCodec(new Class<?>[] { byte[].class }, new IBytesEncoder() {
      @Override
      public byte[] encode(Object obj) {
        return (byte[]) obj;
      }
    }, new IBytesDecoder() {
      @Override
      public Object decode(byte[] param) {
        if (param == null) {
          return HConstants.EMPTY_BYTE_ARRAY;
        }
        return param;
      }
    });
    addCodec(new Class<?>[] { String.class }, new IBytesEncoder() {
      @Override
      public byte[] encode(Object obj) {
        return Bytes.toBytes((String) obj);
      }
    }, new IBytesDecoder() {
      @Override
      public Object decode(byte[] param) {
        return Bytes.toString(param);
      }
    });
    addCodec(new Class<?>[] { Boolean.class, boolean.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return ((Boolean) obj) ? BYTES_TRUE : BYTES_FALSE;
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return param[0] != 0;
          }
        });
    addCodec(new Class<?>[] { Byte.class, byte.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return new byte[]{(Byte) obj};
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return param[0];
          }
        });
    addCodec(new Class<?>[] { Character.class, char.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((short) ((Character) obj).charValue());
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return (char) Bytes.toShort(param);
          }
        });
    addCodec(new Class<?>[] { Short.class, short.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((Short) obj);
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return Bytes.toShort(param);
          }
        });
    addCodec(new Class<?>[] { Integer.class, int.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((Integer) obj);
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return Bytes.toInt(param);
          }
        });
    addCodec(new Class<?>[] { Long.class, long.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((Long) obj);
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return Bytes.toLong(param);
          }
        });
    addCodec(new Class<?>[] { Float.class, float.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((Float) obj);
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return Bytes.toFloat(param);
          }
        });
    addCodec(new Class<?>[] { Double.class, double.class },
        new IBytesEncoder() {
          @Override
          public byte[] encode(Object obj) {
            return Bytes.toBytes((Double) obj);
          }
        }, new IBytesDecoder() {
          @Override
          public Object decode(byte[] param) {
            return Bytes.toDouble(param);
          }
        });
  }

  /**
   * @returns an IBytesDecoder for a specified type. null is returned if not
   *          supported.
   */
  public static IBytesDecoder findDecoder(Class<?> type) {
    // TODO daviddeng support array of supported types.
    return decoders.get(type);
  }

  /**
   * @return an IBytesEncoder for a specified type. null is returned if not
   *         supported
   */
  public static IBytesEncoder findEncoder(Class<?> type) {
    // TODO daviddeng support array of supported types.
    return encoders.get(type);
  }

  /**
   * Decodes a byte array into an object with specified type.
   */
  public static Object decode(Class<?> type, byte[] bytes) {
    return decoders.get(type).decode(bytes);
  }

  /**
   * Encodes an Object into a byte array.
   */
  public static byte[] encodeObject(Object obj) {
    if (obj == null) {
      // We don't distinguish null and zero-length byte array
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    IBytesEncoder enc = findEncoder(obj.getClass());
    if (enc == null) {
      new UnsupportedTypeException(obj.getClass());
    }
    return enc.encode(obj);
  }

  private static final ArrayList<byte[]> EMPTY_ARRAY_LIST = new ArrayList<>(0);

  /**
   * Encodes an array of Objects into an ArrayList of byte arrays.
   */
  public static ArrayList<byte[]> encodeArray(Object[] args) {
    if (args == null || args.length == 0) {
      return EMPTY_ARRAY_LIST;
    }
    ArrayList<byte[]> res = new ArrayList<>(args.length);
    for (int i = 0; i < args.length; i++) {
      res.add(encodeObject(args[i]));
    }
    return res;
  }

}
