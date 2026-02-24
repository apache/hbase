package org.apache.hadoop.hbase.io.compress.lz4;

import java.nio.ByteBuffer;

final class Lz4Native {
  static {
    NativeLoader.load();
  }

  static boolean available() {
    return NativeLoader.isLoaded();
  }

  static boolean isAvailable() {
     return available();
  }

  static native int maxCompressedLength(int srcLen);
  static native int compressDirect(ByteBuffer src, int srcOff, int srcLen,
                                   ByteBuffer dst, int dstOff, int dstCap);
  static native int decompressDirect(ByteBuffer src, int srcOff, int srcLen,
                                     ByteBuffer dst, int dstOff, int dstCap);


  private Lz4Native() {}
}

