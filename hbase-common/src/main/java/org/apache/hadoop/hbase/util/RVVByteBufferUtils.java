package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RVVByteBufferUtils {

    static {
        try {
            System.loadLibrary("scan_rvv_jni");
        } catch (Throwable t) {
            // ignore; use availability checks
        }
    }

    public static boolean available() {
        return ScanRVV.available();
    }

    public static native int compareToRvv(ByteBuffer a, int aOffset, int aLen,
            ByteBuffer b, int bOffset, int bLen);

    public static native int commonPrefixRvv(byte[] left, int leftOffset,
            byte[] right, int rightOffset,
            int maxLen);

    public static native int findCommonPrefixRvv(ByteBuffer a, int aOffset, int aLen,
            ByteBuffer b, int bOffset, int bLen);

    public static native int findCommonPrefixRvv(ByteBuffer a, int aOffset, int aLen,
            byte[] b, int bOffset, int bLen);

    public static byte[] readBytesRvv(ByteBuffer buf) {
        int len = buf.remaining();
        byte[] dst = new byte[len];
        if (!available()) {
            if (buf.hasArray()) {
                System.arraycopy(buf.array(), buf.arrayOffset() + buf.position(), dst, 0, len);
            } else {
                int pos = buf.position();
                for (int i = 0; i < len; i++) {
                    dst[i] = buf.get(pos + i);
                }
            }
            return dst;
        }
        if (buf.hasArray()) {
            System.arraycopy(buf.array(), buf.arrayOffset() + buf.position(), dst, 0, len);
        } else {
            ScanRVV.rvvMemcpy(dst, 0, buf, buf.position(), len);
        }
        return dst;
    }
}
