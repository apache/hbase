package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * RISC-V RVV 向量化扫描优化工具类
 */
@InterfaceAudience.Private
public class ScanRVV {

    private static boolean rvvEnabled = false;

    static {
        try {
            System.loadLibrary("scan_rvv_jni");
            rvvEnabled = true;
        } catch (UnsatisfiedLinkError e) {
            rvvEnabled = false;
        } catch (Throwable t) {
            rvvEnabled = false;
        }
    }

    public static native boolean isEnabled();

    public static void setEnabled(boolean enabled) {
        rvvEnabled = enabled;
    }

    public static boolean available() {
        if (!rvvEnabled) {
            return false;
        }
        try {
            return isEnabled();
        } catch (Throwable t) {
            return false;
        }
    }

    public static native int compareCells(byte[] aKey, int aLen, byte[] bKey, int bLen);

    public static native int compareKeyForNextRow(byte[] indexedKey, int idxLen, byte[] curKey, int curLen);

    public static native int compareKeyForNextColumn(byte[] indexedKey, int idxLen, byte[] curKey, int curLen);

    public static native int memcmp(byte[] a, int offsetA, int lengthA,
            byte[] b, int offsetB, int lengthB);

    public static native boolean prefixMatch(byte[] a, int offsetA,
            byte[] b, int offsetB,
            int prefixLen);

    public static int memcmp(byte[] a, byte[] b, int length) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Input arrays cannot be null");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative");
        }
        return memcmp(a, 0, length, b, 0, length);
    }

    public static boolean prefixMatch(byte[] a, byte[] b, int prefixLen) {
        if (a == null || b == null) {
            return false;
        }
        if (prefixLen <= 0 || prefixLen > a.length || prefixLen > b.length) {
            return false;
        }
        return prefixMatch(a, 0, b, 0, prefixLen);
    }

    public static native int rvvCommonPrefix(byte[] a, int offsetA, int lengthA,
            byte[] b, int offsetB, int lengthB);

    public static native void rvvMemcpy(byte[] dst, int dstOffset,
            byte[] src, int srcOffset,
            int length);

    public static void rvvMemcpy(byte[] dst, int dstOffset,
            ByteBuffer src, int srcOffset,
            int length) {
        if (dst == null || src == null) {
            throw new IllegalArgumentException("Input parameters cannot be null");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative");
        }

        if (src.hasArray()) {
            System.arraycopy(src.array(), src.arrayOffset() + srcOffset, dst, dstOffset, length);
        } else {
            byte[] tmp = new byte[length];
            src.position(srcOffset);
            src.get(tmp);
            rvvMemcpy(dst, dstOffset, tmp, 0, length);
        }
    }

    public static byte[] copyToArray(byte[] src, int offset, int length) {
        if (src == null) {
            throw new IllegalArgumentException("Source array cannot be null");
        }
        if (offset < 0 || length < 0 || offset + length > src.length) {
            throw new IllegalArgumentException("Invalid offset or length");
        }

        byte[] dst = new byte[length];
        ScanRVV.rvvMemcpy(dst, 0, src, offset, length);
        return dst;
    }

    public static byte[] copyToArray(ByteBuffer src) {
        if (src == null) {
            throw new IllegalArgumentException("Source ByteBuffer cannot be null");
        }

        int len = src.remaining();
        byte[] dst = new byte[len];
        if (src.hasArray()) {
            System.arraycopy(src.array(), src.arrayOffset() + src.position(), dst, 0, len);
        } else {
            rvvMemcpy(dst, 0, src, src.position(), len);
        }
        return dst;
    }
}
