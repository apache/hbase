package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BloomFilterRvvNative {
    private static final boolean ENABLED;
    static {
        boolean ok;
        try {
            System.loadLibrary("bloomfilter_rvv");
            ok = true;
        } catch (Throwable t) {
            ok = false;
        }
        ENABLED = ok;
    }

    public static boolean isEnabled() {
        return ENABLED;
    }

    // native
    public native void nativeSetBitOld(byte[] bitmap, int offset, long pos);

    public native void nativeSetBitsBatch(byte[] bitmap, int offset, long[] positions, int length);

    public native void nativeCheckBitsBatch(byte[] bitmap, int offset, long[] positions, int length, boolean[] results);

    public native void addToBloomFinal(byte[] bloomBuf, int hash1, int hash2, int hashCount);

    public native boolean getBitFinal(long pos, byte[] bloomBuf);

    // 旧 API
    public void setBitsRvv(byte[] bitmap, int offset, long[] positions, int length) {
        nativeSetBitsBatch(bitmap, offset, positions, length);
    }

    // 便捷重载：对 positions 的一个切片 [posOff, posOff+length)
    public void setBitsRvv(byte[] bitmap, int offset, long[] positions, int posOff, int length) {
        if (posOff == 0 && length == positions.length) {
            nativeSetBitsBatch(bitmap, offset, positions, length);
        } else {
            long[] slice = new long[length];
            System.arraycopy(positions, posOff, slice, 0, length);
            nativeSetBitsBatch(bitmap, offset, slice, length);
        }
    }

    // 批量查询：对 positions 的一个切片，结果写入 results 的 resOff 开始
    public void checkBitsRvv(byte[] bitmap, int offset, long[] positions, int posOff, int length, boolean[] results,
            int resOff) {
        long[] slice = new long[length];
        System.arraycopy(positions, posOff, slice, 0, length);
        boolean[] tmp = new boolean[length];
        nativeCheckBitsBatch(bitmap, offset, slice, length, tmp);
        System.arraycopy(tmp, 0, results, resOff, length);
    }

    // 批量添加 hash 位到 BloomFilter
    public void setHashLocRvv(byte[] bloomBuf, int hash1, int hash2, int hashCount) {
        addToBloomFinal(bloomBuf, hash1, hash2, hashCount);
    }

    // 查询指定 bit 是否为 1
    public boolean isBitSet(long pos, byte[] bloomBuf) {
        return getBitFinal(pos, bloomBuf);
    }
}