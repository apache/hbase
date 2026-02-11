# Generating XXH3 Test Vectors

This snippet shows how to generate test vectors for the XXH3 hash function using the upstream implementation.

## Clone xxHash

```shell
git clone https://github.com/Cyan4973/xxHash.git
cd xxHash
```

## Create the generator file

```shell
cat > gen_xxh3_vectors.c << 'EOF'
#include "xxhash.c"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <maxLen> [seed...]\n", argv[0]);
        return 1;
    }

    int maxLen = atoi(argv[1]);
    int sc = argc - 2;
    if (sc <= 0) sc = 1;

    unsigned char* buf = malloc((size_t)maxLen + 1);
    if (!buf) return 2;

    for (int len = 0; len <= maxLen; len++) {
        for (int i = 0; i < len; i++) buf[i] = (unsigned char)i;

        printf("%d", len);

        for (int i = 0; i < sc; i++) {
            const char* seedStr = (argc >= 3) ? argv[2 + i] : "0";
            long long s64 = strtoll(seedStr, NULL, 10);
            uint64_t seed = (uint64_t)s64;
            uint64_t h = XXH3_64bits_withSeed(buf, (size_t)len, seed);
            printf(",%lld,%lld", s64, (long long)(int64_t)h);
        }
        putchar('\n');
    }

    free(buf);
    return 0;
}
EOF

cc -O2 -std=c99 gen_xxh3_vectors.c -o gen_xxh3_vectors
```

## Generate CSV

```shell
./gen_xxh3_vectors 2049 0 31 > xxh3_vectors.csv
```

- Lengths: 0 to 2049 (inclusive)
- Seeds: 0, 31
- Output format: `length,seed,hash,seed,hash,...`
