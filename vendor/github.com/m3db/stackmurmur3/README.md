stackmurmur3
=======

## Note: This is a fork of [github.com/spaolacci/murmur3](http://github.com/spaolacci/murmur3) that provides digests that are allocated on the stack and can be incrementally written to. This is useful for places where you perform concurrent hashing and there's no good place to cache a hash without needing to acquire it expensively (under lock, etc).

[![Build Status](https://travis-ci.org/m3db/stackmurmur3.svg?branch=master)](https://travis-ci.org/m3db/stackmurmur3)

Native Go implementation of Austin Appleby's third MurmurHash revision (aka MurmurHash3).

Reference algorithm has been slightly hacked as to support the streaming mode required by Go's standard [Hash interface](http://golang.org/pkg/hash/#Hash).

Benchmarks
----------

This shows that its really only useful to use this stack version when allocating per operation is too expensive (i.e. GC already the limiting factor).

<pre>

Benchmark_Incremental_Origin_128_1-4       20000000      80.4 ns/op      12.43 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_2-4       20000000      89.5 ns/op      22.35 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_4-4       20000000     106 ns/op        37.72 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_8-4       10000000     108 ns/op        73.65 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_16-4      20000000     110 ns/op       144.97 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_32-4      20000000     102 ns/op      1253.59 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_64-4      20000000    88.3 ns/op       725.15 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_128-4     20000000     104 ns/op      1230.40 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_256-4     10000000     141 ns/op      1806.16 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_512-4     10000000     177 ns/op      2882.86 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_1024-4     5000000     317 ns/op      3226.23 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_2048-4     3000000     495 ns/op      4133.69 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_4096-4     2000000     876 ns/op      4673.40 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Origin_128_8192-4     1000000    1719 ns/op      4763.41 MB/s    16 B/op    1 allocs/op
Benchmark_Incremental_Forked_128_1-4       10000000     135 ns/op         7.36 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_2-4       10000000     160 ns/op        12.43 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_4-4       10000000     158 ns/op        25.19 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_8-4       10000000     152 ns/op        52.45 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_16-4      20000000     109 ns/op       145.64 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_32-4      10000000     131 ns/op       970.13 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_64-4      10000000     123 ns/op       517.18 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_128-4     10000000     151 ns/op       844.44 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_256-4     10000000     155 ns/op      1646.55 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_512-4     10000000     205 ns/op      2491.70 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_1024-4     5000000     305 ns/op      3346.95 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_2048-4     3000000     531 ns/op      3853.73 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_4096-4     2000000     908 ns/op      4506.57 MB/s     0 B/op    0 allocs/op
Benchmark_Incremental_Forked_128_8192-4     1000000    1711 ns/op      4785.81 MB/s     0 B/op    0 allocs/op

</pre>
