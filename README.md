BuntDB-Benchmark
================
A utility for measuring performance of [BuntDB](https://github.com/tidwall/buntdb).

Install
-------
[Go](http://golang.com) must be installed before installing.

```sh
go install github.com/tidwall/buntdb-benchmark
```

Running
-------

The following options are supported:

```
Usage of buntdb-benchmark:
  -N int
        Number of times to re-run the tests. -1 = forever (default 1)
  -P int
        Number requests per transaction (default 1)
  -csv
        Output in CSV format
  -mem
        Use only memory, no disk persistence
  -n int
        Number of operations per test (default 100000)
  -q    Quiet. Just show query/sec values
  -r int
        Number of parallel goroutines (default 10)
  -s int
        Number of items in the random set (default 1000)
  -t string
        Only run the comma separated list of tests
```

To run every test execute this: 

```
buntdb-benchmark
```

This will run all tests and output details that look like:

```
====== GET ======
  100000 operations completed in 0.02 seconds
  1000 item random data set
  10 parallel goroutines
  heap usage: 25264 bytes

4141982.69 operations per second

====== SET ======
  100000 operations completed in 0.40 seconds
  1000 item random data set
  10 parallel goroutines
  heap usage: 101864 bytes

249158.03 operations per second
```

The `-q` option will only output the `operations per second` lines.

```
$ buntdb-benchmark -q
GET: 4609604.74 operations per second
SET: 248500.33 operations per second
ASCEND_100: 2268998.79 operations per second
ASCEND_200: 1178388.14 operations per second
ASCEND_400: 679134.20 operations per second
ASCEND_800: 348445.55 operations per second
DESCEND_100: 2313821.69 operations per second
DESCEND_200: 1292738.38 operations per second
DESCEND_400: 675258.76 operations per second
DESCEND_800: 337481.67 operations per second
SPATIAL_SET: 134824.60 operations per second
SPATIAL_INTERSECTS_100: 939491.47 operations per second
SPATIAL_INTERSECTS_200: 561590.40 operations per second
SPATIAL_INTERSECTS_400: 306951.15 operations per second
SPATIAL_INTERSECTS_800: 159673.91 operations per second
```

Contact
-------
Josh Baker [@tidwall](http://twitter.com/tidwall)

Licence
-------
BuntDB-Benchmark source code is available under the MIT [License](/LICENSE).

