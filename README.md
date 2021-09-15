For client-server (local and remote) performance, we used [Arrow Flight Benchmark](https://github.com/Arrow-Genomics/arrow/blob/master/cpp/src/arrow/flight/flight_benchmark.cc), the Python script is placed [here](https://github.com/abs-tudelft/time-to-fly-high/blob/main/perf_test.py). 

For querying NYC Taxi dataset on remote Dremio (client-server) nodes with varying number of records(1-16 millions). Different protocols like ODBC and turbodbc and Arrow Flight implementation is available [here](https://github.com/abs-tudelft/time-to-fly-high/blob/main/dremio_query.py).
