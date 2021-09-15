#### Benchmarking Arrow Flight

For client-server (local and remote) performance, we used [Arrow Flight Benchmark](https://github.com/Arrow-Genomics/arrow/blob/master/cpp/src/arrow/flight/flight_benchmark.cc), the Python script is placed [here](https://github.com/abs-tudelft/time-to-fly-high/blob/main/perf_test.py). 

[Cartesius](https://userinfo.surfsara.nl/systems/cartesius) local node example:
```
singularity exec  /scratch-shared/tahmad/bio_data/flight.simg /arrow/cpp/release/release/arrow-flight-benchmark --server_host tcn541

singularity exec  /scratch-shared/tahmad/bio_data/flight.simg /arrow/cpp/release/release/arrow-flight-perf-server --server_host tcn541
```

For querying NYC Taxi dataset on remote Dremio (client-server) nodes with varying number of records(1-16 millions). Different protocols like ODBC and turbodbc and Arrow Flight implementation is available [here](https://github.com/abs-tudelft/time-to-fly-high/blob/main/dremio_query.py).


For querying NYC Taxi dataset with varying number of records (0.1-16 millions) through remote DataFusion client-server Flight connection, we used [DataFusion Flight](https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples/examples) updated [client](https://github.com/abs-tudelft/time-to-fly-high/blob/main/flight_client.rs)-[server](https://github.com/abs-tudelft/time-to-fly-high/blob/main/flight_server.rs) implementation.

Commands for creating Arrow Flight based singularity container:
```
sudo singularity build -w flight.simg flight.def
sudo singularity shell -w flight.simg
> mkdir /arrow
> cp -r arrow/cpp/release/release /arrow
```
