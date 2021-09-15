import subprocess
BASE = 1000
KILO = 1000
N=1
for parallel_factor in range(5):
    x = pow(2,parallel_factor)
    for num_of_records in range(BASE, 1000000, 199980):
        print(x, num_of_records)
        for rep in range(5):
            subprocess.call(["singularity", "exec",  "/home/tahmad/tahmad/singularity/flight.simg", "/arrow/release/arrow-flight-benchmark",
                             "--server_host", "tcn541",
                             "--records_per_stream", str(num_of_records),
                             "--num_streams", str(x),
                             "--num_threads", str(x)
                             ])

for parallel_factor in range(5):
    x = pow(2,parallel_factor)
    for num_of_records in range(BASE, 1000000, 199980):
        print(x, num_of_records)
        for rep in range(5):
            subprocess.call(["singularity", "exec",  "/home/tahmad/tahmad/singularity/flight.simg", "/arrow/release/arrow-flight-benchmark",
                             "--server_host", "tcn541",
                             "--test_put", "True",
                             "--records_per_batch", str(2048),
                             "--records_per_stream", str(num_of_records),
                             "--num_streams", str(x),
                             "--num_threads", str(x)
                             ])
