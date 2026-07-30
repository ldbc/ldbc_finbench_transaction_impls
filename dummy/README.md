# Dummy Module @ FinBench Transaction Reference Implementation

You can execute the driver with four modes, and you should update the mode in the properties file.
- CREATE_VALIDATION
- VALIDATE_DATABASE
- EXECUTE_BENCHMARK
- OPTIMIZATION_RECOMMENDATION

execute:
```
java -cp xxx.jar org.ldbcouncil.finbench.driver.driver.Driver -P example.properties
```

debug:
- add VM options `-P src/main/resources/example/example.properties`
- run `Driver.main`
# Dummy Optimization Support

`DummyDb` implements the optional FinBench `OptimizationSupport` capability. It only samples the
current JVM CPU and memory usage and executes bounded synthetic CPU/memory/duration loads requested
by the driver. The driver captures and profiles real operations through their normal handlers.

The dummy implementation executes requested memory allocations up to 16 MiB exactly. Larger
requests fail explicitly instead of being silently truncated.

`DummyOptimizationSupport` implements the three system-under-test interfaces directly:

- `getCurrentCpuCores()` delegates to `getCpuCoresFromSystemUnderTest()`.
- `getCurrentMemoryBytes()` delegates to `getMemoryBytesFromSystemUnderTest()`.
- `executeSyntheticLoad()` receives the request from the driver and delegates it to
  `executeLoadOnSystemUnderTest()`.
- `getCpuCoresFromSystemUnderTest()` and `getMemoryBytesFromSystemUnderTest()` obtain current
  resource usage from the system under test.
- `executeLoadOnSystemUnderTest()` applies the requested CPU, memory, and duration and releases the
  resources after completion.

Dummy implements their method bodies with local JVM operations because no external system exists.
A real implementation should implement the same `OptimizationSupport` interface. It can keep the
three interface-method delegations unchanged and replace the three `...SystemUnderTest()` method
bodies with calls to its own system APIs.

Use these files as the implementation reference:

- `DummyOptimizationSupport.java` shows the three interface methods and the isolated SUT call
  locations. Its JVM monitoring, 16 MiB limit, and local busy-loop implementation are Dummy-only
  behavior and should not be copied as real SUT monitoring.
- `DummyDb.java` shows how the database adapter returns the capability from
  `optimizationSupport()`.

The units passed across the interface are CPU cores, memory bytes, and duration nanoseconds.
`executeLoadOnSystemUnderTest()` must release the requested resources before it returns.

For local simulation, Dummy maps the integer part of `targetCpuCores` to fully busy worker threads
and the fractional part to a duty-cycled worker. For example, `2.5` uses two fully busy workers and
one worker that is busy for approximately half of every 10 ms control period.

Synthetic duration values are passed in nanoseconds, so operations shorter than 1 ms keep their
original precision instead of being rounded up to 1 ms.

Run the optimization recommendation example after building the dummy module:

```bash
mvn -pl dummy -am package -DskipTests
cd dummy
sh run_optimization_recommendation.sh
```

The script uses `src/main/resources/example/optimization_recommendation.properties` by default
and writes `optimization-report.md` and `optimization-report.json` under the configured
`results_dir`.
You can tune
`optimization.top_n`, `optimization.reduction_steps`, `optimization.profile_repetitions`, and
`optimization.repetitions` in that properties file.
