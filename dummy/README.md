# Dummy Module @ FinBench Transaction Reference Implementation

You can execute the driver with three modes, and you should update the mode in the properties file.
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

`DummyDb` implements the optional FinBench `OptimizationSupport` capability. It samples the
current JVM CPU and memory usage, runs a representative operation for resource profiling, and then
executes bounded synthetic CPU/memory/duration loads for `OPTIMIZATION_RECOMMENDATION` mode tests.

The dummy implementation caps each synthetic memory allocation at 16 MiB to keep local tests safe.

Run the optimization recommendation example after building the dummy module:

```bash
mvn -pl dummy -am package -DskipTests
cd dummy
sh run_optimization_recommendation.sh
```

The script uses `src/main/resources/example/optimization_recommendation.properties` by default
and writes `optimization-report.md` and `optimization-report.json` under the configured `results_dir`.
You can tune
`optimization.top_n`, `optimization.reduction_steps`, `optimization.profile_repetitions`, and
`optimization.repetitions` in that properties file.
