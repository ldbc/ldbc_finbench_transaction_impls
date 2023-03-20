# README

Here are some resources for example use including,
- the example configuration for driver (file)
- the example data

You can execute the driver with three modes, and you should update the mode in the properties file.
- CREATE_VALIDATION
- VALIDATE_DATABASE
- EXECUTE_BENCHMARK

execute:
```
java -cp xxx.jar org.ldbcouncil.finbench.driver.driver.Driver -P example.properties
```

debug:
- add VM options `-P src/main/resources/example/example.properties`
- run `Driver.main`