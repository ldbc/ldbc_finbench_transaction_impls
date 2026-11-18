/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.ldbcouncil.finbench.impls.dummy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.finbench.driver.control.DriverConfiguration;
import org.ldbcouncil.finbench.driver.control.LocalControlService;
import org.ldbcouncil.finbench.driver.driver.AutomaticTestMode;
import org.ldbcouncil.finbench.driver.driver.DriverException;
import org.ldbcouncil.finbench.driver.driver.ExecuteWorkloadMode;
import org.ldbcouncil.finbench.driver.driver.OptimizationRecommendationMode;
import org.ldbcouncil.finbench.driver.log.Log4jLoggingServiceFactory;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.temporal.SystemTimeSource;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write1;

public class DummyDatabaseResetTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private static final Set<Long> persons = new HashSet<>();
    private static final List<Integer> writesPerReset = new ArrayList<>();
    private static int resets;
    private static int writes;
    private static int failAtReset;
    private static int closes;
    private static long resetCompletedAt;
    private static TrackingDummyDb activeDb;

    @Before
    public void clearState() {
        persons.clear();
        writesPerReset.clear();
        resets = writes = failAtReset = closes = 0;
        resetCompletedAt = 0;
        activeDb = null;
    }

    @Test
    public void shouldResetBaselineProfilesAndEveryScenarioWithWarmup() throws Exception {
        runOptimization(2);
        assertEquals(5, resets); // baseline + 2 profiles + 2 full-workload repetitions
        assertEquals(java.util.Arrays.asList(0, 6, 7, 8, 8), writesPerReset);
        assertEquals(8, writes);
    }

    @Test
    public void shouldResetRoundsWithoutWarmup() throws Exception {
        runOptimization(0);
        assertEquals(5, resets);
        assertEquals(java.util.Arrays.asList(0, 4, 5, 6, 6), writesPerReset);
        assertEquals(6, writes);
    }

    @Test
    public void shouldNotResetOrdinaryBenchmark() throws Exception {
        LocalControlService control = control(2);
        ExecuteWorkloadMode mode = new ExecuteWorkloadMode(control, new SystemTimeSource(), 42);
        mode.init();
        mode.startExecutionAndAwaitCompletion();
        assertEquals(0, resets);
        assertEquals(6, writes);
    }

    @Test
    public void shouldResetEachAutomaticRoundButNotBetweenPhases() throws Exception {
        LocalControlService control = control(2);
        AutomaticTestMode mode = new AutomaticTestMode(control, new SystemTimeSource(), 42);
        mode.init();
        try {
            mode.executionAndAwaitCompletion(true); // rapid estimate: warmup only
            mode.executionAndAwaitCompletion(false); // accurate adjustment: warmup + run
            mode.executionAndAwaitCompletion(false);
            assertEquals(3, resets);
            assertEquals(java.util.Arrays.asList(0, 2, 8), writesPerReset);
            assertEquals(14, writes);
            assertTrue(control.workloadStartTimeAsMilli() > resetCompletedAt);
        } finally {
            if (activeDb != null) {
                activeDb.close();
            }
        }
    }

    @Test
    public void shouldAbortBeforeAnyWritesWhenResetFails() throws Exception {
        failAtReset = 1;
        try {
            runOptimization(2);
            fail("Expected recovery failure");
        } catch (DriverException expected) {
            assertTrue(expected.getCause().getMessage().contains("injected reset failure"));
        }
        assertEquals(0, writes);
        assertEquals(1, resets);
        assertEquals(1, closes);
    }

    @Test
    public void shouldAbortProfilingWhenResetFails() throws Exception {
        failAtReset = 2;
        try {
            runOptimization(0);
            fail("Expected recovery failure");
        } catch (DriverException expected) {
            assertTrue(expected.getCause().getMessage().contains("injected reset failure"));
        }
        assertEquals(4, writes); // only baseline completed
        assertEquals(2, resets);
        assertEquals(2, closes);
    }

    @Test
    public void shouldAllowAdaptersWithoutResetImplementation() throws Exception {
        Db legacyAdapter = new Db() {
            @Override
            protected void onInit(Map<String, String> params, LoggingService logging) { }
            @Override
            protected void onClose() { }
            @Override
            protected DbConnectionState getConnectionState() { return null; }
        };
        legacyAdapter.resetDatabaseState();
    }

    private void runOptimization(int warmup) throws Exception {
        LocalControlService control = control(warmup);
        OptimizationRecommendationMode mode = new OptimizationRecommendationMode(control, 42);
        mode.init();
        mode.startExecutionAndAwaitCompletion();
        assertTrue(control.workloadStartTimeAsMilli() > 0);
        assertTrue(new File(control.configuration().resultDirPath(), "optimization-report.json").isFile());
    }

    private LocalControlService control(int warmup) throws Exception {
        DriverConfiguration template = ConsoleAndFileDriverConfiguration.fromArgs(new String[] {
            "-P", "src/main/resources/example/optimization_recommendation.properties"
        });
        Map<String, String> overrides = new HashMap<>();
        for (String key : template.asMap().keySet()) {
            if (key.endsWith("_enable")) {
                overrides.put(key, "false");
            }
        }
        File updates = temporary.newFolder();
        StringBuilder data = new StringBuilder(
            "createTime|dependencyTime|personId|personName|isBlocked|gender|birthday|country|city\n");
        for (int i = 1; i <= 8; i++) {
            data.append(1000000 + i).append("|0|").append(i)
                .append("|Person|false|male|2000-01-01|country|city\n");
        }
        // Workload opens every update file even when only Write1 is enabled.
        File sampleUpdates = new File("src/main/resources/example/data/incremental");
        for (File file : sampleUpdates.listFiles()) {
            Files.copy(file.toPath(), new File(updates, file.getName()).toPath());
        }
        Files.write(new File(updates, "AddPersonWrite1.csv").toPath(),
            data.toString().getBytes(StandardCharsets.UTF_8));
        overrides.put("ldbc.finbench.transaction.queries.updates_dir", updates.getAbsolutePath());
        overrides.put("ldbc.finbench.transaction.queries.Write1_enable", "true");
        overrides.put("db", TrackingDummyDb.class.getName());
        overrides.put("results_dir", temporary.newFolder().getAbsolutePath());
        overrides.put("operation_count", "4");
        overrides.put("warmup", Integer.toString(warmup));
        overrides.put("optimization.top_n", "1");
        overrides.put("optimization.reduction_steps", "0.1");
        overrides.put("optimization.profile_repetitions", "2");
        overrides.put("optimization.repetitions", "2");
        overrides.put("optimization.sample_interval_millis", "1");
        overrides.put("estimate", "-1");
        overrides.put("accurate", "-1");
        DriverConfiguration config = template.applyArgs(overrides);
        return new LocalControlService(0, config, new Log4jLoggingServiceFactory(false), new SystemTimeSource());
    }

    public static class TrackingDummyDb extends DummyDb {
        @Override
        protected void onInit(Map<String, String> params, LoggingService logging) throws DbException {
            activeDb = this;
            registerOperationHandler(Write1.class, TrackingWrite.class);
        }

        @Override
        public void resetDatabaseState() throws DbException {
            super.resetDatabaseState();
            resets++;
            writesPerReset.add(writes);
            if (resets == failAtReset) {
                throw new DbException("injected reset failure");
            }
            persons.clear();
            resetCompletedAt = System.currentTimeMillis();
        }

        @Override
        protected void onClose() throws IOException {
            closes++;
            super.onClose();
        }
    }

    public static class TrackingWrite implements OperationHandler<Write1, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write1 operation, DummyDbConnectionState state, ResultReporter reporter)
            throws DbException {
            // Repeated baseline/profile writes fail unless the reset hook has cleared prior data.
            if (!persons.add(operation.getPersonId())) {
                throw new DbException("Duplicate person: " + operation.getPersonId());
            }
            // If reset were accidentally called between warmup and measurement, this would fail.
            if (operation.getPersonId() == 4 && !persons.contains(2L)) {
                throw new DbException("Warmup data was lost: " + persons);
            }
            writes++;
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}
