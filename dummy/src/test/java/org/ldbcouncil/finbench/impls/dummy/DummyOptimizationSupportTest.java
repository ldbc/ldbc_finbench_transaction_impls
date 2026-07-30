/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package org.ldbcouncil.finbench.impls.dummy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.optimization.SyntheticExecutionResult;
import org.ldbcouncil.finbench.driver.optimization.SyntheticLoadSpec;

public class DummyOptimizationSupportTest {
    @Test
    public void shouldSampleResourcesAndExecuteSyntheticLoad() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        double cpuCores = support.getCurrentCpuCores();
        long memoryBytes = support.getCurrentMemoryBytes();
        SyntheticExecutionResult result =
            support.executeSyntheticLoad(
                new SyntheticLoadSpec(
                    "ComplexRead1", 0, 1024, TimeUnit.MILLISECONDS.toNanos(20)));

        assertTrue(cpuCores >= 0);
        assertTrue(memoryBytes > 0);
        assertTrue(result.actualDurationNanos() >= TimeUnit.MILLISECONDS.toNanos(15));
        assertEquals(1024, result.peakMemoryBytes());
    }

    @Test(expected = DbException.class)
    public void shouldRejectSyntheticMemoryAboveSafetyLimit() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        support.executeSyntheticLoad(
            new SyntheticLoadSpec(
                "ComplexRead1", 0, 16L * 1024 * 1024 + 1, TimeUnit.MILLISECONDS.toNanos(1)));
    }

    @Test
    public void shouldExecuteFractionalCpuLoad() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        SyntheticExecutionResult result =
            support.executeSyntheticLoad(
                new SyntheticLoadSpec(
                    "ComplexRead1", 0.25, 0, TimeUnit.MILLISECONDS.toNanos(30)));

        assertTrue(result.actualDurationNanos() >= TimeUnit.MILLISECONDS.toNanos(25));
    }

    @Test
    public void shouldExecuteSubMillisecondLoad() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        SyntheticExecutionResult result =
            support.executeSyntheticLoad(
                new SyntheticLoadSpec(
                    "ComplexRead1", 0, 0, TimeUnit.MICROSECONDS.toNanos(500)));

        assertTrue(result.actualDurationNanos() >= TimeUnit.MICROSECONDS.toNanos(400));
    }

}
