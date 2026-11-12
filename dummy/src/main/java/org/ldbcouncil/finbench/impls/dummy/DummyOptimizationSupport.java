/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package org.ldbcouncil.finbench.impls.dummy;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.optimization.OptimizationSupport;
import org.ldbcouncil.finbench.driver.optimization.SyntheticExecutionResult;
import org.ldbcouncil.finbench.driver.optimization.SyntheticLoadSpec;

/**
 * Test implementation that models CPU, memory and duration inside the driver JVM.
 */
public class DummyOptimizationSupport implements OptimizationSupport {
    private static final long MAX_TEST_ALLOCATION_BYTES = 16L * 1024 * 1024;
    private static final long CPU_CONTROL_PERIOD_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    @Override
    public double getCurrentCpuCores() {
        return getCpuCoresFromSystemUnderTest();
    }

    @Override
    public long getCurrentMemoryBytes() {
        return getMemoryBytesFromSystemUnderTest();
    }

    @Override
    public SyntheticExecutionResult executeSyntheticLoad(SyntheticLoadSpec spec)
        throws DbException {
        return executeLoadOnSystemUnderTest(
            spec.targetCpuCores(),
            spec.targetMemoryBytes(),
            spec.targetDurationNanos());
    }

    /**
     * Calls the system under test to obtain its current CPU usage in cores.
     *
     * <p>Dummy has no external system, so this implementation samples the current JVM. A real
     * implementation can keep {@link #getCurrentCpuCores()} unchanged and replace only this method
     * body with its own monitoring API call.
     */
    private static double getCpuCoresFromSystemUnderTest() {
        double cpuCores = 0;
        java.lang.management.OperatingSystemMXBean bean =
            ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof OperatingSystemMXBean) {
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) bean;
            double processLoad = osBean.getProcessCpuLoad();
            if (processLoad >= 0) {
                cpuCores = processLoad * osBean.getAvailableProcessors();
            }
        }
        return cpuCores;
    }

    /**
     * Calls the system under test to obtain its current memory usage in bytes.
     *
     * <p>Dummy has no external system, so this implementation samples the current JVM. A real
     * implementation can keep {@link #getCurrentMemoryBytes()} unchanged and replace only this
     * method body with its own monitoring API call.
     */
    private static long getMemoryBytesFromSystemUnderTest() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Calls the system under test to consume the driver-specified resources for the requested
     * duration.
     *
     * <p>Dummy performs the work in the current JVM. A real implementation can keep
     * {@link #executeSyntheticLoad(SyntheticLoadSpec)} unchanged and replace only this method body
     * with a call to the system under test. The system under test must apply and release the
     * requested resources.
     */
    private static SyntheticExecutionResult executeLoadOnSystemUnderTest(
        double targetCpuCores, long targetMemoryBytes, long targetDurationNanos)
        throws DbException {
        if (targetDurationNanos < 0 || targetMemoryBytes < 0 || targetCpuCores < 0) {
            throw new DbException("Synthetic load values must be non-negative");
        }
        if (targetMemoryBytes > MAX_TEST_ALLOCATION_BYTES) {
            throw new DbException(
                "Dummy synthetic memory exceeds the 16 MiB safety limit: "
                    + targetMemoryBytes);
        }
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (targetCpuCores > availableProcessors) {
            throw new DbException(
                "Dummy synthetic CPU exceeds the available processor count: "
                    + targetCpuCores);
        }

        byte[] allocation = allocateDummyMemory(targetMemoryBytes);

        long start = System.nanoTime();
        long deadline = start + targetDurationNanos;
        if (targetCpuCores > 0) {
            long value = consumeDummyCpu(targetCpuCores, deadline);
            if (allocation != null) {
                allocation[0] = (byte) value;
            }
        } else {
            sleepUntil(deadline);
        }
        long durationNanos = System.nanoTime() - start;
        return new SyntheticExecutionResult(
            durationNanos, allocation == null ? 0 : allocation.length);
    }

    private static byte[] allocateDummyMemory(long targetMemoryBytes) {
        int allocationSize = (int) targetMemoryBytes;
        if (allocationSize == 0) {
            return null;
        }

        byte[] allocation = new byte[allocationSize];
        for (int offset = 0; offset < allocation.length; offset += 4096) {
            allocation[offset] = 1;
        }
        return allocation;
    }

    private static long consumeDummyCpu(double targetCpuCores, long deadline)
        throws DbException {
        int workerCount = (int) Math.ceil(targetCpuCores);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        List<Future<Long>> futures = new ArrayList<>(workerCount);
        for (int worker = 0; worker < workerCount; worker++) {
            double utilization = Math.min(1, targetCpuCores - worker);
            long seed = Double.doubleToLongBits(targetCpuCores) + worker;
            futures.add(executor.submit(() -> consumeDummyCpuWorker(utilization, deadline, seed)));
        }

        long value = 0;
        try {
            for (Future<Long> future : futures) {
                value ^= future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DbException("Synthetic CPU load interrupted", e);
        } catch (ExecutionException e) {
            throw new DbException("Synthetic CPU load failed", e.getCause());
        } finally {
            executor.shutdownNow();
        }
        return value;
    }

    private static long consumeDummyCpuWorker(
        double utilization, long deadline, long value) throws DbException {
        long busyNanos = (long) (CPU_CONTROL_PERIOD_NANOS * utilization);
        while (System.nanoTime() < deadline) {
            long periodStart = System.nanoTime();
            long busyDeadline = Math.min(deadline, periodStart + busyNanos);
            while (System.nanoTime() < busyDeadline) {
                value = value * 31 + 17;
            }
            sleepUntil(Math.min(deadline, periodStart + CPU_CONTROL_PERIOD_NANOS));
        }
        return value;
    }

    private static void sleepUntil(long deadline) throws DbException {
        while (System.nanoTime() < deadline) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(remainingNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DbException("Synthetic load interrupted", e);
            }
        }
    }
}
