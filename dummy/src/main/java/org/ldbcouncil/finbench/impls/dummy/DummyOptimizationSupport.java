package org.ldbcouncil.finbench.impls.dummy;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.optimization.OptimizationSupport;
import org.ldbcouncil.finbench.driver.optimization.OperationProfileSpec;
import org.ldbcouncil.finbench.driver.optimization.ResourceSnapshot;
import org.ldbcouncil.finbench.driver.optimization.SyntheticExecutionResult;
import org.ldbcouncil.finbench.driver.optimization.SyntheticLoadSpec;

/**
 * Test implementation that models CPU, memory and duration inside the driver JVM.
 */
public class DummyOptimizationSupport implements OptimizationSupport {
    private static final long MAX_TEST_ALLOCATION_BYTES = 16L * 1024 * 1024;

    @Override
    public ResourceSnapshot sampleResources() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double cpuCores = 0;
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof OperatingSystemMXBean) {
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) bean;
            double processLoad = osBean.getProcessCpuLoad();
            if (processLoad >= 0) {
                cpuCores = processLoad * osBean.getAvailableProcessors();
            }
        }
        return new ResourceSnapshot(System.currentTimeMillis(), cpuCores, usedMemory);
    }

    @Override
    public SyntheticExecutionResult executeRepresentativeOperation(OperationProfileSpec spec) throws DbException {
        long durationMillis = Math.max(1, spec.expectedDurationMillis());
        long memoryBytes = 1024L * Math.max(1, spec.operationName().length());
        return executeSyntheticLoad(new SyntheticLoadSpec(spec.operationName(), 0.25, memoryBytes, durationMillis));
    }

    @Override
    public SyntheticExecutionResult executeSyntheticLoad(SyntheticLoadSpec spec) throws DbException {
        if (spec.targetDurationMillis() < 0 || spec.targetMemoryBytes() < 0 || spec.targetCpuCores() < 0) {
            throw new DbException("Synthetic load values must be non-negative");
        }

        int allocationSize = (int) Math.min(spec.targetMemoryBytes(), MAX_TEST_ALLOCATION_BYTES);
        byte[] allocation = allocationSize == 0 ? null : new byte[allocationSize];
        if (allocation != null) {
            for (int offset = 0; offset < allocation.length; offset += 4096) {
                allocation[offset] = 1;
            }
        }

        long start = System.nanoTime();
        long deadline = start + TimeUnit.MILLISECONDS.toNanos(spec.targetDurationMillis());
        if (spec.targetCpuCores() > 0) {
            long value = 1;
            while (System.nanoTime() < deadline) {
                value = value * 31 + 17;
            }
            if (allocation != null) {
                allocation[0] = (byte) value;
            }
        } else {
            sleepUntil(deadline);
        }
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        return new SyntheticExecutionResult(durationMillis, allocationSize);
    }

    private static void sleepUntil(long deadline) throws DbException {
        while (System.nanoTime() < deadline) {
            long remainingNanos = deadline - System.nanoTime();
            try {
                TimeUnit.NANOSECONDS.sleep(remainingNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DbException("Synthetic load interrupted", e);
            }
        }
    }
}
