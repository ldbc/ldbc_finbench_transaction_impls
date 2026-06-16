package org.ldbcouncil.finbench.impls.dummy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.ldbcouncil.finbench.driver.optimization.OperationProfileSpec;
import org.ldbcouncil.finbench.driver.optimization.ResourceSnapshot;
import org.ldbcouncil.finbench.driver.optimization.SyntheticExecutionResult;
import org.ldbcouncil.finbench.driver.optimization.SyntheticLoadSpec;

public class DummyOptimizationSupportTest {
    @Test
    public void shouldSampleResourcesAndExecuteSyntheticLoad() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        ResourceSnapshot snapshot = support.sampleResources();
        SyntheticExecutionResult result =
            support.executeSyntheticLoad(new SyntheticLoadSpec("ComplexRead1", 0, 1024, 20));

        assertNotNull(snapshot);
        assertTrue(snapshot.memoryBytes() > 0);
        assertTrue(result.actualDurationMillis() >= 15);
        assertEquals(1024, result.peakMemoryBytes());
    }

    @Test
    public void shouldExecuteRepresentativeOperationForProfiling() throws Exception {
        DummyOptimizationSupport support = new DummyOptimizationSupport();

        SyntheticExecutionResult result =
            support.executeRepresentativeOperation(new OperationProfileSpec("ComplexRead1", 10));

        assertTrue(result.actualDurationMillis() >= 5);
        assertTrue(result.peakMemoryBytes() > 0);
    }
}
