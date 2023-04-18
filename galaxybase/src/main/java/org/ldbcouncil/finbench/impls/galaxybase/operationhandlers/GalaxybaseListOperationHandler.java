package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import java.util.ArrayList;
import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import org.ldbcouncil.finbench.impls.galaxybase.converter.GalaxybaseConverter;

public abstract class GalaxybaseListOperationHandler<
    TOperationResult,
    TOperation extends Operation<List<TOperationResult>>>
    implements OperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation, GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        // TODO add galaxybase dependency
    }

    protected abstract String getQueryName();

    protected abstract String getQueryParam(GalaxybaseDbConnectionState state, TOperation operation);

    protected abstract TOperationResult convertSingleResult(GalaxybaseDbConnectionState state, String[] results);

}
