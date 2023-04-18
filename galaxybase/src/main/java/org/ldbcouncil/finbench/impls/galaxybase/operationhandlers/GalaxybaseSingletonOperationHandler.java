package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;

public abstract class GalaxybaseSingletonOperationHandler<
    TOperationResult,
    TOperation extends Operation<TOperationResult>>
    implements OperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation, GalaxybaseDbConnectionState state, ResultReporter resultReporter)
        throws DbException {
        // TODO add galaxybase dependency
    }

    abstract String getQueryName();

    abstract String getQueryParam(GalaxybaseDbConnectionState state, TOperation operation);

    abstract TOperationResult convertSingleResult(GalaxybaseDbConnectionState state, String[] results);


}
