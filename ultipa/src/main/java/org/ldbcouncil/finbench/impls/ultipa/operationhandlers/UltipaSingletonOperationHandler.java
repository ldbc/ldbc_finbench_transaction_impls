package org.ldbcouncil.finbench.impls.ultipa.operationhandlers;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.UltipaDbConnectionState;

import java.util.Map;

public abstract class UltipaSingletonOperationHandler <TOperation extends Operation<LdbcNoResult>>
        implements UpdateOperationHandler<TOperation, UltipaDbConnectionState> {

    @Override
    public abstract void executeOperation(TOperation operation,
                                 UltipaDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException;

    public abstract Map<String, Object> getParameters(UltipaDbConnectionState state, TOperation operation );
}
