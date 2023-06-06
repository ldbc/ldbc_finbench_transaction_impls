package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;

import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.v1.Record;
import com.graphdbapi.driver.v1.StatementResult;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.common.operationhandlers.SingletonOperationHandler;

public abstract class GalaxybaseSingletonOperationHandler<
    TOperationResult,
    TOperation extends Operation<TOperationResult>>
    implements SingletonOperationHandler<TOperationResult, TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter)
        throws DbException {
        Graph graph = state.getGraph();
        TOperationResult tuple;
        int resultCount = 0;

        String queryString = getQueryString(state, operation);
        queryString = queryString.replace("TIMESTAMP_ASCENDING", "ASC");
        queryString = queryString.replace("TIMESTAMP_DESCENDING", "DESC");
        StatementResult statementResult = graph.executeQuery(queryString);
        Record record = statementResult.next();

        resultCount++;
        tuple = convertSingleResult(record);
        resultReporter.report(resultCount, tuple, operation);
    }

    protected abstract TOperationResult convertSingleResult(Record record);



}
