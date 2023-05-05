package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.galaxybase.client.driver.Graph;
import com.galaxybase.client.driver.v1.StatementResult;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import org.ldbcouncil.finbench.impls.galaxybase.converter.GalaxybaseConverter;

public abstract class GalaxybaseSingletonOperationHandler<
    TOperationResult,
    TOperation extends Operation<TOperationResult>>
    implements OperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter)
        throws DbException {
        Graph graph = state.getGraph();
        TOperationResult tuple;
        int resultCount = 0;

        String queryName = getQueryName();
        String queryParams = getQueryParam(state, operation);
        StatementResult statementResult = graph.plugin("ldbc.finbench." + queryName, queryParams);
        String next = statementResult.next();

        resultCount++;
        tuple = convertSingleResult(state,  GalaxybaseConverter.getValues(next));
        resultReporter.report(resultCount, tuple, operation);
    }

    abstract String getQueryName();

    abstract String getQueryParam(GalaxybaseDbConnectionState state, TOperation operation);

    abstract TOperationResult convertSingleResult(GalaxybaseDbConnectionState state, String[] results);


}
