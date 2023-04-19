package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.galaxybase.client.driver.Graph;
import com.galaxybase.client.driver.v1.StatementResult;
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
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Graph graph = state.getGraph();
        List<TOperationResult> results = new ArrayList<>();
        int resultCount = 0;

        String queryName = getQueryName();
        String queryParams = getQueryParam(state, operation);

        StatementResult statementResult = graph.plugin("ldbc.finbench." + queryName, queryParams);
        while (statementResult.hasNext()) {
            String next = statementResult.next();
            resultCount++;
            TOperationResult tuple;
            tuple = convertSingleResult(state, GalaxybaseConverter.getValues(next));
            results.add(tuple);
        }

        resultReporter.report(resultCount, results, operation);
    }

    protected abstract String getQueryName();

    protected abstract String getQueryParam(GalaxybaseDbConnectionState state, TOperation operation);

    protected abstract TOperationResult convertSingleResult(GalaxybaseDbConnectionState state, String[] results);

}
