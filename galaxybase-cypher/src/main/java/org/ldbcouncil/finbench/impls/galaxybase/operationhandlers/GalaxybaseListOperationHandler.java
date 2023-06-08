package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.v1.Record;
import com.graphdbapi.driver.v1.StatementResult;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import java.util.ArrayList;
import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.common.operationhandlers.ListOperationHandler;

public abstract class GalaxybaseListOperationHandler<TOperationResult, TOperation extends Operation<List<TOperationResult>>>
    implements ListOperationHandler<TOperationResult, TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Graph graph = state.getGraph();
        List<TOperationResult> results = new ArrayList<>();
        int resultCount = 0;

        String queryString = getQueryString(state, operation);
        queryString = queryString.replace("TIMESTAMP_ASCENDING", "ASC");
        queryString = queryString.replace("TIMESTAMP_DESCENDING", "DESC");

        try {
            StatementResult statementResult = graph.executeQuery(queryString);
            while (statementResult.hasNext()) {
                Record record = statementResult.next();
                resultCount++;
                TOperationResult tuple;
                tuple = convertSingleResult(record);
                results.add(tuple);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


        resultReporter.report(resultCount, results, operation);
    }
    protected abstract TOperationResult convertSingleResult(Record record);

}
