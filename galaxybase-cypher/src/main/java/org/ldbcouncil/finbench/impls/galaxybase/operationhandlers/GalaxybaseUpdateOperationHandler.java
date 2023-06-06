package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.v1.StatementResult;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;

public abstract class GalaxybaseUpdateOperationHandler<
    TOperation extends Operation<LdbcNoResult>>
    implements UpdateOperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter)
        throws DbException {
        Graph graph = state.getGraph();
        String queryString = getQueryString(state, operation);
        queryString = queryString.replace("TIMESTAMP_ASCENDING", "ASC");
        queryString = queryString.replace("TIMESTAMP_DESCENDING", "DESC");
        StatementResult statementResult = graph.executeQuery(queryString);
        while (statementResult.hasNext()) {
            statementResult.next();
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

}
