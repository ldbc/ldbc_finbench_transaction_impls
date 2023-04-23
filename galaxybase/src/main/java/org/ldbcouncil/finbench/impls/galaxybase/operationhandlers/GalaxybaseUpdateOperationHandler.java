package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.galaxybase.client.driver.Graph;
import com.galaxybase.client.driver.v1.StatementResult;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;

public abstract class GalaxybaseUpdateOperationHandler<
    TOperation extends Operation<LdbcNoResult>>
    implements OperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter)
        throws DbException {
        Graph graph = state.getGraph();
        String queryName = getQueryName();
        String queryParams = getQueryParam(state, operation);
        StatementResult statementResult = graph.plugin("ldbc.finbench." + queryName, queryParams);
        while (statementResult.hasNext()) {
            statementResult.next();
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

    protected abstract String getQueryName();

    protected abstract String getQueryParam(GalaxybaseDbConnectionState state, TOperation operation);

}
