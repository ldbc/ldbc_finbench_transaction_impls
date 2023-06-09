package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.v1.StatementResult;
import com.graphdbapi.driver.v1.Value;
import java.util.Map;
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

        String query = getQuery(state, operation);
        Map<String, Value> params = getParams(state, operation);
        if (query.contains("$truncationOrder")) {
            query = query.replace("$truncationOrder", params.get("truncationOrder").asString());
            query = query.replace("$truncationLimit", String.valueOf(params.get("truncationLimit").asInt()));
        }

        try {
            StatementResult statementResult = graph.executeCypher(query, params);
            while (statementResult.hasNext()) {
                statementResult.next();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

    protected abstract String getQuery(GalaxybaseDbConnectionState state, TOperation operation);

    protected abstract Map<String, Value> getParams(GalaxybaseDbConnectionState state, TOperation operation);

}
