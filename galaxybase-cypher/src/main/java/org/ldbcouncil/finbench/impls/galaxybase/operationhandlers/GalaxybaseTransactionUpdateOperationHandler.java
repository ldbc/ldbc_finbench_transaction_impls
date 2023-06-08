package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.GraphTransaction;
import com.graphdbapi.driver.Isolation;
import com.graphdbapi.driver.v1.Record;
import com.graphdbapi.driver.v1.StatementResult;
import com.graphdbapi.driver.v1.Value;
import java.util.Map;
import org.ldbcouncil.finbench.impls.galaxybase.GalaxybaseDbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;

public abstract class GalaxybaseTransactionUpdateOperationHandler<
    TOperation extends Operation<LdbcNoResult>>
    implements UpdateOperationHandler<TOperation, GalaxybaseDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GalaxybaseDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Graph graph = state.getGraph();

        String queryString = getQuery(state, operation);
        Map<String, Value> params = getParams(state, operation);
        if (queryString.contains("$truncationOrder")) {
            queryString = queryString.replace("$truncationOrder", params.get("truncationOrder").asString());
            queryString = queryString.replace("$truncationLimit", String.valueOf(params.get("truncationLimit").asInt()));
        }
        String[] txns = queryString.split("BEGIN|COMMIT", 1000);

        try {
            for (String txn : txns) {
                if (txn.trim().isEmpty()) {
                    continue;
                }
                GraphTransaction tx = graph.beginTransaction();
                String[] queries = txn.split("QUERY", 1000);
                int successNum = 0;
                try {
                    boolean isSuccess = true;
                    for (String query : queries) {
                        if (query.trim().isEmpty()) {
                            continue;
                        }
                        StatementResult statementResult = graph.executeCypher(query, params);
                        if (statementResult.hasNext()) {
                            Record record = statementResult.next();
                            if (!record.get(0).asBoolean()) {
                                isSuccess = false;
                                break;
                            }
                        } else {
                            isSuccess = false;
                            break;
                        }
                        successNum++;
                    }
                    if (isSuccess) {
                        tx.success();
                    } else {
                        tx.failure();
                    }
                } finally {
                    tx.close();
                }
                // Whether to do step 4
                // step 1 is success
                // step 2 is success
                // step 3 is failure
                if (successNum != 2) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

    protected abstract String getQuery(GalaxybaseDbConnectionState state, TOperation operation);

    protected abstract Map<String, Value> getParams(GalaxybaseDbConnectionState state, TOperation operation);
}
