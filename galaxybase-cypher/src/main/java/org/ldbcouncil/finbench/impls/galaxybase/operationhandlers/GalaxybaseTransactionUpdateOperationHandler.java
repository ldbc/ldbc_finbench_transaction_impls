package org.ldbcouncil.finbench.impls.galaxybase.operationhandlers;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.GraphTransaction;
import com.graphdbapi.driver.Isolation;
import com.graphdbapi.driver.v1.Record;
import com.graphdbapi.driver.v1.StatementResult;
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

        String queryString = getQueryString(state, operation);
        queryString = queryString.replace("TIMESTAMP_ASCENDING", "ASC");
        queryString = queryString.replace("TIMESTAMP_DESCENDING", "DESC");
        System.out.println(operation.toString());
        System.out.println(queryString);
        String[] txns = queryString.split("BEGIN|COMMIT", 1000);
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
                    System.out.println(query + "\n");
                    StatementResult statementResult = tx.executeQuery(query);
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
            System.out.println(successNum);
            if (successNum != 2) {
                break;
            }
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
}
