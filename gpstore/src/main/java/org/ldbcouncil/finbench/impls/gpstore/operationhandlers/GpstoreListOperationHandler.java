package org.ldbcouncil.finbench.impls.gpstore.operationhandlers;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.common.operationhandlers.ListOperationHandler;
import org.ldbcouncil.finbench.impls.gpstore.GpstoreDbConnectionState;
import org.pkumod.gpstore.Record;
import org.pkumod.gpstore.Result;
import org.pkumod.gpstore.driver.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GpstoreListOperationHandler<TOperation extends Operation<List<TOperationResult>>, TOperationResult>
        implements ListOperationHandler<TOperationResult, TOperation, GpstoreDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 GpstoreDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        String querySpecification = getQueryString(state, operation);
        String cypher = formatQuery(querySpecification, operation);
        System.out.println(cypher);
        try (final Session session = state.getSession()) {
            List<TOperationResult> results = new ArrayList<>();
            final Result result = session.run(cypher);
            int ansNum = 0;
            if (result != null) {
                ansNum = result.ansNum();
                for (Record record: result.list()) {
                    TOperationResult item = toResult(record);
                    if (item != null) {
                        results.add(item);
                    }
                }
                resultReporter.report(ansNum, results, operation);
            } else {
                resultReporter.report(0, results, operation);
            }
        } catch (Exception e) {
            throw new DbException(e);
        }
    }

    protected String formatQuery(String querySpecification, TOperation operation) {
        Map<String, Object> parameterSubstitutions = getParameters(operation);
        for (String parameter : parameterSubstitutions.keySet()) {
            querySpecification = querySpecification.replace(
                    "$" + parameter,
                    (String) parameterSubstitutions.get(parameter)
            );
        }
        return querySpecification;
    }

    protected abstract TOperationResult toResult(Record record) throws DbException;

    protected abstract Map<String, Object> getParameters(TOperation operation);
}
