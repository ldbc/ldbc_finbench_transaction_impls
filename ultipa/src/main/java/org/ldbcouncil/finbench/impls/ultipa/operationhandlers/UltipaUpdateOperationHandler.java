package org.ldbcouncil.finbench.impls.ultipa.operationhandlers;

import com.ultipa.sdk.connect.Connection;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.UltipaDbConnectionState;
import org.ldbcouncil.finbench.impls.ultipa.converter.UltipaConverter;

import java.util.Map;

public abstract class UltipaUpdateOperationHandler <TOperation extends Operation<LdbcNoResult>>
        implements UpdateOperationHandler<TOperation,UltipaDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 UltipaDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Connection conn = state.getConn();
        Map<String, Object> map = getParameters(state,operation);
        String query = getQueryString(state,operation);

        String[] querys = query.split(";");
        for (int i = 0; i < querys.length; i++) {
            String uql = UltipaConverter.replaceVariables(querys[i],map);
            System.out.println(uql);
            conn.uql(uql);
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    };


    public abstract Map<String, Object> getParameters(UltipaDbConnectionState state, TOperation operation );

}
