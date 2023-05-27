package org.ldbcouncil.finbench.impls.ultipa.operationhandlers;

import com.ultipa.sdk.connect.Connection;
import com.ultipa.sdk.operate.response.Response;
import com.ultipa.sdk.utils.StringUtils;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.impls.common.operationhandlers.ListOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.UltipaDbConnectionState;
import org.ldbcouncil.finbench.impls.ultipa.converter.UltipaConverter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class UltipaListOperationHandler<TOperation extends Operation<List<TOperationResult>>, TOperationResult>
        implements ListOperationHandler<TOperationResult,TOperation,UltipaDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 UltipaDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        Connection conn = state.getConn();
        System.out.println(conn.sayHello("Hello"));
        int resultCount;
        Map<String, Object> map = getParameters(state,operation);
        String query = getQueryString(state,operation);
        String uql = UltipaConverter.replaceVariables(query,map);
        System.out.println(uql);
        List<TOperationResult> results;
        try {
            Response resp = conn.uql(uql);
            if(resp.getItems().size()>0) {
                resultCount = resp.get(0).asTable().getRows().size();
                results = toResult(resp);
            }else{
                resultCount = 0;
                results = new ArrayList<>();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        resultReporter.report(resultCount, results, operation);
    }

    public abstract List<TOperationResult> toResult(Response resp) throws ParseException;

    public abstract Map<String, Object> getParameters(UltipaDbConnectionState state, TOperation operation );

}
