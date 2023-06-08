package org.ldbcouncil.finbench.impls.galaxybase;

import com.google.common.collect.ImmutableMap;
import com.graphdbapi.driver.v1.Value;
import com.graphdbapi.driver.v1.Values;
import java.util.Map;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write13;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write14;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write15;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write16;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write17;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write18;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write19;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write9;
import org.ldbcouncil.finbench.impls.common.QueryStore;
import org.ldbcouncil.finbench.impls.common.QueryType;

public class GalaxybaseQueryStore extends QueryStore {

    public GalaxybaseQueryStore(String path) throws DbException {
        super(path, ".cypher");
    }

    protected String getQuery(QueryType queryType) {
        return queries.get(queryType);
    }

    public Map<String, Value> getParamsComplexRead1(ComplexRead1 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead1.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead1.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead1.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead1.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead1.TRUNCATION_ORDER, Values.value(Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC").equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead2(ComplexRead2 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead2.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead2.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead2.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead2.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead2.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead3(ComplexRead3 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead3.ID1, Values.value(String.valueOf(operation.getId1())))
            .put(ComplexRead3.ID2, Values.value(String.valueOf(operation.getId2())))
            .put(ComplexRead3.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead3.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsComplexRead4(ComplexRead4 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead4.ID1, Values.value(String.valueOf(operation.getId1())))
            .put(ComplexRead4.ID2, Values.value(String.valueOf(operation.getId2())))
            .put(ComplexRead4.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead4.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsComplexRead5(ComplexRead5 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead5.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead5.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead5.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead5.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead5.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead6(ComplexRead6 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead6.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead6.THRESHOLD1, Values.value(operation.getThreshold1()))
            .put(ComplexRead6.THRESHOLD2, Values.value(operation.getThreshold2()))
            .put(ComplexRead6.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead6.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead6.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead6.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead7(ComplexRead7 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead7.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead7.THRESHOLD, Values.value(operation.getThreshold()))
            .put(ComplexRead7.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead7.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead7.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead7.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead8(ComplexRead8 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead8.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead8.THRESHOLD, Values.value(operation.getThreshold()))
            .put(ComplexRead8.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead8.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead8.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead8.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead9(ComplexRead9 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead9.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead9.THRESHOLD, Values.value(operation.getThreshold()))
            .put(ComplexRead9.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead9.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead9.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead9.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead10(ComplexRead10 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead10.PID1, Values.value(String.valueOf(operation.getPid1())))
            .put(ComplexRead10.PID2, Values.value(String.valueOf(operation.getPid2())))
            .put(ComplexRead10.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead10.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsComplexRead11(ComplexRead11 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead11.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead11.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead11.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead11.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead11.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsComplexRead12(ComplexRead12 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ComplexRead12.ID, Values.value(String.valueOf(operation.getId())))
            .put(ComplexRead12.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ComplexRead12.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ComplexRead12.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ComplexRead12.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead1(SimpleRead1 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead1.ID, Values.value(String.valueOf(operation.getId())))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead2(SimpleRead2 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead2.ID, Values.value(String.valueOf(operation.getId())))
            .put(SimpleRead2.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(SimpleRead2.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead3(SimpleRead3 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead3.ID, Values.value(String.valueOf(operation.getId())))
            .put(SimpleRead3.THRESHOLD, Values.value(operation.getThreshold()))
            .put(SimpleRead3.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(SimpleRead3.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead4(SimpleRead4 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead4.ID, Values.value(String.valueOf(operation.getId())))
            .put(SimpleRead4.THRESHOLD, Values.value(operation.getThreshold()))
            .put(SimpleRead4.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(SimpleRead4.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead5(SimpleRead5 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead5.ID, Values.value(String.valueOf(operation.getId())))
            .put(SimpleRead5.THRESHOLD, Values.value(operation.getThreshold()))
            .put(SimpleRead5.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(SimpleRead5.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamsSimpleRead6(SimpleRead6 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(SimpleRead6.ID, Values.value(String.valueOf(operation.getId())))
            .put(SimpleRead6.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(SimpleRead6.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite1(Write1 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write1.PERSON_ID, Values.value(String.valueOf(operation.getPersonId())))
            .put(Write1.PERSON_NAME, Values.value(operation.getPersonName()))
            .put(Write1.IS_BLOCKED, Values.value(operation.getIsBlocked()))
            .build();
    }

    public Map<String, Value> getParamWrite2(Write2 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write2.COMPANY_ID, Values.value(String.valueOf(operation.getCompanyId())))
            .put(Write2.COMPANY_NAME, Values.value(operation.getCompanyName()))
            .put(Write2.IS_BLOCKED, Values.value(operation.getIsBlocked()))
            .build();
    }

    public Map<String, Value> getParamWrite3(Write3 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write3.MEDIUM_ID, Values.value(String.valueOf(operation.getMediumId())))
            .put(Write3.MEDIUM_TYPE, Values.value(operation.getMediumType()))
            .put(Write3.IS_BLOCKED, Values.value(operation.getIsBlocked()))
            .build();
    }

    public Map<String, Value> getParamWrite4(Write4 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write4.PERSON_ID, Values.value(String.valueOf(operation.getPersonId())))
            .put(Write4.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .put(Write4.TIME, Values.value(operation.getTime().getTime()))
            .put(Write4.ACCOUNT_BLOCKED, Values.value(operation.getAccountBlocked()))
            .put(Write4.ACCOUNT_TYPE, Values.value(operation.getAccountType()))
            .build();
    }

    public Map<String, Value> getParamWrite5(Write5 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write5.COMPANY_ID, Values.value(String.valueOf(operation.getCompanyId())))
            .put(Write5.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .put(Write5.TIME, Values.value(operation.getTime().getTime()))
            .put(Write5.ACCOUNT_BLOCKED, Values.value(operation.getAccountBlocked()))
            .put(Write5.ACCOUNT_TYPE, Values.value(operation.getAccountType()))
            .build();
    }

    public Map<String, Value> getParamWrite6(Write6 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write6.PERSON_ID, Values.value(String.valueOf(operation.getPersonId())))
            .put(Write6.LOAN_ID, Values.value(String.valueOf(operation.getLoanId())))
            .put(Write6.LOAN_AMOUNT, Values.value(operation.getLoanAmount()))
            .put(Write6.BALANCE, Values.value(operation.getBalance()))
            .put(Write6.TIME, Values.value(operation.getTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite7(Write7 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write7.COMPANY_ID, Values.value(String.valueOf(operation.getCompanyId())))
            .put(Write7.LOAN_ID, Values.value(String.valueOf(operation.getLoanId())))
            .put(Write7.LOAN_AMOUNT, Values.value(operation.getLoanAmount()))
            .put(Write7.BALANCE, Values.value(operation.getBalance()))
            .put(Write7.TIME, Values.value(operation.getTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite8(Write8 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write8.PERSON_ID, Values.value(String.valueOf(operation.getPersonId())))
            .put(Write8.COMPANY_ID, Values.value(String.valueOf(operation.getCompanyId())))
            .put(Write8.TIME, Values.value(operation.getTime().getTime()))
            .put(Write8.RATIO, Values.value(operation.getRatio()))
            .build();
    }

    public Map<String, Value> getParamWrite9(Write9 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write9.COMPANY_ID1, Values.value(String.valueOf(operation.getCompanyId1())))
            .put(Write9.COMPANY_ID2, Values.value(String.valueOf(operation.getCompanyId2())))
            .put(Write9.TIME, Values.value(operation.getTime().getTime()))
            .put(Write9.RATIO, Values.value(operation.getRatio()))
            .build();
    }

    public Map<String, Value> getParamWrite10(Write10 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write10.PERSON_ID1, Values.value(String.valueOf(operation.getPersonId1())))
            .put(Write10.PERSON_ID2, Values.value(String.valueOf(operation.getPersonId2())))
            .put(Write10.TIME, Values.value(operation.getTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite11(Write11 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write11.COMPANY_ID1, Values.value(String.valueOf(operation.getCompanyId1())))
            .put(Write11.COMPANY_ID2, Values.value(String.valueOf(operation.getCompanyId2())))
            .put(Write11.TIME, Values.value(operation.getTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite12(Write12 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write12.ACCOUNT_ID1, Values.value(String.valueOf(operation.getAccountId1())))
            .put(Write12.ACCOUNT_ID2, Values.value(String.valueOf(operation.getAccountId2())))
            .put(Write12.TIME, Values.value(operation.getTime().getTime()))
            .put(Write12.AMOUNT, Values.value(operation.getAmount()))
            .build();
    }

    public Map<String, Value> getParamWrite13(Write13 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write13.ACCOUNT_ID1, Values.value(String.valueOf(operation.getAccountId1())))
            .put(Write13.ACCOUNT_ID2, Values.value(String.valueOf(operation.getAccountId2())))
            .put(Write13.TIME, Values.value(operation.getTime().getTime()))
            .put(Write13.AMOUNT, Values.value(operation.getAmount()))
            .build();
    }

    public Map<String, Value> getParamWrite14(Write14 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write14.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .put(Write14.LOAN_ID, Values.value(String.valueOf(operation.getLoanId())))
            .put(Write14.TIME, Values.value(operation.getTime().getTime()))
            .put(Write14.AMOUNT, Values.value(operation.getAmount()))
            .build();
    }

    public Map<String, Value> getParamWrite15(Write15 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write15.LOAN_ID, Values.value(String.valueOf(operation.getLoanId())))
            .put(Write15.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .put(Write15.TIME, Values.value(operation.getTime().getTime()))
            .put(Write15.AMOUNT, Values.value(operation.getAmount()))
            .build();
    }

    public Map<String, Value> getParamWrite16(Write16 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write16.MEDIUM_ID, Values.value(String.valueOf(operation.getMediumId())))
            .put(Write16.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .put(Write16.TIME, Values.value(operation.getTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamWrite17(Write17 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write17.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .build();
    }

    public Map<String, Value> getParamWrite18(Write18 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write18.ACCOUNT_ID, Values.value(String.valueOf(operation.getAccountId())))
            .build();
    }

    public Map<String, Value> getParamWrite19(Write19 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(Write19.PERSON_ID, Values.value(String.valueOf(operation.getPersonId())))
            .build();
    }

    public Map<String, Value> getParamReadWrite1(ReadWrite1 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ReadWrite1.SRC_ID, Values.value(String.valueOf(operation.getSrcId())))
            .put(ReadWrite1.DST_ID, Values.value(String.valueOf(operation.getDstId())))
            .put(ReadWrite1.TIME, Values.value(operation.getTime().getTime()))
            .put(ReadWrite1.AMOUNT, Values.value(operation.getAmount()))
            .put(ReadWrite1.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ReadWrite1.END_TIME, Values.value(operation.getEndTime().getTime()))
            .build();
    }

    public Map<String, Value> getParamReadWrite2(ReadWrite2 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ReadWrite2.SRC_ID, Values.value(String.valueOf(operation.getSrcId())))
            .put(ReadWrite2.DST_ID, Values.value(String.valueOf(operation.getDstId())))
            .put(ReadWrite2.TIME, Values.value(operation.getTime().getTime()))
            .put(ReadWrite2.AMOUNT_THRESHOLD, Values.value(operation.getAmountThreshold()))
            .put(ReadWrite2.AMOUNT, Values.value(operation.getAmount()))
            .put(ReadWrite2.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ReadWrite2.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ReadWrite2.RATIO_THRESHOLD, Values.value(operation.getRatioThreshold()))
            .put(ReadWrite2.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ReadWrite2.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }

    public Map<String, Value> getParamReadWrite3(ReadWrite3 operation) {
        return new ImmutableMap.Builder<String, Value>()
            .put(ReadWrite3.SRC_ID, Values.value(String.valueOf(operation.getSrcId())))
            .put(ReadWrite3.DST_ID, Values.value(String.valueOf(operation.getDstId())))
            .put(ReadWrite2.TIME, Values.value(operation.getTime().getTime()))
            .put(ReadWrite3.THRESHOLD, Values.value(operation.getThreshold()))
            .put(ReadWrite3.START_TIME, Values.value(operation.getStartTime().getTime()))
            .put(ReadWrite3.END_TIME, Values.value(operation.getEndTime().getTime()))
            .put(ReadWrite3.TRUNCATION_LIMIT, Values.value(operation.getTruncationLimit()))
            .put(ReadWrite3.TRUNCATION_ORDER, Values.value(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
            .build();
    }
}
