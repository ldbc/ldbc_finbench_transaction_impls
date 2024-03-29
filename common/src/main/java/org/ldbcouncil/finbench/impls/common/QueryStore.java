package org.ldbcouncil.finbench.impls.common;
/**
 * QueryStore.java
 * <p>
 * This class stores functions to query definition files and to retrieve the following used in operation handlers:
 * - Query definition strings
 * - Parameter map (Map<String, Object>), with as default String objects as values
 * - Prepared queries (using string substitution)
 * <p>
 * Implementations can extend this class and override functions to change e.g.
 * - ParameterPrefix ()
 * - ParameterPostfix (file extension of query files)
 * - Parameter map with different type stored than a string.
 */

import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.finbench.driver.DbException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;


public abstract class QueryStore {

    /**
     * Stores the loaded queries.
     */
    protected Map<QueryType, String> queries = new HashMap<>();

    /**
     * Create QueryStore
     *
     * @param path:   Path to the directory containing query files
     * @param postfix The extension of the query files
     * @throws DbException
     */
    public QueryStore(String path, String postfix) throws DbException {
        for (QueryType queryType : QueryType.values()) {
            queries.put(queryType, loadQueryFromFile(path, queryType.getName() + postfix));
        }
    }

    /**
     * Parameter prefix used in query definitions. Defaults to '$'
     *
     * @return
     */
    protected String getParameterPrefix() {
        return "$";
    }

    /**
     * The file extension for query files.
     *
     * @return
     */
    protected String getParameterPostfix() {
        return "";
    }

    /**
     * Load a query file
     *
     * @param path     Path to the file
     * @param filename The name of the query file
     * @return Loaded query definition
     * @throws DbException
     */
    protected String loadQueryFromFile(String path, String filename) throws DbException {
        final String filePath = path + File.separator + filename;
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            System.err.printf("Unable to load query from file: %s", filePath);
            return null;
        }
    }

    /**
     * Prepares the parameterized query using the parameter map
     *
     * @param queryType              Type of query to prepare (QueryType)
     * @param parameterSubstitutions The parameter map containing the subsitute values
     * @return Prepared query string
     */
    protected String prepare(QueryType queryType, Map<String, Object> parameterSubstitutions) {
        String querySpecification = queries.get(queryType);
        for (String parameter : parameterSubstitutions.keySet()) {
            querySpecification = querySpecification.replace(
                    getParameterPrefix() + parameter + getParameterPostfix(),
                    (String) parameterSubstitutions.get(parameter)
            );
        }
        return querySpecification;
    }

    public String getParameterizedQuery(QueryType queryType) {
        return queries.get(queryType);
    }

    public String getComplexRead1(ComplexRead1 operation) {
        return prepare(QueryType.TransactionComplexRead1, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead1.ID, Long.toString(operation.getId()))
                .put(ComplexRead1.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead1.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead1.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead1.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead2(ComplexRead2 operation) {
        return prepare(QueryType.TransactionComplexRead2, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead2.ID, Long.toString(operation.getId()))
                .put(ComplexRead2.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead2.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead2.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead2.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead3(ComplexRead3 operation) {
        return prepare(QueryType.TransactionComplexRead3, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead3.ID1, Long.toString(operation.getId1()))
                .put(ComplexRead3.ID2, Long.toString(operation.getId2()))
                .put(ComplexRead3.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead3.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getComplexRead4(ComplexRead4 operation) {
        return prepare(QueryType.TransactionComplexRead4, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead4.ID1, Long.toString(operation.getId1()))
                .put(ComplexRead4.ID2, Long.toString(operation.getId2()))
                .put(ComplexRead4.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead4.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getComplexRead5(ComplexRead5 operation) {
        return prepare(QueryType.TransactionComplexRead5, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead5.ID, Long.toString(operation.getId()))
                .put(ComplexRead5.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead5.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead5.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead5.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead6(ComplexRead6 operation) {
        return prepare(QueryType.TransactionComplexRead6, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead6.ID, Long.toString(operation.getId()))
                .put(ComplexRead6.THRESHOLD1, Double.toString(operation.getThreshold1()))
                .put(ComplexRead6.THRESHOLD2, Double.toString(operation.getThreshold2()))
                .put(ComplexRead6.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead6.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead6.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead6.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead7(ComplexRead7 operation) {
        return prepare(QueryType.TransactionComplexRead7, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead7.ID, Long.toString(operation.getId()))
                .put(ComplexRead7.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(ComplexRead7.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead7.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead7.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead7.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead8(ComplexRead8 operation) {
        return prepare(QueryType.TransactionComplexRead8, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead8.ID, Long.toString(operation.getId()))
                .put(ComplexRead8.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(ComplexRead8.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead8.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead8.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead8.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead9(ComplexRead9 operation) {
        return prepare(QueryType.TransactionComplexRead9, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead9.ID, Long.toString(operation.getId()))
                .put(ComplexRead9.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(ComplexRead9.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead9.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead9.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead9.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead10(ComplexRead10 operation) {
        return prepare(QueryType.TransactionComplexRead10, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead10.PID1, Long.toString(operation.getPid1()))
                .put(ComplexRead10.PID2, Long.toString(operation.getPid2()))
                .put(ComplexRead10.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead10.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getComplexRead11(ComplexRead11 operation) {
        return prepare(QueryType.TransactionComplexRead11, new ImmutableMap.Builder<String, Object>()
                .put(ComplexRead11.ID, Long.toString(operation.getId()))
                .put(ComplexRead11.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ComplexRead11.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ComplexRead11.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ComplexRead11.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getComplexRead12(ComplexRead12 operation) {
        return prepare(QueryType.TransactionComplexRead12, new ImmutableMap.Builder<String, Object>()
            .put(ComplexRead12.ID, Long.toString(operation.getId()))
            .put(ComplexRead12.START_TIME, Long.toString(operation.getStartTime().getTime()))
            .put(ComplexRead12.END_TIME, Long.toString(operation.getEndTime().getTime()))
            .put(ComplexRead12.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
            .put(ComplexRead12.TRUNCATION_ORDER, operation.getTruncationOrder().name())
            .build());
    }

    public String getSimpleRead1(SimpleRead1 operation) {
        return prepare(QueryType.TransactionSimpleRead1, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead1.ID, Long.toString(operation.getId()))
                .build());
    }

    public String getSimpleRead2(SimpleRead2 operation) {
        return prepare(QueryType.TransactionSimpleRead2, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead2.ID, Long.toString(operation.getId()))
                .put(SimpleRead2.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(SimpleRead2.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getSimpleRead3(SimpleRead3 operation) {
        return prepare(QueryType.TransactionSimpleRead3, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead3.ID, Long.toString(operation.getId()))
                .put(SimpleRead3.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(SimpleRead3.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(SimpleRead3.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getSimpleRead4(SimpleRead4 operation) {
        return prepare(QueryType.TransactionSimpleRead4, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead4.ID, Long.toString(operation.getId()))
                .put(SimpleRead4.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(SimpleRead4.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(SimpleRead4.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getSimpleRead5(SimpleRead5 operation) {
        return prepare(QueryType.TransactionSimpleRead5, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead5.ID, Long.toString(operation.getId()))
                .put(SimpleRead5.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(SimpleRead5.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(SimpleRead5.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getSimpleRead6(SimpleRead6 operation) {
        return prepare(QueryType.TransactionSimpleRead6, new ImmutableMap.Builder<String, Object>()
                .put(SimpleRead6.ID, Long.toString(operation.getId()))
                .put(SimpleRead6.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(SimpleRead6.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getWrite1(Write1 operation) {
        return prepare(QueryType.TransactionWrite1, new ImmutableMap.Builder<String, Object>()
                .put(Write1.PERSON_ID, Long.toString(operation.getPersonId()))
                .put(Write1.PERSON_NAME, operation.getPersonName())
                .put(Write1.IS_BLOCKED, Boolean.toString(operation.getIsBlocked()))
                .build());
    }

    public String getWrite2(Write2 operation) {
        return prepare(QueryType.TransactionWrite2, new ImmutableMap.Builder<String, Object>()
                .put(Write2.COMPANY_ID, Long.toString(operation.getCompanyId()))
                .put(Write2.COMPANY_NAME, operation.getCompanyName())
                .put(Write2.IS_BLOCKED, Boolean.toString(operation.getIsBlocked()))
                .build());
    }

    public String getWrite3(Write3 operation) {
        return prepare(QueryType.TransactionWrite3, new ImmutableMap.Builder<String, Object>()
                .put(Write3.MEDIUM_ID, Long.toString(operation.getMediumId()))
                .put(Write3.MEDIUM_TYPE, operation.getMediumType())
                .put(Write3.IS_BLOCKED, Boolean.toString(operation.getIsBlocked()))
                .build());
    }

    public String getWrite4(Write4 operation) {
        return prepare(QueryType.TransactionWrite4, new ImmutableMap.Builder<String, Object>()
                .put(Write4.PERSON_ID, Long.toString(operation.getPersonId()))
                .put(Write4.ACCOUNT_ID, Long.toString(operation.getAccountId()))
                .put(Write4.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write4.ACCOUNT_BLOCKED, Boolean.toString(operation.getAccountBlocked()))
                .put(Write4.ACCOUNT_TYPE, operation.getAccountType())
                .build());
    }

    public String getWrite5(Write5 operation) {
        return prepare(QueryType.TransactionWrite5, new ImmutableMap.Builder<String, Object>()
                .put(Write5.COMPANY_ID, Long.toString(operation.getCompanyId()))
                .put(Write5.ACCOUNT_ID, Long.toString(operation.getAccountId()))
                .put(Write5.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write5.ACCOUNT_BLOCKED, Boolean.toString(operation.getAccountBlocked()))
                .put(Write5.ACCOUNT_TYPE, operation.getAccountType())
                .build());
    }

    public String getWrite6(Write6 operation) {
        return prepare(QueryType.TransactionWrite6, new ImmutableMap.Builder<String, Object>()
                .put(Write6.PERSON_ID, Long.toString(operation.getPersonId()))
                .put(Write6.LOAN_ID, Long.toString(operation.getLoanId()))
                .put(Write6.LOAN_AMOUNT, Double.toString(operation.getLoanAmount()))
                .put(Write6.BALANCE, Double.toString(operation.getBalance()))
                .put(Write6.TIME, Long.toString(operation.getTime().getTime()))
                .build());
    }

    public String getWrite7(Write7 operation) {
        return prepare(QueryType.TransactionWrite7, new ImmutableMap.Builder<String, Object>()
                .put(Write7.COMPANY_ID, Long.toString(operation.getCompanyId()))
                .put(Write7.LOAN_ID, Long.toString(operation.getLoanId()))
                .put(Write7.LOAN_AMOUNT, Double.toString(operation.getLoanAmount()))
                .put(Write7.BALANCE, Double.toString(operation.getBalance()))
                .put(Write7.TIME, Long.toString(operation.getTime().getTime()))
                .build());
    }

    public String getWrite8(Write8 operation) {
        return prepare(QueryType.TransactionWrite8, new ImmutableMap.Builder<String, Object>()
                .put(Write8.PERSON_ID, Long.toString(operation.getPersonId()))
                .put(Write8.COMPANY_ID, Long.toString(operation.getCompanyId()))
                .put(Write8.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write8.RATIO, Double.toString(operation.getRatio()))
                .build());
    }

    public String getWrite9(Write9 operation) {
        return prepare(QueryType.TransactionWrite9, new ImmutableMap.Builder<String, Object>()
                .put(Write9.COMPANY_ID1, Long.toString(operation.getCompanyId1()))
                .put(Write9.COMPANY_ID2, Long.toString(operation.getCompanyId2()))
                .put(Write9.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write9.RATIO, Double.toString(operation.getRatio()))
                .build());
    }

    public String getWrite10(Write10 operation) {
        return prepare(QueryType.TransactionWrite10, new ImmutableMap.Builder<String, Object>()
                .put(Write10.PERSON_ID1, Long.toString(operation.getPersonId1()))
                .put(Write10.PERSON_ID2, Long.toString(operation.getPersonId2()))
                .put(Write10.TIME, Long.toString(operation.getTime().getTime()))
                .build());
    }

    public String getWrite11(Write11 operation) {
        return prepare(QueryType.TransactionWrite11, new ImmutableMap.Builder<String, Object>()
                .put(Write11.COMPANY_ID1, Long.toString(operation.getCompanyId1()))
                .put(Write11.COMPANY_ID2, Long.toString(operation.getCompanyId2()))
                .put(Write11.TIME, Long.toString(operation.getTime().getTime()))
                .build());
    }

    public String getWrite12(Write12 operation) {
        return prepare(QueryType.TransactionWrite12, new ImmutableMap.Builder<String, Object>()
                .put(Write12.ACCOUNT_ID1, Long.toString(operation.getAccountId1()))
                .put(Write12.ACCOUNT_ID2, Long.toString(operation.getAccountId2()))
                .put(Write12.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write12.AMOUNT, Double.toString(operation.getAmount()))
                .build());
    }

    public String getWrite13(Write13 operation) {
        return prepare(QueryType.TransactionWrite13, new ImmutableMap.Builder<String, Object>()
                .put(Write13.ACCOUNT_ID1, Long.toString(operation.getAccountId1()))
                .put(Write13.ACCOUNT_ID2, Long.toString(operation.getAccountId2()))
                .put(Write13.TIME, Long.toString(operation.getTime().getTime()))
                .put(Write13.AMOUNT, Double.toString(operation.getAmount()))
                .build());
    }

    public String getWrite14(Write14 operation) {
        return prepare(QueryType.TransactionWrite14, new ImmutableMap.Builder<String, Object>()
            .put(Write14.ACCOUNT_ID, Long.toString(operation.getAccountId()))
            .put(Write14.LOAN_ID, Long.toString(operation.getLoanId()))
            .put(Write14.TIME, Long.toString(operation.getTime().getTime()))
            .put(Write14.AMOUNT, Double.toString(operation.getAmount()))
            .build());
    }

    public String getWrite15(Write15 operation) {
        return prepare(QueryType.TransactionWrite15, new ImmutableMap.Builder<String, Object>()
            .put(Write15.LOAN_ID, Long.toString(operation.getLoanId()))
            .put(Write15.ACCOUNT_ID, Long.toString(operation.getAccountId()))
            .put(Write15.TIME, Long.toString(operation.getTime().getTime()))
            .put(Write15.AMOUNT, Double.toString(operation.getAmount()))
            .build());
    }

    public String getWrite16(Write16 operation) {
        return prepare(QueryType.TransactionWrite16, new ImmutableMap.Builder<String, Object>()
            .put(Write16.MEDIUM_ID, Long.toString(operation.getMediumId()))
            .put(Write16.ACCOUNT_ID, Long.toString(operation.getAccountId()))
            .put(Write16.TIME, Long.toString(operation.getTime().getTime()))
            .build());
    }

    public String getWrite17(Write17 operation) {
        return prepare(QueryType.TransactionWrite17, new ImmutableMap.Builder<String, Object>()
            .put(Write17.ACCOUNT_ID, Long.toString(operation.getAccountId()))
            .build());
    }

    public String getWrite18(Write18 operation) {
        return prepare(QueryType.TransactionWrite18, new ImmutableMap.Builder<String, Object>()
            .put(Write18.ACCOUNT_ID, Long.toString(operation.getAccountId()))
            .build());
    }

    public String getWrite19(Write19 operation) {
        return prepare(QueryType.TransactionWrite19, new ImmutableMap.Builder<String, Object>()
            .put(Write19.PERSON_ID, Long.toString(operation.getPersonId()))
            .build());
    }

    public String getReadWrite1(ReadWrite1 operation) {
        return prepare(QueryType.TransactionReadWrite1, new ImmutableMap.Builder<String, Object>()
                .put(ReadWrite1.SRC_ID, Long.toString(operation.getSrcId()))
                .put(ReadWrite1.DST_ID, Long.toString(operation.getDstId()))
                .put(ReadWrite1.TIME, Long.toString(operation.getTime().getTime()))
                .put(ReadWrite1.AMOUNT, Double.toString(operation.getAmount()))
                .put(ReadWrite1.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ReadWrite1.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .build());
    }

    public String getReadWrite2(ReadWrite2 operation) {
        return prepare(QueryType.TransactionReadWrite2, new ImmutableMap.Builder<String, Object>()
                .put(ReadWrite2.SRC_ID, Long.toString(operation.getSrcId()))
                .put(ReadWrite2.DST_ID, Long.toString(operation.getDstId()))
                .put(ReadWrite2.TIME, Long.toString(operation.getTime().getTime()))
                .put(ReadWrite2.AMOUNT_THRESHOLD, Double.toString(operation.getAmountThreshold()))
                .put(ReadWrite2.AMOUNT, Double.toString(operation.getAmount()))
                .put(ReadWrite2.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ReadWrite2.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ReadWrite2.RATIO_THRESHOLD, Float.toString(operation.getRatioThreshold()))
                .put(ReadWrite2.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ReadWrite2.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }

    public String getReadWrite3(ReadWrite3 operation) {
        return prepare(QueryType.TransactionReadWrite3, new ImmutableMap.Builder<String, Object>()
                .put(ReadWrite3.SRC_ID, Long.toString(operation.getSrcId()))
                .put(ReadWrite3.DST_ID, Long.toString(operation.getDstId()))
                .put(ReadWrite2.TIME, Long.toString(operation.getTime().getTime()))
                .put(ReadWrite3.THRESHOLD, Double.toString(operation.getThreshold()))
                .put(ReadWrite3.START_TIME, Long.toString(operation.getStartTime().getTime()))
                .put(ReadWrite3.END_TIME, Long.toString(operation.getEndTime().getTime()))
                .put(ReadWrite3.TRUNCATION_LIMIT, Integer.toString(operation.getTruncationLimit()))
                .put(ReadWrite3.TRUNCATION_ORDER, operation.getTruncationOrder().name())
                .build());
    }
}
