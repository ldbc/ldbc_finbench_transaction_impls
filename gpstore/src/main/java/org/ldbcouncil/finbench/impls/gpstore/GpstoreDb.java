package org.ldbcouncil.finbench.impls.gpstore;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.ldbcouncil.finbench.impls.common.QueryType;
import org.ldbcouncil.finbench.impls.gpstore.converter.GpstoreConverter;
import org.ldbcouncil.finbench.impls.gpstore.operationhandlers.GpstoreListOperationHandler;
import org.ldbcouncil.finbench.impls.gpstore.operationhandlers.GpstoreUpdateOperationHandler;
import org.pkumod.gpstore.Record;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class GpstoreDb extends Db {

    static Logger logger = LogManager.getLogger(GpstoreDb.class);

    GpstoreDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {
        logger.info("GPStore initialized");
        dcs = new GpstoreDbConnectionState(map, new GpstoreQueryStore(map.get("queryDir")));
        // complex reads
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        registerOperationHandler(ComplexRead2.class, ComplexRead2Handler.class);
        registerOperationHandler(ComplexRead3.class, ComplexRead3Handler.class);
        registerOperationHandler(ComplexRead4.class, ComplexRead4Handler.class);
        registerOperationHandler(ComplexRead5.class, ComplexRead5Handler.class);
        registerOperationHandler(ComplexRead6.class, ComplexRead6Handler.class);
        registerOperationHandler(ComplexRead7.class, ComplexRead7Handler.class);
        registerOperationHandler(ComplexRead8.class, ComplexRead8Handler.class);
        registerOperationHandler(ComplexRead9.class, ComplexRead9Handler.class);
        registerOperationHandler(ComplexRead10.class, ComplexRead10Handler.class);
        registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
        registerOperationHandler(ComplexRead12.class, ComplexRead12Handler.class);

        // simple reads
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);

        // writes
        registerOperationHandler(Write1.class, Write1Handler.class);
        registerOperationHandler(Write2.class, Write2Handler.class);
        registerOperationHandler(Write3.class, Write3Handler.class);
        registerOperationHandler(Write4.class, Write4Handler.class);
        registerOperationHandler(Write5.class, Write5Handler.class);
        registerOperationHandler(Write6.class, Write6Handler.class);
        registerOperationHandler(Write7.class, Write7Handler.class);
        registerOperationHandler(Write8.class, Write8Handler.class);
        registerOperationHandler(Write9.class, Write9Handler.class);
        registerOperationHandler(Write10.class, Write10Handler.class);
        registerOperationHandler(Write11.class, Write11Handler.class);
        registerOperationHandler(Write12.class, Write12Handler.class);
        registerOperationHandler(Write13.class, Write13Handler.class);
        registerOperationHandler(Write14.class, Write14Handler.class);
        registerOperationHandler(Write15.class, Write15Handler.class);
        registerOperationHandler(Write16.class, Write16Handler.class);
        registerOperationHandler(Write17.class, Write17Handler.class);
        registerOperationHandler(Write18.class, Write18Handler.class);
        registerOperationHandler(Write19.class, Write19Handler.class);

        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("GPStore closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class ComplexRead1Handler extends GpstoreListOperationHandler<ComplexRead1, ComplexRead1Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead1);
        }

        @Override
        protected ComplexRead1Result toResult(Record record) throws DbException {
            if (record != null) {
                long otherId = record.get(0).asLong();
                int accountDistance = record.get(1).asInt();
                long mediumId = record.get(2).asLong();
                String mediumType = record.get(3).asString();
                return new ComplexRead1Result(otherId, accountDistance, mediumId, mediumType);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead1.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead1.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead1.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead1.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead1.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead2Handler extends GpstoreListOperationHandler<ComplexRead2, ComplexRead2Result> {

        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead2);
        }

        @Override
        protected ComplexRead2Result toResult(Record record) throws DbException {
            if (record != null) {
                long otherId = record.get(0).asLong();
                double sumLoanAmount = record.get(1).asDouble();
                double sumLoanBalance = record.get(2).asDateLong();
                return new ComplexRead2Result(otherId, sumLoanAmount, sumLoanBalance);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead2.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead2.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead2.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead2.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead2.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead3Handler extends GpstoreListOperationHandler<ComplexRead3, ComplexRead3Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead3);
        }

        @Override
        protected ComplexRead3Result toResult(Record record) throws DbException {
            if (record != null) {
                long shortestPathLength = record.get(0).asLong();
                return new ComplexRead3Result(shortestPathLength);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead3.ID1, GpstoreConverter.convertLong(operation.getId1()))
                    .put(ComplexRead3.ID2, GpstoreConverter.convertLong(operation.getId2()))
                    .put(ComplexRead3.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead3.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class ComplexRead4Handler extends GpstoreListOperationHandler<ComplexRead4, ComplexRead4Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead4);
        }

        @Override
        protected ComplexRead4Result toResult(Record record) throws DbException {
            if (record != null) {
                long otherId = record.get(0).asLong();
                long numEdge2 = record.get(1).asLong();
                double sumEdge2Amount = record.get(2).asDouble();
                double maxEdge2Amount = record.get(3).asDouble();
                long numEdge3 = record.get(4).asLong();
                double sumLoanAmount = record.get(5).asDouble();
                double maxEdge3Amount = record.get(6).asDouble();
                return new ComplexRead4Result(otherId, numEdge2, sumEdge2Amount, maxEdge2Amount, numEdge3, sumLoanAmount, maxEdge3Amount);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead4.ID1, GpstoreConverter.convertLong(operation.getId1()))
                    .put(ComplexRead4.ID2, GpstoreConverter.convertLong(operation.getId2()))
                    .put(ComplexRead4.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead4.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class ComplexRead5Handler extends GpstoreListOperationHandler<ComplexRead5, ComplexRead5Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead5);
        }

        @Override
        protected ComplexRead5Result toResult(Record record) throws DbException {
            if (record != null) {
                org.ldbcouncil.finbench.driver.result.Path path = new org.ldbcouncil.finbench.driver.result.Path();
                List<String> idStr = record.get(0).asList();
                for (String id : idStr) {
                    path.addId(Long.parseLong(id));
                }
                return new ComplexRead5Result(path);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead5.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead5.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead5.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead5.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead5.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead6Handler extends GpstoreListOperationHandler<ComplexRead6, ComplexRead6Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead6);
        }

        @Override
        protected ComplexRead6Result toResult(Record record) throws DbException {
            if (record != null) {
                long midId = record.get(0).asLong();
                double sumEdge1Amount = record.get(1).asDouble();
                double maxEdge2Amount = record.get(2).asDouble();
                return new ComplexRead6Result(midId, sumEdge1Amount, maxEdge2Amount);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead6.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead6.THRESHOLD1, GpstoreConverter.convertDouble(operation.getThreshold1()))
                    .put(ComplexRead6.THRESHOLD2, GpstoreConverter.convertDouble(operation.getThreshold2()))
                    .put(ComplexRead6.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead6.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead6.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead6.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead7Handler extends GpstoreListOperationHandler<ComplexRead7, ComplexRead7Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead7);
        }

        @Override
        protected ComplexRead7Result toResult(Record record) throws DbException {
            if (record != null) {
                int numSrc = record.get(0).asInt();
                int numDst = record.get(1).asInt();
                float inOutRatio = record.get(2).asFloat();
                return new ComplexRead7Result(numSrc, numDst, inOutRatio);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead7 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead7.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead7.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(ComplexRead7.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead7.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead7.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead7.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead8Handler extends GpstoreListOperationHandler<ComplexRead8, ComplexRead8Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead8);
        }

        @Override
        protected ComplexRead8Result toResult(Record record) throws DbException {
            if (record != null) {
                long dstId = record.get(0).asLong();
                float ratio = record.get(1).asFloat();
                int minDistanceFromLoan = record.get(2).asInt();
                return new ComplexRead8Result(dstId, ratio, minDistanceFromLoan);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead8 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead8.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead8.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(ComplexRead8.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead8.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead8.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead8.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead9Handler extends GpstoreListOperationHandler<ComplexRead9, ComplexRead9Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead9);
        }

        @Override
        protected ComplexRead9Result toResult(Record record) throws DbException {
            if (record != null) {
                float ratioRepay = record.get(0).asFloat();
                float ratioDeposit = record.get(1).asFloat();
                float ratioTransfer = record.get(2).asFloat();
                return new ComplexRead9Result(ratioRepay, ratioDeposit, ratioTransfer);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead9 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead9.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead9.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(ComplexRead9.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead9.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead9.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead9.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead10Handler extends GpstoreListOperationHandler<ComplexRead10, ComplexRead10Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getComplexRead10(operation);
        }

        @Override
        protected ComplexRead10Result toResult(Record record) throws DbException {
            if (record != null) {
                float jaccardSimilarity = record.get(0).asFloat();
                return new ComplexRead10Result(jaccardSimilarity);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead10 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead10.PID1, GpstoreConverter.convertLong(operation.getPid1()))
                    .put(ComplexRead10.PID2, GpstoreConverter.convertLong(operation.getPid2()))
                    .put(ComplexRead10.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead10.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class ComplexRead11Handler extends GpstoreListOperationHandler<ComplexRead11, ComplexRead11Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead11);
        }

        @Override
        protected ComplexRead11Result toResult(Record record) throws DbException {
            if (record != null) {
                double sumLoanAmount = record.get(0).asDouble();
                int numLoans = record.get(1).asInt();
                return new ComplexRead11Result(sumLoanAmount, numLoans);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead11 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead11.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead11.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead11.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead11.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead11.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead12Handler extends GpstoreListOperationHandler<ComplexRead12, ComplexRead12Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead12);
        }

        @Override
        protected ComplexRead12Result toResult(Record record) throws DbException {
            if (record != null) {
                long compAccountId = record.get(0).asLong();
                double sumEdge2Amount = record.get(1).asDouble();
                return new ComplexRead12Result(compAccountId, sumEdge2Amount);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(ComplexRead12 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ComplexRead12.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(ComplexRead12.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ComplexRead12.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ComplexRead12.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ComplexRead12.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class SimpleRead1Handler extends GpstoreListOperationHandler<SimpleRead1, SimpleRead1Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead1);
        }

        @Override
        protected SimpleRead1Result toResult(Record record) throws DbException {
            if (record != null) {
                Date createTime = record.get(0).asDate();
                boolean isBlocked = record.get(1).asBoolean();
                String type = record.get(2).asString();
                return new SimpleRead1Result(createTime, isBlocked, type);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead1.ID, GpstoreConverter.convertLong(operation.getId()))
                    .build();
        }
    }

    public static class SimpleRead2Handler extends GpstoreListOperationHandler<SimpleRead2, SimpleRead2Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead2);
        }

        @Override
        protected SimpleRead2Result toResult(Record record) throws DbException {
            if (record != null) {
                double sumEdge1Amount = record.get(0).asDouble();
                double maxEdge1Amount = record.get(1).asDouble();
                long numEdge1 = record.get(2).asLong();
                double sumEdge2Amount = record.get(3).asDouble();
                double maxEdge2Amount = record.get(4).asDouble();
                long numEdge2 = record.get(5).asLong();
                return new SimpleRead2Result(sumEdge1Amount, maxEdge1Amount, numEdge1, sumEdge2Amount, maxEdge2Amount, numEdge2);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead2.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(SimpleRead2.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(SimpleRead2.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class SimpleRead3Handler extends GpstoreListOperationHandler<SimpleRead3, SimpleRead3Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead3);
        }

        @Override
        protected SimpleRead3Result toResult(Record record) throws DbException {
            if (record != null) {
                float blockRatio = record.get(0).asFloat();
                return new SimpleRead3Result(blockRatio);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead3.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(SimpleRead3.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(SimpleRead3.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(SimpleRead3.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class SimpleRead4Handler extends GpstoreListOperationHandler<SimpleRead4, SimpleRead4Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead4);
        }

        @Override
        protected SimpleRead4Result toResult(Record record) throws DbException {
            if (record != null) {
                long dstId = record.get(0).asLong();
                int numEdges = record.get(1).asInt();
                double sumAmount = record.get(2).asDouble();
                return new SimpleRead4Result(dstId, numEdges, sumAmount);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead4.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(SimpleRead4.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(SimpleRead4.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(SimpleRead4.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class SimpleRead5Handler extends GpstoreListOperationHandler<SimpleRead5, SimpleRead5Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead5);
        }

        @Override
        protected SimpleRead5Result toResult(Record record) throws DbException {
            if (record != null) {
                long srcId = record.get(0).asLong();
                int numEdges = record.get(1).asInt();
                double sumAmount = record.get(2).asDouble();
                return new SimpleRead5Result(srcId, numEdges, sumAmount);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead5.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(SimpleRead5.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(SimpleRead5.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(SimpleRead5.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class SimpleRead6Handler extends GpstoreListOperationHandler<SimpleRead6, SimpleRead6Result> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead6);
        }

        @Override
        protected SimpleRead6Result toResult(Record record) throws DbException {
            if (record != null) {
                long srcId = record.get(0).asLong();
                return new SimpleRead6Result(srcId);
            } else {
                return null;
            }
        }

        @Override
        protected Map<String, Object> getParameters(SimpleRead6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(SimpleRead6.ID, GpstoreConverter.convertLong(operation.getId()))
                    .put(SimpleRead6.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(SimpleRead6.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class Write1Handler extends GpstoreUpdateOperationHandler<Write1> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite1);
        }

        @Override
        public Map<String, Object> getParameters(Write1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PERSON_ID, GpstoreConverter.convertLong(operation.getPersonId()))
                    .put(operation.PERSON_NAME, GpstoreConverter.convertString(operation.getPersonName()))
                    .put(operation.IS_BLOCKED, GpstoreConverter.convertBool(operation.getIsBlocked()))
                    .build();
        }
    }

    public static class Write2Handler extends GpstoreUpdateOperationHandler<Write2> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite2);
        }

        @Override
        protected Map<String, Object> getParameters(Write2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write2.COMPANY_ID, GpstoreConverter.convertLong(operation.getCompanyId()))
                    .put(Write2.COMPANY_NAME, GpstoreConverter.convertString(operation.getCompanyName()))
                    .put(Write2.IS_BLOCKED, GpstoreConverter.convertBool(operation.getIsBlocked()))
                    .build();
        }
    }

    public static class Write3Handler extends GpstoreUpdateOperationHandler<Write3> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite3);
        }

        @Override
        protected Map<String, Object> getParameters(Write3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write3.MEDIUM_ID, GpstoreConverter.convertLong(operation.getMediumId()))
                    .put(Write3.MEDIUM_TYPE, GpstoreConverter.convertString(operation.getMediumType()))
                    .put(Write3.IS_BLOCKED, GpstoreConverter.convertBool(operation.getIsBlocked()))
                    .build();
        }
    }

    public static class Write4Handler extends GpstoreUpdateOperationHandler<Write4> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite4);
        }

        @Override
        protected Map<String, Object> getParameters(Write4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write4.PERSON_ID, GpstoreConverter.convertLong(operation.getPersonId()))
                    .put(Write4.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .put(Write4.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write4.ACCOUNT_BLOCKED, GpstoreConverter.convertBool(operation.getAccountBlocked()))
                    .put(Write4.ACCOUNT_TYPE, GpstoreConverter.convertString(operation.getAccountType()))
                    .build();
        }
    }

    public static class Write5Handler extends GpstoreUpdateOperationHandler<Write5> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite5);
        }

        @Override
        protected Map<String, Object> getParameters(Write5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write5.COMPANY_ID, GpstoreConverter.convertLong(operation.getCompanyId()))
                    .put(Write5.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .put(Write5.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write5.ACCOUNT_BLOCKED, GpstoreConverter.convertBool(operation.getAccountBlocked()))
                    .put(Write5.ACCOUNT_TYPE, GpstoreConverter.convertString(operation.getAccountType()))
                    .build();
        }
    }

    public static class Write6Handler extends GpstoreUpdateOperationHandler<Write6> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite6);
        }

        @Override
        protected Map<String, Object> getParameters(Write6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write6.PERSON_ID, GpstoreConverter.convertLong(operation.getPersonId()))
                    .put(Write6.LOAN_ID, GpstoreConverter.convertLong(operation.getLoanId()))
                    .put(Write6.LOAN_AMOUNT, GpstoreConverter.convertDouble(operation.getLoanAmount()))
                    .put(Write6.BALANCE, GpstoreConverter.convertDouble(operation.getBalance()))
                    .put(Write6.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .build();
        }
    }

    public static class Write7Handler extends GpstoreUpdateOperationHandler<Write7> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite7);
        }

        @Override
        protected Map<String, Object> getParameters(Write7 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write7.COMPANY_ID, GpstoreConverter.convertLong(operation.getCompanyId()))
                    .put(Write7.LOAN_ID, GpstoreConverter.convertLong(operation.getLoanId()))
                    .put(Write7.LOAN_AMOUNT, GpstoreConverter.convertDouble(operation.getLoanAmount()))
                    .put(Write7.BALANCE, GpstoreConverter.convertDouble(operation.getBalance()))
                    .put(Write7.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .build();
        }
    }

    public static class Write8Handler extends GpstoreUpdateOperationHandler<Write8> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite8);
        }

        @Override
        protected Map<String, Object> getParameters(Write8 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write8.PERSON_ID, GpstoreConverter.convertLong(operation.getPersonId()))
                    .put(Write8.COMPANY_ID, GpstoreConverter.convertLong(operation.getCompanyId()))
                    .put(Write8.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write8.RATIO, GpstoreConverter.convertDouble(operation.getRatio()))
                    .build();
        }
    }

    public static class Write9Handler extends GpstoreUpdateOperationHandler<Write9> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite9);
        }

        @Override
        protected Map<String, Object> getParameters(Write9 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write9.COMPANY_ID1, GpstoreConverter.convertLong(operation.getCompanyId1()))
                    .put(Write9.COMPANY_ID2, GpstoreConverter.convertLong(operation.getCompanyId2()))
                    .put(Write9.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write9.RATIO, GpstoreConverter.convertDouble(operation.getRatio()))
                    .build();
        }
    }

    public static class Write10Handler extends GpstoreUpdateOperationHandler<Write10> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite10);
        }

        @Override
        protected Map<String, Object> getParameters(Write10 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write10.PERSON_ID1, GpstoreConverter.convertLong(operation.getPersonId1()))
                    .put(Write10.PERSON_ID2, GpstoreConverter.convertLong(operation.getPersonId2()))
                    .put(Write10.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .build();
        }
    }

    public static class Write11Handler extends GpstoreUpdateOperationHandler<Write11> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite11);
        }

        @Override
        protected Map<String, Object> getParameters(Write11 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write11.COMPANY_ID1, GpstoreConverter.convertLong(operation.getCompanyId1()))
                    .put(Write11.COMPANY_ID2, GpstoreConverter.convertLong(operation.getCompanyId2()))
                    .put(Write11.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .build();
        }
    }

    public static class Write12Handler extends GpstoreUpdateOperationHandler<Write12> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite12);
        }

        @Override
        protected Map<String, Object> getParameters(Write12 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write12.ACCOUNT_ID1, GpstoreConverter.convertLong(operation.getAccountId1()))
                    .put(Write12.ACCOUNT_ID2, GpstoreConverter.convertLong(operation.getAccountId2()))
                    .put(Write12.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write12.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write13Handler extends GpstoreUpdateOperationHandler<Write13> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite13);
        }

        @Override
        protected Map<String, Object> getParameters(Write13 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write13.ACCOUNT_ID1, GpstoreConverter.convertLong(operation.getAccountId1()))
                    .put(Write13.ACCOUNT_ID2, GpstoreConverter.convertLong(operation.getAccountId2()))
                    .put(Write13.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write13.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write14Handler extends GpstoreUpdateOperationHandler<Write14> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write14 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite14);
        }

        @Override
        protected Map<String, Object> getParameters(Write14 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write14.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .put(Write14.LOAN_ID, GpstoreConverter.convertLong(operation.getLoanId()))
                    .put(Write14.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write14.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write15Handler extends GpstoreUpdateOperationHandler<Write15> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write15 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite15);
        }

        @Override
        protected Map<String, Object> getParameters(Write15 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write15.LOAN_ID, GpstoreConverter.convertLong(operation.getLoanId()))
                    .put(Write15.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .put(Write15.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(Write15.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write16Handler extends GpstoreUpdateOperationHandler<Write16> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write16 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite16);
        }

        @Override
        protected Map<String, Object> getParameters(Write16 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write16.MEDIUM_ID, GpstoreConverter.convertLong(operation.getMediumId()))
                    .put(Write16.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .put(Write16.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .build();
        }
    }

    public static class Write17Handler extends GpstoreUpdateOperationHandler<Write17> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write17 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite17);
        }

        @Override
        protected Map<String, Object> getParameters(Write17 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write17.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .build();
        }
    }

    public static class Write18Handler extends GpstoreUpdateOperationHandler<Write18> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write18 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite18);
        }

        @Override
        protected Map<String, Object> getParameters(Write18 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write18.ACCOUNT_ID, GpstoreConverter.convertLong(operation.getAccountId()))
                    .build();
        }
    }

    public static class Write19Handler extends GpstoreUpdateOperationHandler<Write19> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, Write19 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite19);
        }

        @Override
        protected Map<String, Object> getParameters(Write19 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(Write19.PERSON_ID, GpstoreConverter.convertLong(operation.getPersonId()))
                    .build();
        }
    }

    public static class ReadWrite1Handler extends GpstoreUpdateOperationHandler<ReadWrite1> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite1);
        }

        @Override
        protected Map<String, Object> getParameters(ReadWrite1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ReadWrite1.SRC_ID, GpstoreConverter.convertLong(operation.getSrcId()))
                    .put(ReadWrite1.DST_ID, GpstoreConverter.convertLong(operation.getDstId()))
                    .put(ReadWrite1.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(ReadWrite1.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .put(ReadWrite1.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ReadWrite1.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .build();
        }
    }

    public static class ReadWrite2Handler extends GpstoreUpdateOperationHandler<ReadWrite2> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite2);
        }

        @Override
        protected Map<String, Object> getParameters(ReadWrite2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ReadWrite2.SRC_ID, GpstoreConverter.convertLong(operation.getSrcId()))
                    .put(ReadWrite2.DST_ID, GpstoreConverter.convertLong(operation.getDstId()))
                    .put(ReadWrite2.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(ReadWrite2.AMOUNT_THRESHOLD, GpstoreConverter.convertDouble(operation.getAmountThreshold()))
                    .put(ReadWrite2.AMOUNT, GpstoreConverter.convertDouble(operation.getAmount()))
                    .put(ReadWrite2.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ReadWrite2.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ReadWrite2.RATIO_THRESHOLD, GpstoreConverter.convertFloat(operation.getRatioThreshold()))
                    .put(ReadWrite2.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ReadWrite2.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ReadWrite3Handler extends GpstoreUpdateOperationHandler<ReadWrite3> {
        @Override
        public String getQueryString(GpstoreDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite3);
        }

        @Override
        protected Map<String, Object> getParameters(ReadWrite3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(ReadWrite3.SRC_ID, GpstoreConverter.convertLong(operation.getSrcId()))
                    .put(ReadWrite3.DST_ID, GpstoreConverter.convertLong(operation.getDstId()))
                    .put(ReadWrite2.TIME, GpstoreConverter.convertLong(operation.getTime().getTime()))
                    .put(ReadWrite3.THRESHOLD, GpstoreConverter.convertDouble(operation.getThreshold()))
                    .put(ReadWrite3.START_TIME, GpstoreConverter.convertLong(operation.getStartTime().getTime()))
                    .put(ReadWrite3.END_TIME, GpstoreConverter.convertLong(operation.getEndTime().getTime()))
                    .put(ReadWrite3.TRUNCATION_LIMIT, GpstoreConverter.convertInt(operation.getTruncationLimit()))
                    .put(ReadWrite3.TRUNCATION_ORDER, GpstoreConverter.convertOrder(operation.getTruncationOrder()))
                    .build();
        }
    }
}
