package org.ldbcouncil.finbench.impls.ultipa;

import com.google.common.collect.ImmutableMap;
import com.ultipa.sdk.connect.Connection;
import com.ultipa.sdk.connect.transaction.Transaction;
import com.ultipa.sdk.operate.entity.*;
import com.ultipa.sdk.operate.response.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.ldbcouncil.finbench.impls.common.QueryType;
import org.ldbcouncil.finbench.impls.ultipa.converter.UltipaConverter;
import org.ldbcouncil.finbench.impls.ultipa.operationhandlers.UltipaListOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.operationhandlers.UltipaSingletonOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.operationhandlers.UltipaSpeListOperationHandler;
import org.ldbcouncil.finbench.impls.ultipa.operationhandlers.UltipaUpdateOperationHandler;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UltipaDb extends Db {
    static Logger logger = LogManager.getLogger("Ultipa");

    UltipaDbConnectionState dcs;
    UltipaQueryStore queryStore;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("UltipaDb initialized");

        queryStore = new UltipaQueryStore(properties.get("queryDir"));
        dcs = new UltipaDbConnectionState(properties,queryStore);

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

        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("UltipaTransactionDb closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState(){
        return dcs;
    }

    public static class ComplexRead1Handler extends UltipaListOperationHandler<ComplexRead1, ComplexRead1Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead1);
        }

        @Override
        public List<ComplexRead1Result> toResult(Response resp) throws ParseException {
            List<ComplexRead1Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead1Result result = new ComplexRead1Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Integer.parseInt(dataList.get(1).toString()),
                        Long.parseLong(dataList.get(2).toString().split("\\|")[1].toString()),
                        dataList.get(3).toString());
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead2Handler extends UltipaListOperationHandler<ComplexRead2, ComplexRead2Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead2);
        }

        @Override
        public List<ComplexRead2Result> toResult(Response resp) throws ParseException {
            List<ComplexRead2Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead2Result result = new ComplexRead2Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Double.parseDouble(dataList.get(1).toString()),
                        Double.parseDouble(dataList.get(2).toString()));
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Person|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead3Handler extends UltipaListOperationHandler<ComplexRead3, ComplexRead3Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead3);
        }

        @Override
        public List<ComplexRead3Result> toResult(Response resp) throws ParseException {
            List<ComplexRead3Result> resultList = new ArrayList<>();

            if (resp.getItems().size() > 0) {
                Table table = resp.get(0).asTable();
                for(List<Object> dataList : table.getRows()){
                    ComplexRead3Result result = new ComplexRead3Result(
                            Long.parseLong(dataList.get(0).toString()));
                    resultList.add(result);
                }
            }else{
                ComplexRead3Result result = new ComplexRead3Result(-1);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID1, UltipaConverter.convertIdStr(operation.getId1(),"Account|"))
                    .put(operation.ID2, UltipaConverter.convertIdStr(operation.getId2(),"Account|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead4Handler extends UltipaListOperationHandler<ComplexRead4, ComplexRead4Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead4);
        }

        @Override
        public List<ComplexRead4Result> toResult(Response resp) throws ParseException {
            List<ComplexRead4Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead4Result result = new ComplexRead4Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Long.parseLong(dataList.get(1).toString()),
                        Double.parseDouble(dataList.get(2).toString()),
                        Double.parseDouble(dataList.get(3).toString()),
                        Long.parseLong(dataList.get(4).toString()),
                        Double.parseDouble(dataList.get(5).toString()),
                        Double.parseDouble(dataList.get(6).toString())
                        );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID1, UltipaConverter.convertIdStr(operation.getId1(),"Account|"))
                    .put(operation.ID2, UltipaConverter.convertIdStr(operation.getId2(),"Account|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead5Handler extends UltipaSpeListOperationHandler<ComplexRead5, ComplexRead5Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead5);
        }

        @Override
        public List<ComplexRead5Result> toResult(Response resp,Connection conn) throws ParseException {
            List<ComplexRead5Result> resultList = new ArrayList<>();

            List<Object> list = resp.getItems().get("rest").asAttr().getValues();
            for(Object o : list){
                org.ldbcouncil.finbench.driver.result.Path pathRest = new org.ldbcouncil.finbench.driver.result.Path();
                List<Node> nodes = (List<Node>) o;
                for(Node node:nodes){
                    Response resp2 = conn.uql("find().nodes("+node.getUUID()+") as node return node{*}");
                    String nodeId = resp2.get(0).asNodes().get(0).getID();
                    pathRest.addId(Long.valueOf(nodeId.split("\\|")[1].toString()));
                }
                ComplexRead5Result result = new ComplexRead5Result(pathRest);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Person|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead6Handler extends UltipaListOperationHandler<ComplexRead6, ComplexRead6Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead6);
        }

        @Override
        public List<ComplexRead6Result> toResult(Response resp) throws ParseException {
            List<ComplexRead6Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead6Result result = new ComplexRead6Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Double.parseDouble(dataList.get(1).toString()),
                        Double.parseDouble(dataList.get(2).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD1, UltipaConverter.convertDouble(operation.getThreshold1()))
                    .put(operation.THRESHOLD2, UltipaConverter.convertDouble(operation.getThreshold2()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead7Handler extends UltipaListOperationHandler<ComplexRead7, ComplexRead7Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead7);
        }

        @Override
        public List<ComplexRead7Result> toResult(Response resp) throws ParseException {
            List<ComplexRead7Result> resultList = new ArrayList<>();
            if(resp.getItems().size() > 0){
                Table table = resp.get(0).asTable();
                for(List<Object> dataList : table.getRows()){
                    ComplexRead7Result result = new ComplexRead7Result(
                            Integer.parseInt(dataList.get(0).toString()),
                            Integer.parseInt(dataList.get(1).toString()),
                            Float.parseFloat(dataList.get(2).toString())
                    );
                    resultList.add(result);
                }
            }else{
                ComplexRead7Result result = new ComplexRead7Result(0,0,-1);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead7 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead8Handler extends UltipaListOperationHandler<ComplexRead8, ComplexRead8Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead8);
        }

        @Override
        public List<ComplexRead8Result> toResult(Response resp) throws ParseException {
            List<ComplexRead8Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead8Result result = new ComplexRead8Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Float.parseFloat(dataList.get(1).toString()),
                        Integer.parseInt(dataList.get(2).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead8 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Loan|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertFloat(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead9Handler extends UltipaListOperationHandler<ComplexRead9, ComplexRead9Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead9);
        }

        @Override
        public List<ComplexRead9Result> toResult(Response resp) throws ParseException {
            List<ComplexRead9Result> resultList = new ArrayList<>();
            if(resp.getItems().size() > 0){
                Table table = resp.get(0).asTable();
                for(List<Object> dataList : table.getRows()){
                    ComplexRead9Result result = new ComplexRead9Result(
                            Float.parseFloat(dataList.get(0).toString()),
                            Float.parseFloat(dataList.get(1).toString()),
                            Float.parseFloat(dataList.get(2).toString())
                    );
                    resultList.add(result);
                }
            }else{
                ComplexRead9Result result = new ComplexRead9Result(-1,-1,-1);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead9 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead10Handler extends UltipaListOperationHandler<ComplexRead10, ComplexRead10Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead10);
        }

        @Override
        public List<ComplexRead10Result> toResult(Response resp) throws ParseException {
            List<ComplexRead10Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead10Result result = new ComplexRead10Result(
                        Float.parseFloat(dataList.get(0).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead10 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PID1, UltipaConverter.convertIdStr(operation.getPid1(),"Person|"))
                    .put(operation.PID2, UltipaConverter.convertIdStr(operation.getPid2(),"Person|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class ComplexRead11Handler extends UltipaListOperationHandler<ComplexRead11, ComplexRead11Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead11);
        }

        @Override
        public List<ComplexRead11Result> toResult(Response resp) throws ParseException {
            List<ComplexRead11Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead11Result result = new ComplexRead11Result(
                        Double.parseDouble(dataList.get(0).toString()),
                        Integer.parseInt(dataList.get(1).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead11 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Person|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT, UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead12Handler extends UltipaListOperationHandler<ComplexRead12, ComplexRead12Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead12);
        }

        @Override
        public List<ComplexRead12Result> toResult(Response resp) throws ParseException {
            List<ComplexRead12Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                ComplexRead12Result result = new ComplexRead12Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Double.parseDouble(dataList.get(1).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead12 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Person|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class SimpleRead1Handler extends UltipaListOperationHandler<SimpleRead1, SimpleRead1Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead1);
        }

        @Override
        public List<SimpleRead1Result> toResult(Response resp) throws ParseException {
            List<SimpleRead1Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                SimpleRead1Result result = new SimpleRead1Result(
                        new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy", Locale.US).parse(dataList.get(0).toString()),
                        Boolean.parseBoolean(dataList.get(1).toString()),
                        dataList.get(1).toString()
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .build();
        }
    }

    public static class SimpleRead2Handler extends UltipaListOperationHandler<SimpleRead2, SimpleRead2Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead2);
        }

        @Override
        public List<SimpleRead2Result> toResult(Response resp) throws ParseException {
            List<SimpleRead2Result> resultList = new ArrayList<>();
            if(resp.getItems().size()>0){
                Table table = resp.get(0).asTable();
                for(List<Object> dataList : table.getRows()){
                    SimpleRead2Result result = new SimpleRead2Result(
                            Double.parseDouble(dataList.get(0).toString()),
                            Double.parseDouble(dataList.get(1).toString()),
                            Long.parseLong(dataList.get(2).toString()),
                            Double.parseDouble(dataList.get(3).toString()),
                            Double.parseDouble(dataList.get(4).toString()),
                            Long.parseLong(dataList.get(5).toString())
                    );
                    resultList.add(result);
                }
            }else{
                SimpleRead2Result result = new SimpleRead2Result(0,-1,0,0,-1,0);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class SimpleRead3Handler extends UltipaListOperationHandler<SimpleRead3, SimpleRead3Result> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead3);
        }

        @Override
        public List<SimpleRead3Result> toResult(Response resp) throws ParseException {
            List<SimpleRead3Result> resultList = new ArrayList<>();
            if(resp.getItems().size()>0){
                Table table = resp.get(0).asTable();
                for(List<Object> dataList : table.getRows()){
                    SimpleRead3Result result = new SimpleRead3Result(
                            Float.parseFloat(dataList.get(0).toString())
                    );
                    resultList.add(result);
                }
            }else{
                SimpleRead3Result result = new SimpleRead3Result(-1);
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class SimpleRead4Handler extends UltipaListOperationHandler<SimpleRead4, SimpleRead4Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead4);
        }

        @Override
        public List<SimpleRead4Result> toResult(Response resp) throws ParseException {
            List<SimpleRead4Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                SimpleRead4Result result = new SimpleRead4Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Integer.parseInt(dataList.get(1).toString()),
                        Double.parseDouble(dataList.get(2).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class SimpleRead5Handler extends UltipaListOperationHandler<SimpleRead5, SimpleRead5Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead5);
        }

        @Override
        public List<SimpleRead5Result> toResult(Response resp) throws ParseException {
            List<SimpleRead5Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                SimpleRead5Result result = new SimpleRead5Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString()),
                        Integer.parseInt(dataList.get(1).toString()),
                        Double.parseDouble(dataList.get(2).toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class SimpleRead6Handler extends UltipaListOperationHandler<SimpleRead6, SimpleRead6Result> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionSimpleRead6);
        }

        @Override
        public List<SimpleRead6Result> toResult(Response resp) throws ParseException {
            List<SimpleRead6Result> resultList = new ArrayList<>();
            Table table = resp.get(0).asTable();
            for(List<Object> dataList : table.getRows()){
                SimpleRead6Result result = new SimpleRead6Result(
                        Long.parseLong(dataList.get(0).toString().split("\\|")[1].toString())
                );
                resultList.add(result);
            }
            return resultList;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, SimpleRead6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .build();
        }
    }

    public static class Write1Handler extends UltipaUpdateOperationHandler<Write1> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite1);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .put(operation.PERSON_NAME, UltipaConverter.convertString(operation.getPersonName()))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.ACCOUNT_BLOCKED, UltipaConverter.convertBool(operation.getAccountBlocked()))
                    .put(operation.ACCOUNT_TYPE, UltipaConverter.convertString(operation.getAccountType()))
                    .build();
        }
    }

    public static class Write2Handler extends UltipaUpdateOperationHandler<Write2> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite2);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.COMPANY_ID, UltipaConverter.convertIdStr(operation.getCompanyId(),"Company|"))
                    .put(operation.COMPANY_NAME, UltipaConverter.convertString(operation.getCompanyName()))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.ACCOUNT_BLOCKED, UltipaConverter.convertBool(operation.getAccountBlocked()))
                    .put(operation.ACCOUNT_TYPE, UltipaConverter.convertString(operation.getAccountType()))
                    .build();
        }
    }

    public static class Write3Handler extends UltipaUpdateOperationHandler<Write3> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite3);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.SRC_ID, UltipaConverter.convertIdStr(operation.getSrcId(),"Account|"))
                    .put(operation.DST_ID, UltipaConverter.convertIdStr(operation.getDstId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write4Handler extends UltipaUpdateOperationHandler<Write4> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite4);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write4 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.SRC_ID, UltipaConverter.convertIdStr(operation.getSrcId(),"Account|"))
                    .put(operation.DST_ID, UltipaConverter.convertIdStr(operation.getDstId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write5Handler extends UltipaUpdateOperationHandler<Write5> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite5);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write5 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write6Handler extends UltipaUpdateOperationHandler<Write6> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite6);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write6 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.COMPANY_ID, UltipaConverter.convertIdStr(operation.getCompanyId(),"Company|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write7Handler extends UltipaUpdateOperationHandler<Write7> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite7);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write7 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.MEDIUM_ID, UltipaConverter.convertIdStr(operation.getMediumId(),"Medium|"))
                    .put(operation.MEDIUM_BLOCKED, UltipaConverter.convertBool(operation.getMediumBlocked()))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .build();
        }
    }

    public static class Write8Handler extends UltipaUpdateOperationHandler<Write8> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite8);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write8 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write9Handler extends UltipaUpdateOperationHandler<Write9> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite9);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write9 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write10Handler extends UltipaUpdateOperationHandler<Write10> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite10);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write10 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .build();
        }
    }

    public static class Write11Handler extends UltipaUpdateOperationHandler<Write11> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite11);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write11 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .build();
        }
    }

    public static class Write12Handler extends UltipaUpdateOperationHandler<Write12> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite12);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write12 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PID1, UltipaConverter.convertIdStr(operation.getPid1(),"Person|"))
                    .put(operation.PID2, UltipaConverter.convertIdStr(operation.getPid2(),"Person|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .build();
        }
    }

    public static class Write13Handler extends UltipaUpdateOperationHandler<Write13> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite13);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write13 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .build();
        }
    }

    public static class ReadWrite1Handler extends UltipaSingletonOperationHandler<ReadWrite1> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite1);
        }

        @Override
        public void executeOperation(ReadWrite1 operation, UltipaDbConnectionState state, ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }

            Transaction tx = conn.beginTransaction();
            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = tx.run(uql0);
            if(resp0.getItems().size()>0){
                String value = resp0.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql0 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }

            String uql1 = uqlList.get(1);
            System.out.println(uql1);
            Response resp1 = tx.run(uql1);
            if(resp1.getItems().size()>0){
                String value = resp1.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql1 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }

            if (!isBlocked) {
                String uql2 = uqlList.get(2);
                System.out.println(uql2);
                tx.run(uql2);

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = tx.run(uql3);
                if(resp3.getItems().size()>0){
                    tx.rollback();
                    String uql4 = uqlList.get(4);
                    System.out.println(uql4);
                    tx.run(uql4);
                }
            }else{
                tx.rollback();
            }
            tx.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ReadWrite1 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.SRC_ID, UltipaConverter.convertIdStr(operation.getSrcId(),"Account|"))
                    .put(operation.DST_ID, UltipaConverter.convertIdStr(operation.getDstId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ReadWrite2Handler extends UltipaSingletonOperationHandler<ReadWrite2> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite2);
        }

        @Override
        public void executeOperation(ReadWrite2 operation, UltipaDbConnectionState state, ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }

            Transaction tx = conn.beginTransaction();
            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = tx.run(uql0);
            if(resp0.getItems().size()>0){
                String value = resp0.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql0 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }

            String uql1 = uqlList.get(1);
            System.out.println(uql1);
            Response resp1 = tx.run(uql1);
            if(resp1.getItems().size()>0){
                String value = resp1.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql1 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }
            if (!isBlocked) {
                String uql2 = uqlList.get(2);
                System.out.println(uql2);
                tx.run(uql2);

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = tx.run(uql3);
                System.out.println(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                float ratio = Float.parseFloat(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                float ratioThreshold = Float.parseFloat(map.get("ratioThreshold").toString());
                System.out.println("ratioThreshold:"+ratioThreshold);
                if (ratio < ratioThreshold){
                    tx.rollback();
                    String uql4 = uqlList.get(4);
                    System.out.println(uql4);
                    tx.run(uql4);
                }
            }else{
                tx.rollback();
            }
            tx.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ReadWrite2 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.SRC_ID, UltipaConverter.convertIdStr(operation.getSrcId(),"Account|"))
                    .put(operation.DST_ID, UltipaConverter.convertIdStr(operation.getDstId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT_THRESHOLD, UltipaConverter.convertDouble(operation.getAmountThreshold()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.RATIO_THRESHOLD, UltipaConverter.convertFloat(operation.getRatioThreshold()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ReadWrite3Handler extends UltipaSingletonOperationHandler<ReadWrite3> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionReadWrite3);
        }

        @Override
        public void executeOperation(ReadWrite3 operation, UltipaDbConnectionState state, ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }

            Transaction tx = conn.beginTransaction();
            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = tx.run(uql0);
            if(resp0.getItems().size()>0){
                String value = resp0.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql0 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }

            String uql1 = uqlList.get(1);
            System.out.println(uql1);
            Response resp1 = tx.run(uql1);
            if(resp1.getItems().size()>0){
                String value = resp1.getItems().get("isBlocked").asAttr().getValues().get(0).toString();
                System.out.println("uql1 isBlocked-->"+value);
                if("true".equals(value)){
                    isBlocked = true;
                }
            }else{
                isBlocked = true;
            }

            if (!isBlocked) {
                String uql2 = uqlList.get(2);
                System.out.println(uql2);
                tx.run(uql2);

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = tx.run(uql3);

                float sumAmount = Float.parseFloat(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                System.out.println("sumAmount:"+sumAmount);
                float threshold = Float.parseFloat(map.get("threshold").toString());
                System.out.println("threshold:"+threshold);
                if (sumAmount < threshold){
                    tx.rollback();
                    String uql4 = uqlList.get(4);
                    System.out.println(uql4);
                    tx.run(uql4);
                }
            }else{
                tx.rollback();
            }
            tx.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ReadWrite3 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.SRC_ID, UltipaConverter.convertIdStr(operation.getSrcId(),"Person|"))
                    .put(operation.DST_ID, UltipaConverter.convertIdStr(operation.getDstId(),"Person|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder(operation.getTruncationOrder()))
                    .build();
        }
    }


}
