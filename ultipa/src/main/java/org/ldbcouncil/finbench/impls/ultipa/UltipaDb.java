package org.ldbcouncil.finbench.impls.ultipa;

import com.google.common.collect.ImmutableMap;
import com.ultipa.sdk.connect.Connection;
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
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder2(operation.getTruncationOrder()))
                    .build();
        }
    }

    public static class ComplexRead7Handler extends UltipaListOperationHandler<ComplexRead7, ComplexRead7Result> {
        @Override
        public String getQueryString(UltipaDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionComplexRead7);
        }

        @Override
        public void executeOperation(ComplexRead7 operation,
                                     UltipaDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            //System.out.println(conn.sayHello("Hello"));
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }
            List<ComplexRead7Result> resultList = new ArrayList<>();
            try {
                float inOutRatio = 0.0F;

                int count1 = 0;
                float sum1 = 0.0F;
                int count2 = 0;
                float sum2 = 0.0F;
                String uql0 = uqlList.get(0);
                System.out.println(uql0);
                Response resp0 = conn.uql(uql0);
                if(resp0.getItems().size()>0){
                    count1 = Integer.parseInt(resp0.get(0).asTable().getRows().get(0).get(0).toString());
                    sum1 = Float.parseFloat(resp0.get(0).asTable().getRows().get(0).get(1).toString());
                }

                String uql1 = uqlList.get(1);
                System.out.println(uql1);
                Response resp1 = conn.uql(uql1);
                if(resp1.getItems().size()>0){
                    count2 = Integer.parseInt(resp1.get(0).asTable().getRows().get(0).get(0).toString());
                    sum2 = Float.parseFloat(resp1.get(0).asTable().getRows().get(0).get(1).toString());
                }else{
                    inOutRatio = -1;
                }

                if(inOutRatio != -1){
                    inOutRatio = Float.parseFloat(String.format("%.3f",sum1/sum2)) ;
                }

                ComplexRead7Result result = new ComplexRead7Result(
                        count1,count2,inOutRatio
                );
                resultList.add(result);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(1, resultList, operation);
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
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder2(operation.getTruncationOrder()))
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
        public void executeOperation(ComplexRead9 operation,
                                     UltipaDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            //System.out.println(conn.sayHello("Hello"));
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }
            List<ComplexRead9Result> resultList = new ArrayList<>();
            try {
                String ratioRepay = "0.0";
                String ratioDeposit = "0.0";
                String ratioTransfer = "0.0";

                float sum1 = 0.0F;
                float sum2 = 0.0F;
                float sum3 = 0.0F;
                float sum4 = 0.0F;
                String uql0 = uqlList.get(0);
                System.out.println(uql0);
                Response resp0 = conn.uql(uql0);
                if(resp0.getItems().size()>0){
                    sum1 = Float.parseFloat(resp0.get(0).asTable().getRows().get(0).get(0).toString());
                }

                String uql1 = uqlList.get(1);
                System.out.println(uql1);
                Response resp1 = conn.uql(uql1);
                if(resp1.getItems().size()>0){
                    sum2 = Float.parseFloat(resp1.get(0).asTable().getRows().get(0).get(0).toString());
                }else{
                    ratioRepay = "-1";
                }

                String uql2 = uqlList.get(2);
                System.out.println(uql2);
                Response resp2 = conn.uql(uql2);
                if(resp2.getItems().size()>0){
                    sum3 = Float.parseFloat(resp2.get(0).asTable().getRows().get(0).get(0).toString());
                }

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = conn.uql(uql3);
                if(resp3.getItems().size()>0){
                    sum4 = Float.parseFloat(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                }else{
                    ratioDeposit = "-1";
                    ratioTransfer = "-1";
                }

                if(!"-1".equals(ratioRepay)){
                    ratioRepay = String.format("%.3f",sum1/sum2);
                }
                if(!"-1".equals(ratioDeposit)){
                    ratioDeposit = String.format("%.3f",sum1/sum4);
                }
                if(!"-1".equals(ratioTransfer)){
                    ratioTransfer = String.format("%.3f",sum3/sum4);
                }
                ComplexRead9Result result = new ComplexRead9Result(
                        Float.parseFloat(ratioRepay),
                        Float.parseFloat(ratioDeposit),
                        Float.parseFloat(ratioTransfer)
                );
                resultList.add(result);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(1, resultList, operation);
        }

        @Override
        public List<ComplexRead9Result> toResult(Response resp) throws ParseException {
            return null;
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, ComplexRead9 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ID, UltipaConverter.convertIdStr(operation.getId(),"Account|"))
                    .put(operation.THRESHOLD, UltipaConverter.convertDouble(operation.getThreshold()))
                    .put(operation.START_TIME, UltipaConverter.convertDate(operation.getStartTime()))
                    .put(operation.END_TIME, UltipaConverter.convertDate(operation.getEndTime()))
                    .put(operation.TRUNCATION_LIMIT,UltipaConverter.convertInteger(operation.getTruncationLimit()))
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder2(operation.getTruncationOrder()))
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
                    .put(operation.TRUNCATION_ORDER,UltipaConverter.converOrder2(operation.getTruncationOrder()))
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
                        //new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(dataList.get(0).toString()),
                        new Date(Long.parseLong(dataList.get(0).toString())),
                        Boolean.parseBoolean(dataList.get(1).toString()),
                        dataList.get(2).toString()
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
        public void executeOperation(SimpleRead2 operation,
                                     UltipaDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            //System.out.println(conn.sayHello("Hello"));
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }

            List<SimpleRead2Result> results = new ArrayList<>();

            double rs1 = 0.0;
            double rm1 = 0.0;
            long c1 = 0;

            double rs2 = 0.0;
            double rm2 = 0.0;
            long c2 = 0;

            try {
                String uql0 = uqlList.get(0);
                System.out.println(uql0);
                Response resp0 = conn.uql(uql0);
                if(resp0.getItems().size()>0){
                    Table table = resp0.get(0).asTable();
                    rs1 = Double.parseDouble(table.getRows().get(0).get(0).toString());
                    rm1 = Double.parseDouble(table.getRows().get(0).get(1).toString());
                    c1 = Long.parseLong(table.getRows().get(0).get(2).toString());
                }else{
                    rm1 = -1;
                }

                String uql1 = uqlList.get(1);
                System.out.println(uql1);
                Response resp1 = conn.uql(uql1);
                if(resp1.getItems().size()>0){
                    Table table = resp1.get(0).asTable();
                    rs2 = Double.parseDouble(table.getRows().get(0).get(0).toString());
                    rm2 = Double.parseDouble(table.getRows().get(0).get(1).toString());
                    c2 = Long.parseLong(table.getRows().get(0).get(2).toString());
                }else{
                    rm2 = -1;
                }
                SimpleRead2Result result = new SimpleRead2Result(
                        rs1,rm1,c1,rs2,rm2,c2
                );
                results.add(result);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(1, results, operation);
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
        public void executeOperation(SimpleRead3 operation,
                                     UltipaDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = state.getConn();
            //System.out.println(conn.sayHello("Hello"));
            int resultCount;
            Map<String, Object> map = getParameters(state,operation);
            String query = getQueryString(state,operation);

            List<String> uqlList = new ArrayList<>();
            String[] querys = query.split(";");
            for (int i = 0; i < querys.length; i++) {
                String uql = UltipaConverter.replaceVariables(querys[i],map);
                uqlList.add(uql);
            }
            float ct1 = 0;
            float ct2 = 0;

            List<SimpleRead3Result> results = new ArrayList<>();
            try {

                String uql0 = uqlList.get(0);
                System.out.println(uql0);
                Response resp0 = conn.uql(uql0);
                if(resp0.getItems().size()>0){
                    Table table = resp0.get(0).asTable();
                    ct1 = Float.parseFloat(table.getRows().get(0).get(0).toString());
                }

                String uql1 = uqlList.get(1);
                System.out.println(uql1);
                Response resp1 = conn.uql(uql1);
                if(resp1.getItems().size()>0){
                    Table table = resp1.get(0).asTable();
                    ct2 = Float.parseFloat(table.getRows().get(0).get(0).toString());
                }
                float ratio = 0;
                if(ct2==0){
                    ratio = -1;
                }else{
                    ratio = (int)Math.round(ct1/ct2*1000)/1000f;
                }
                SimpleRead3Result result = new SimpleRead3Result(
                        ratio
                );
                results.add(result);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(1, results, operation);
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
                    .put(operation.IS_BLOCKED, UltipaConverter.convertBool(operation.getIsBlocked()))
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
                    .put(operation.IS_BLOCKED, UltipaConverter.convertBool(operation.getIsBlocked()))
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
                    .put(operation.MEDIUM_ID, UltipaConverter.convertIdStr(operation.getMediumId(),"Medium|"))
                    .put(operation.MEDIUM_TYPE, UltipaConverter.convertString(operation.getMediumType()))
                    .put(operation.IS_BLOCKED, UltipaConverter.convertBool(operation.getIsBlocked()))
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
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.ACCOUNT_BLOCKED, UltipaConverter.convertBool(operation.getAccountBlocked()))
                    .put(operation.ACCOUNT_TYPE, UltipaConverter.convertString(operation.getAccountType()))
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
                    .put(operation.COMPANY_ID, UltipaConverter.convertIdStr(operation.getCompanyId(),"Company|"))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.ACCOUNT_BLOCKED, UltipaConverter.convertBool(operation.getAccountBlocked()))
                    .put(operation.ACCOUNT_TYPE, UltipaConverter.convertString(operation.getAccountType()))
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
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.LOAN_AMOUNT, UltipaConverter.convertDouble(operation.getLoanAmount()))
                    .put(operation.BALANCE, UltipaConverter.convertDouble(operation.getBalance()))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
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
                    .put(operation.COMPANY_ID, UltipaConverter.convertIdStr(operation.getCompanyId(),"Company|"))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.LOAN_AMOUNT, UltipaConverter.convertDouble(operation.getLoanAmount()))
                    .put(operation.BALANCE, UltipaConverter.convertDouble(operation.getBalance()))
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
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
                    .put(operation.COMPANY_ID, UltipaConverter.convertIdStr(operation.getCompanyId(),"Company|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.RATIO, UltipaConverter.convertDouble(operation.getRatio()))
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
                    .put(operation.COMPANY_ID1, UltipaConverter.convertIdStr(operation.getCompanyId1(),"Company|"))
                    .put(operation.COMPANY_ID2, UltipaConverter.convertIdStr(operation.getCompanyId2(),"Company|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.RATIO, UltipaConverter.convertDouble(operation.getRatio()))
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
                    .put(operation.PERSON_ID1, UltipaConverter.convertIdStr(operation.getPersonId1(),"Person|"))
                    .put(operation.PERSON_ID2, UltipaConverter.convertIdStr(operation.getPersonId2(),"Person|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
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
                    .put(operation.COMPANY_ID1, UltipaConverter.convertIdStr(operation.getCompanyId1(),"Company|"))
                    .put(operation.COMPANY_ID2, UltipaConverter.convertIdStr(operation.getCompanyId2(),"Company|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
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
                    .put(operation.ACCOUNT_ID1, UltipaConverter.convertIdStr(operation.getAccountId1(),"Account|"))
                    .put(operation.ACCOUNT_ID2, UltipaConverter.convertIdStr(operation.getAccountId2(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
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
                    .put(operation.ACCOUNT_ID1, UltipaConverter.convertIdStr(operation.getAccountId1(),"Account|"))
                    .put(operation.ACCOUNT_ID2, UltipaConverter.convertIdStr(operation.getAccountId2(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write14Handler extends UltipaUpdateOperationHandler<Write14> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write14 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite14);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write14 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write15Handler extends UltipaUpdateOperationHandler<Write15> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write15 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite15);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write15 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.LOAN_ID, UltipaConverter.convertIdStr(operation.getLoanId(),"Loan|"))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .put(operation.AMOUNT, UltipaConverter.convertDouble(operation.getAmount()))
                    .build();
        }
    }

    public static class Write16Handler extends UltipaUpdateOperationHandler<Write16> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write16 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite16);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write16 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.MEDIUM_ID, UltipaConverter.convertIdStr(operation.getMediumId(),"Medium|"))
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .put(operation.TIME, UltipaConverter.convertDate(operation.getTime()))
                    .build();
        }
    }

    public static class Write17Handler extends UltipaUpdateOperationHandler<Write17> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write17 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite17);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write17 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .build();
        }
    }

    public static class Write18Handler extends UltipaUpdateOperationHandler<Write18> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write18 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite18);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write18 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.ACCOUNT_ID, UltipaConverter.convertIdStr(operation.getAccountId(),"Account|"))
                    .build();
        }
    }

    public static class Write19Handler extends UltipaUpdateOperationHandler<Write19> {

        @Override
        public String getQueryString(UltipaDbConnectionState state, Write19 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.TransactionWrite19);
        }

        @Override
        public Map<String, Object> getParameters(UltipaDbConnectionState state, Write19 operation) {
            return new ImmutableMap.Builder<String, Object>()
                    .put(operation.PERSON_ID, UltipaConverter.convertIdStr(operation.getPersonId(),"Person|"))
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

            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = conn.uql(uql0);
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
            Response resp1 = conn.uql(uql1);
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
                Response resp2 = conn.uql(uql2);
                List<Edge> edges = resp2.get(0).asEdges();

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = conn.uql(uql3);
                if(resp3.getItems().size()>0){
                    for(Edge e : edges){
                        System.out.println("delete().edges("+e.getUUID()+")");
                        conn.uql("delete().edges("+e.getUUID()+")");
                    }
                    String uql4 = uqlList.get(4);
                    System.out.println(uql4);
                    conn.uql(uql4);
                }
            }
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

            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = conn.uql(uql0);
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
            Response resp1 = conn.uql(uql1);
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
                Response resp2 = conn.uql(uql2);
                List<Edge> edges = resp2.get(0).asEdges();

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = conn.uql(uql3);
                if (resp3.getItems().size()>0){
                    System.out.println(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                    float ratio = Float.parseFloat(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                    float ratioThreshold = Float.parseFloat(map.get("ratioThreshold").toString());
                    System.out.println("ratioThreshold:"+ratioThreshold);
                    if (ratio <= ratioThreshold){
                        for(Edge e : edges){
                            System.out.println("delete().edges("+e.getUUID()+")");
                            conn.uql("delete().edges("+e.getUUID()+")");
                        }
                        String uql4 = uqlList.get(4);
                        System.out.println(uql4);
                        conn.uql(uql4);
                    }
                }
            }
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

            boolean isBlocked = false;
            String uql0 = uqlList.get(0);
            System.out.println(uql0);
            Response resp0 = conn.uql(uql0);
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
            Response resp1 = conn.uql(uql1);
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
                Response resp2 = conn.uql(uql2);
                List<Edge> edges = resp2.get(0).asEdges();

                String uql3 = uqlList.get(3);
                System.out.println(uql3);
                Response resp3 = conn.uql(uql3);

                if(resp3.getItems().size()>0){
                    float sumAmount = Float.parseFloat(resp3.get(0).asTable().getRows().get(0).get(0).toString());
                    System.out.println("sumAmount:"+sumAmount);
                    float threshold = Float.parseFloat(map.get("threshold").toString());
                    System.out.println("threshold:"+threshold);
                    if (sumAmount > threshold){
                        for(Edge e : edges){
                            System.out.println("delete().edges("+e.getUUID()+")");
                            conn.uql("delete().edges("+e.getUUID()+")");
                        }
                        String uql4 = uqlList.get(4);
                        System.out.println(uql4);
                        conn.uql(uql4);
                    }
                }
            }
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
