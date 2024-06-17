package org.ldbcouncil.finbench.impls.tugraph;

import com.antgroup.tugraph.TuGraphDbRpcClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.result.Path;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.ArrayList;

import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import com.alibaba.fastjson.*;

public class TuGraphTransactionDb extends Db {
    static Logger logger = LogManager.getLogger("TuGraph");

    TuGraphDbConnectionState dcs;

    // Return byte type of 1 if it's ordered DESC
    private static byte encodeTruncationOrder(TruncationOrder truncationOrder) {
        return (byte) (truncationOrder == TruncationOrder.TIMESTAMP_DESCENDING ? 1 : 0);
    }

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("TuGraphTransactionDb initialized");

        try {
            dcs = new TuGraphDbConnectionState(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        logger.info("TuGraphTransactionDb closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH p = (acc:Account {id:%d})-[e1:transfer *1..3]->(other:Account)<-[e2:signIn]-(medium) WHERE isAsc(getMemberProp(e1, 'timestamp'))=true AND head(getMemberProp(e1, 'timestamp')) > %d AND last(getMemberProp(e1, 'timestamp')) < %d AND e2.timestamp > %d AND e2.timestamp < %d AND medium.isBlocked = true RETURN DISTINCT other.id as otherId, length(p)-1 as accountDistance, medium.id as mediumId, medium.type as mediumType ORDER BY accountDistance, otherId, mediumId;";
                long startTime = cr1.getStartTime().getTime();
                long endTime = cr1.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr1.getId(), startTime, endTime, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr1.getTruncationLimit());
                ArrayList<ComplexRead1Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead1Result result = new ComplexRead1Result(
                            ob.getLongValue("otherId"),
                            ob.getIntValue("accountDistance"),
                            ob.getLongValue("mediumId"),
                            ob.getString("mediumType"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr1);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p:Person {id:%d})-[e1:own]->(acc:Account) <-[e2:transfer*1..3]-(other:Account) WHERE isDesc(getMemberProp(e2, 'timestamp'))=true AND head(getMemberProp(e2, 'timestamp')) < %d AND last(getMemberProp(e2, 'timestamp')) > %d WITH DISTINCT other MATCH (other)<-[e3:deposit]-(loan:Loan) WHERE e3.timestamp > %d AND e3.timestamp < %d WITH DISTINCT other.id AS otherId, loan.loanAmount AS loanAmount, loan.balance AS loanBalance WITH otherId AS otherId, sum(loanAmount) as sumLoanAmount, sum(loanBalance) as sumLoanBalance RETURN otherId, round(sumLoanAmount * 1000) / 1000 as sumLoanAmount, round(sumLoanBalance * 1000) / 1000 as sumLoanBalance ORDER BY sumLoanAmount DESC, otherId ASC;";
                long startTime = cr2.getStartTime().getTime();
                long endTime = cr2.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr2.getId(), endTime, startTime, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr2.getTruncationLimit());
                ArrayList<ComplexRead2Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead2Result result = new ComplexRead2Result(
                            ob.getLongValue("otherId"),
                            ob.getDoubleValue("sumLoanAmount"),
                            ob.getDoubleValue("sumLoanBalance"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr2);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (src:Account{id:%d}), (dst:Account{id:%d}) CALL algo.shortestPath( src, dst, { direction: 'PointingRight', relationshipQuery:'transfer', edgeFilter: { timestamp: { smaller_than: %d, greater_than: %d } } } ) YIELD nodeCount RETURN nodeCount - 1 AS len;";
                long startTime = cr3.getStartTime().getTime();
                long endTime = cr3.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr3.getId1(), cr3.getId2(), endTime, startTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<ComplexRead3Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead3Result result = new ComplexRead3Result(
                            ob.getLongValue("len"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr3);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (src:Account {id:%d})-[e1:transfer]->(dst:Account {id:%d}) WHERE e1.timestamp > %d AND e1.timestamp < %d WITH src, dst.id as dstid MATCH (src)<-[e2:transfer]-(other:Account)<-[e3:transfer]-(dst:Account) WHERE dst.id=dstid AND e2.timestamp > %d AND e2.timestamp < %d AND e3.timestamp > %d AND e3.timestamp < %d WITH DISTINCT src, other, dst MATCH (src)<-[e2:transfer]-(other) WHERE e2.timestamp > %d AND e2.timestamp < %d WITH src, other, dst, count(e2) as numEdge2, sum(e2.amount) as sumEdge2Amount, max(e2.amount) as maxEdge2Amount MATCH (other)<-[e3:transfer]-(dst) WHERE e3.timestamp > %d AND e3.timestamp < %d WITH other.id as otherId, numEdge2, sumEdge2Amount, maxEdge2Amount, count(e3) as numEdge3, sum(e3.amount) as sumEdge3Amount, max(e3.amount) as maxEdge3Amount RETURN otherId, numEdge2, round(sumEdge2Amount * 1000) / 1000 as sumEdge2Amount, round(maxEdge2Amount * 1000) / 1000 as maxEdge2Amount, numEdge3, round(sumEdge3Amount * 1000) / 1000 as sumEdge3Amount, round(maxEdge3Amount * 1000) / 1000 as maxEdge3Amount ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, otherId ASC;";
                long startTime = cr4.getStartTime().getTime();
                long endTime = cr4.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr4.getId1(), cr4.getId2(), startTime, endTime, startTime, endTime, startTime, endTime,
                        startTime, endTime, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<ComplexRead4Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead4Result result = new ComplexRead4Result(
                            ob.getLongValue("otherId"),
                            ob.getLongValue("numEdge2"),
                            ob.getDoubleValue("sumEdge2Amount"),
                            ob.getDoubleValue("maxEdge2Amount"),
                            ob.getLongValue("numEdge3"),
                            ob.getDoubleValue("sumEdge3Amount"),
                            ob.getDoubleValue("maxEdge3Amount"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr4);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (person:Person {id:%d})-[e1:own]->(src:Account) WITH src MATCH p=(src)-[e2:transfer*1..3]->(dst:Account) WHERE isAsc(getMemberProp(e2, 'timestamp'))=true AND head(getMemberProp(e2, 'timestamp')) > %d AND last(getMemberProp(e2, 'timestamp')) < %d WITH DISTINCT getMemberProp(nodes(p), 'id') as path, length(p) as len ORDER BY len DESC WHERE hasDuplicates(path)=false RETURN path;";
                long startTime = cr5.getStartTime().getTime();
                long endTime = cr5.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr5.getId(), startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr5.getTruncationLimit());
                ArrayList<ComplexRead5Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONArray json_path = array.getJSONObject(i).getJSONArray("path");
                    Path path = new Path();
                    for (int j = 0; j < json_path.size(); j++) {
                        path.addId(json_path.getLongValue(j));
                    }
                    ComplexRead5Result result = new ComplexRead5Result(path);
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr5);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (dstCard:Account {id:%d} )<-[edge2:withdraw]-(mid:Account) WHERE dstCard.type ENDS WITH 'card' AND edge2.timestamp > %d AND edge2.timestamp < %d AND edge2.amount > %f WITH mid, sum(edge2.amount) as sumEdge2Amount, count(edge2.amount) as t MATCH (mid)<-[edge1:transfer]-(src:Account) WHERE edge1.timestamp > %d AND edge1.timestamp < %d AND edge1.amount > %f WITH mid.id AS midId, count(edge1) AS edge1Count, sum(edge1.amount) AS sumEdge1Amount, sumEdge2Amount WHERE edge1Count > 3 WITH midId, sumEdge1Amount, sumEdge2Amount RETURN midId, round(sumEdge1Amount * 1000) / 1000 as sumEdge1Amount, round(sumEdge2Amount * 1000) / 1000 as sumEdge2Amount ORDER BY sumEdge2Amount DESC;";
                long startTime = cr6.getStartTime().getTime();
                long endTime = cr6.getEndTime().getTime();
                double threshold1 = cr6.getThreshold1();
                double threshold2 = cr6.getThreshold2();
                cypher = String.format(
                        cypher,
                        cr6.getId(), startTime, endTime, threshold2, startTime, endTime, threshold1);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr6.getTruncationLimit());
                ArrayList<ComplexRead6Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead6Result result = new ComplexRead6Result(
                            ob.getLongValue("midId"),
                            ob.getDoubleValue("sumEdge1Amount"),
                            ob.getDoubleValue("sumEdge2Amount"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr6);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (mid:Account {id:%d}) WITH mid OPTIONAL MATCH (mid)-[edge2:transfer]->(dst:Account) WHERE edge2.timestamp > %d AND edge2.timestamp < %d AND edge2.amount > %f WITH mid, count(distinct dst) as numDst, sum(edge2.amount) as amountDst OPTIONAL MATCH (mid)<-[edge1:transfer]-(src:Account) WHERE edge1.timestamp > %d AND edge1.timestamp < %d AND edge1.amount > %f WITH count(distinct src) as numSrc, sum(edge1.amount) as amountSrc, numDst, amountDst RETURN numSrc, numDst, CASE WHEN amountDst=0 THEN -1 ELSE round(1000.0 * amountSrc / amountDst) / 1000 END AS inOutRatio;";
                long startTime = cr7.getStartTime().getTime();
                long endTime = cr7.getEndTime().getTime();
                double threshold = cr7.getThreshold();
                cypher = String.format(
                        cypher,
                        cr7.getId(), startTime, endTime, threshold, startTime, endTime, threshold);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr7.getTruncationLimit());
                ArrayList<ComplexRead7Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead7Result result = new ComplexRead7Result(
                            ob.getIntValue("numSrc"),
                            ob.getIntValue("numDst"),
                            ob.getFloatValue("inOutRatio"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr7);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CALL plugin.cpp.tcr8({ id:%d, threshold: %f, startTime: %d, endTime: %d, limit: %d });";
                long startTime = cr8.getStartTime().getTime();
                long endTime = cr8.getEndTime().getTime();
                double threshold = cr8.getThreshold();
                cypher = String.format(
                        cypher,
                        cr8.getId(), threshold, startTime, endTime, cr8.getTruncationLimit());
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr8.getTruncationLimit());
                ArrayList<ComplexRead8Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject item = array.getJSONObject(i);
                    ComplexRead8Result result = new ComplexRead8Result(
                            item.getLongValue("i"),
                            item.getFloatValue("r"),
                            item.getIntValue("d"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr8);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (mid:Account {id:%d}) WITH mid OPTIONAL MATCH (mid)-[edge2:repay]->(loan:Loan) WHERE edge2.amount > %f AND edge2.timestamp > %d AND edge2.timestamp < %d WITH mid, sum(edge2.amount) AS edge2Amount OPTIONAL MATCH (mid)<-[edge1:deposit]-(loan:Loan) WHERE edge1.amount > %f AND edge1.timestamp > %d AND edge1.timestamp < %d WITH mid, sum(edge1.amount) AS edge1Amount, edge2Amount OPTIONAL MATCH (mid)-[edge4:transfer]->(down:Account) WHERE edge4.amount > %f AND edge4.timestamp > %d AND edge4.timestamp < %d WITH mid, edge1Amount, edge2Amount, sum(edge4.amount) AS edge4Amount OPTIONAL MATCH (mid)<-[edge3:transfer]-(up:Account) WHERE edge3.amount > %f AND edge3.timestamp > %d AND edge3.timestamp < %d WITH edge1Amount, edge2Amount, sum(edge3.amount) AS edge3Amount, edge4Amount RETURN CASE WHEN edge2Amount=0 THEN -1 ELSE round(1000.0 * edge1Amount / edge2Amount) / 1000 END AS ratioRepay, CASE WHEN edge4Amount=0 THEN -1 ELSE round(1000.0 * edge1Amount / edge4Amount) / 1000 END AS ratioDeposit, CASE WHEN edge4Amount=0 THEN -1 ELSE round(1000.0 * edge3Amount / edge4Amount) / 1000 END AS ratioTransfer;";
                long startTime = cr9.getStartTime().getTime();
                long endTime = cr9.getEndTime().getTime();
                double threshold = cr9.getThreshold();
                cypher = String.format(
                        cypher,
                        cr9.getId(),
                        threshold, startTime, endTime,
                        threshold, startTime, endTime,
                        threshold, startTime, endTime,
                        threshold, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr9.getTruncationLimit());
                ArrayList<ComplexRead9Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead9Result result = new ComplexRead9Result(
                            ob.getFloatValue("ratioRepay"),
                            ob.getFloatValue("ratioDeposit"),
                            ob.getFloatValue("ratioTransfer"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr9);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p1:Person {id:%d})-[edge1:invest]->(m1:Company) WHERE edge1.timestamp > %d AND edge1.timestamp < %d WITH collect(distinct id(m1)) as m1_vids MATCH (p2:Person {id:%d})-[edge2:invest]->(m2:Company) WHERE edge2.timestamp > %d AND edge2.timestamp < %d WITH collect(distinct id(m2)) as m2_vids, m1_vids CALL algo.jaccard(m1_vids, m2_vids) YIELD similarity RETURN round(similarity*1000)/1000 AS similarity;";
                long startTime = cr10.getStartTime().getTime();
                long endTime = cr10.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr10.getPid1(), startTime, endTime,
                        cr10.getPid2(), startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<ComplexRead10Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead10Result result = new ComplexRead10Result(
                            ob.getFloatValue("similarity"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr10);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p1:Person {id:%d})-[edge:guarantee*1..5]->(pN:Person) -[:apply]->(loan:Loan) WHERE minInList(getMemberProp(edge, 'timestamp')) > %d AND maxInList(getMemberProp(edge, 'timestamp')) < %d WITH DISTINCT loan WITH sum(loan.loanAmount) as sumLoanAmount, count(distinct loan) as numLoans RETURN round(sumLoanAmount * 1000) / 1000 as sumLoanAmount, numLoans;";
                long startTime = cr11.getStartTime().getTime();
                long endTime = cr11.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr11.getId(), startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr11.getTruncationLimit());
                ArrayList<ComplexRead11Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead11Result result = new ComplexRead11Result(
                            ob.getDoubleValue("sumLoanAmount"),
                            ob.getIntValue("numLoans"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr11);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (person:Person {id:%d})-[edge1:own]->(pAcc:Account) -[edge2:transfer]->(compAcc:Account) <-[edge3:own]-(com:Company) WHERE edge2.timestamp > %d AND edge2.timestamp < %d WITH compAcc.id AS compAccountId, sum(edge2.amount) AS sumEdge2Amount RETURN compAccountId, round(sumEdge2Amount * 1000) / 1000 as sumEdge2Amount ORDER BY sumEdge2Amount DESC;";
                long startTime = cr12.getStartTime().getTime();
                long endTime = cr12.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        cr12.getId(), startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0, cr12.getTruncationLimit());
                ArrayList<ComplexRead12Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead12Result result = new ComplexRead12Result(
                            ob.getLongValue("compAccountId"),
                            ob.getDoubleValue("sumEdge2Amount"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr12);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (n:Account{id:%d}) RETURN n.createTime as createTime, n.isBlocked as isBlocked, n.type as type;";
                cypher = String.format(
                        cypher,
                        sr1.getId());
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead1Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead1Result result = new SimpleRead1Result(
                            new Date(ob.getLongValue("createTime")),
                            ob.getBooleanValue("isBlocked"),
                            ob.getString("type"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr1);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (n:Account{id:%d}) WITH n OPTIONAL MATCH (n)-[e:transfer]->(m:Account) WHERE e.timestamp > %d AND e.timestamp < %d WITH n, sum(e.amount) as sumEdge1Amount, max(e.amount) as maxEdge1Amount, count(e) as numEdge1 OPTIONAL MATCH (n)<-[e:transfer]-(m:Account) WHERE e.timestamp > %d AND e.timestamp < %d WITH sumEdge1Amount, maxEdge1Amount, numEdge1, sum(e.amount) as sumEdge2Amount, max(e.amount) as maxEdge2Amount, count(e) as numEdge2 RETURN round(sumEdge1Amount * 1000) / 1000 as sumEdge1Amount, CASE WHEN maxEdge1Amount < 0 THEN -1 ELSE round(maxEdge1Amount * 1000) / 1000 END as maxEdge1Amount, numEdge1, round(sumEdge2Amount * 1000) / 1000 as sumEdge2Amount, CASE WHEN maxEdge2Amount < 0 THEN -1 ELSE round(maxEdge2Amount * 1000) / 1000 END as maxEdge2Amount, numEdge2;";
                long startTime = sr2.getStartTime().getTime();
                long endTime = sr2.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        sr2.getId(), startTime, endTime, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead2Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead2Result result = new SimpleRead2Result(
                            ob.getDoubleValue("sumEdge1Amount"),
                            ob.getDoubleValue("maxEdge1Amount"),
                            ob.getLongValue("numEdge1"),
                            ob.getDoubleValue("sumEdge2Amount"),
                            ob.getDoubleValue("maxEdge2Amount"),
                            ob.getLongValue("numEdge2"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr2);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "OPTIONAL MATCH (n:Account{id:%d})<-[e:transfer]-(m:Account) WHERE e.amount > %f AND e.timestamp > %d AND e.timestamp < %d AND m.isBlocked=true WITH count(m) * 1.0 as numM OPTIONAL MATCH (n:Account{id:%d})<-[e:transfer]-(m:Account) WITH count(m) as numIn, numM RETURN CASE WHEN numIn = 0 THEN -1 ELSE round(numM / numIn * 1000) / 1000 END as blockRatio;";
                long startTime = sr3.getStartTime().getTime();
                long endTime = sr3.getEndTime().getTime();
                double threshold = sr3.getThreshold();
                cypher = String.format(
                        cypher,
                        sr3.getId(), threshold, startTime, endTime, sr3.getId());
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead3Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead3Result result = new SimpleRead3Result(
                            ob.getFloatValue("blockRatio"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr3);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (n:Account{id:%d}) WITH n MATCH (n)-[e:transfer]->(m:Account) WHERE e.amount > %f AND e.timestamp > %d AND e.timestamp < %d WITH m.id as dstId, count(e) as numEdges, sum(e.amount) as sumAmount RETURN dstId, numEdges, round(sumAmount * 1000) / 1000 as sumAmount ORDER BY sumAmount DESC, dstId ASC;";
                long startTime = sr4.getStartTime().getTime();
                long endTime = sr4.getEndTime().getTime();
                double threshold = sr4.getThreshold();
                cypher = String.format(
                        cypher,
                        sr4.getId(), threshold, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead4Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead4Result result = new SimpleRead4Result(
                            ob.getLongValue("dstId"),
                            ob.getIntValue("numEdges"),
                            ob.getDoubleValue("sumAmount"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr4);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (n:Account{id:%d})<-[e:transfer]-(m:Account) WHERE e.amount > %f AND e.timestamp > %d AND e.timestamp < %d WITH m.id as srcId, count(e) as numEdges, sum(e.amount) as sumAmount RETURN srcId, numEdges, round(sumAmount * 1000) / 1000 as sumAmount ORDER BY sumAmount DESC, srcId ASC;";
                long startTime = sr5.getStartTime().getTime();
                long endTime = sr5.getEndTime().getTime();
                double threshold = sr5.getThreshold();
                cypher = String.format(
                        cypher,
                        sr5.getId(), threshold, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead5Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead5Result result = new SimpleRead5Result(
                            ob.getLongValue("srcId"),
                            ob.getIntValue("numEdges"),
                            ob.getDoubleValue("sumAmount"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr5);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (src:Account{id:%d})<-[e1:transfer]-(m:Account) -[e2:transfer]->(dst:Account) WHERE dst.isBlocked = true AND src.id <> dst.id AND e1.timestamp > %d AND e1.timestamp < %d AND e2.timestamp > %d AND e2.timestamp < %d RETURN DISTINCT dst.id as dstId ORDER BY dstId ASC;";
                long startTime = sr6.getStartTime().getTime();
                long endTime = sr6.getEndTime().getTime();
                cypher = String.format(
                        cypher,
                        sr6.getId(), startTime, endTime, startTime, endTime);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<SimpleRead6Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size(); i++) {
                    JSONObject ob = array.getJSONObject(i);
                    SimpleRead6Result result = new SimpleRead6Result(
                            ob.getLongValue("dstId"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, sr6);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CREATE (p:Person{id:%d, name:\"%s\", isBlocked:%s});";
                cypher = String.format(
                        cypher,
                        w1.getPersonId(), w1.getPersonName(), String.valueOf(w1.getIsBlocked()));
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CREATE (:Company {id:%d, name:\"%s\", isBlocked:%s});";
                cypher = String.format(
                        cypher,
                        w2.getCompanyId(), w2.getCompanyName(), String.valueOf(w2.getIsBlocked()));
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CREATE (:Medium {id: %d, isBlocked: %s, type: '%s'});";
                cypher = String.format(
                        cypher,
                        w3.getMediumId(), String.valueOf(w3.getIsBlocked()), w3.getMediumType());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p:Person{id:%d}) WITH p CREATE(p)-[:own]->(a:Account{id:%d, createTime:%d, isBlocked:%s, type:'%s'});";
                cypher = String.format(
                        cypher,
                        w4.getPersonId(), w4.getAccountId(), w4.getTime().getTime(),
                        String.valueOf(w4.getAccountBlocked()), w4.getAccountType());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (c:Company {id:%d}) WITH c CREATE (c)-[:own]->(:Account {id: %d, createTime: %d, isBlocked: %s, type: '%s'});";
                cypher = String.format(
                        cypher,
                        w5.getCompanyId(), w5.getAccountId(), w5.getTime().getTime(),
                        String.valueOf(w5.getAccountBlocked()), w5.getAccountType());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p:Person {id: %d}) CREATE (:Loan {id: %d, loanAmount: %f, balance: %f})<-[:apply {timestamp: %d}]-(p);";
                cypher = String.format(
                        cypher,
                        w6.getPersonId(), w6.getLoanId(), w6.getLoanAmount(), w6.getBalance(), w6.getTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (c:Company {id: %d}) CREATE (:Loan {id: %d, loanAmount: %f, balance: %f})<-[:apply {timestamp: %d}]-(c);";
                cypher = String.format(
                        cypher,
                        w7.getCompanyId(), w7.getLoanId(), w7.getLoanAmount(), w7.getBalance(), w7.getTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p:Person{id:%d}), (c:Company{id:%d}) CREATE (p)-[:invest{timestamp: %d, ratio: %f}]->(c);";
                cypher = String.format(
                        cypher,
                        w8.getPersonId(), w8.getCompanyId(), w8.getTime().getTime(), w8.getRatio());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (c1:Company{id:%d}), (c2:Company{id:%d}) CREATE (c1)-[:invest{timestamp: %d, ratio: %f}]->(c2);";
                cypher = String.format(
                        cypher,
                        w9.getCompanyId1(), w9.getCompanyId2(), w9.getTime().getTime(), w9.getRatio());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p1:Person{id:%d}), (p2:Person{id:%d}) CREATE (p1)-[:guarantee{timestamp: %d}]->(p2);";
                cypher = String.format(
                        cypher,
                        w10.getPersonId1(), w10.getPersonId2(), w10.getTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (c1:Company{id:%d}), (c2:Company{id:%d}) CREATE (c1)-[:guarantee{timestamp: %d}]->(c2);";
                cypher = String.format(
                        cypher,
                        w11.getCompanyId1(), w11.getCompanyId2(), w11.getTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (a1:Account{id:%d}), (a2:Account{id:%d}) CREATE (a1)-[:transfer{timestamp: %d, amount: %f}]->(a2);";
                cypher = String.format(
                        cypher,
                        w12.getAccountId1(), w12.getAccountId2(), w12.getTime().getTime(), w12.getAmount());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (a1:Account{id:%d}), (a2:Account{id:%d}) CREATE (a1)-[:withdraw{timestamp: %d, amount: %f}]->(a2);";
                cypher = String.format(
                        cypher,
                        w13.getAccountId1(), w13.getAccountId2(), w13.getTime().getTime(), w13.getAmount());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (a:Account{id:%d}), (loan:Loan{id:%d}) CREATE (a)-[:repay{timestamp: %d, amount: %f}]->(loan);";
                cypher = String.format(
                        cypher,
                        w14.getAccountId(), w14.getLoanId(), w14.getTime().getTime(), w14.getAmount());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (a:Account{id:%d}), (loan:Loan{id:%d}) CREATE (a)<-[:deposit{timestamp: %d, amount: %f}]-(loan);";
                cypher = String.format(
                        cypher,
                        w15.getAccountId(), w15.getLoanId(), w15.getTime().getTime(), w15.getAmount());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (acc:Account {id:%d}), (med:Medium {id:%d}) CREATE (acc)<-[:signIn {timestamp: %d}]-(med);";
                cypher = String.format(
                        cypher,
                        w16.getAccountId(), w16.getMediumId(), w16.getTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (acc:Account {id: %d}) WITH acc OPTIONAL MATCH (acc)-[:repay|deposit]-(loan:Loan) DELETE acc, loan;";
                cypher = String.format(
                        cypher,
                        w17.getAccountId());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (a:Account {id:%d}) SET a.isBlocked = true;";
                cypher = String.format(
                        cypher,
                        w18.getAccountId());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "MATCH (p:Person {id:%d}) SET p.isBlocked = true;";
                cypher = String.format(
                        cypher,
                        w19.getPersonId());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CALL plugin.cpp.trw1({srcId: %d, dstId: %d, time: %d, amt: %f, startTime: %d, endTime: %d});";
                cypher = String.format(
                        cypher,
                        rw1.getSrcId(), rw1.getDstId(),
                        rw1.getTime().getTime(), rw1.getAmount(),
                        rw1.getStartTime().getTime(), rw1.getEndTime().getTime());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CALL plugin.cpp.trw2({ srcId: %d, dstId: %d, time: %d, amt: %f, threshold: %f, startTime: %d, endTime: %d, limit: %d});";
                cypher = String.format(
                        cypher,
                        rw2.getSrcId(), rw2.getDstId(),
                        rw2.getTime().getTime(), rw2.getAmount(),
                        rw2.getAmountThreshold(),
                        rw2.getStartTime().getTime(), rw2.getEndTime().getTime(), rw2.getTruncationLimit());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CALL plugin.cpp.trw3({ srcId: %d, dstId: %d, time: %d, threshold: %f, startTime: %d, endTime: %d, limit: %d});";
                cypher = String.format(
                        cypher,
                        rw3.getSrcId(), rw3.getDstId(),
                        rw3.getTime().getTime(), rw3.getThreshold(),
                        rw3.getStartTime().getTime(), rw3.getEndTime().getTime(), rw3.getTruncationLimit());
                String graph = "default";
                client.callCypher(cypher, graph, 0);
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
