package org.ldbcouncil.finbench.impls.tigergraph;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.ldbcouncil.finbench.impls.dummy.DummyDb;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TigerGraphTransactionDb extends Db {
    static Logger logger = LogManager.getLogger("TigerGraph");
    private static final int QUERY_TIMEOUT_SECONDS = 60;

    TigerGraphDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("TigerGraphTransactionDb initialized");

        try {
            dcs = new TigerGraphDbConnectionState(properties);
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
        logger.info("TigerGraphTransactionDb closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    private static double getRoundedDouble(ResultSet rs, String columnName) throws SQLException {
        return rs.getBigDecimal(columnName).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private static void closeResources(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr1(id=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr1.getId());
                pstmt.setLong(2, cr1.getStartTime().getTime());
                pstmt.setLong(3, cr1.getEndTime().getTime());
                pstmt.setInt(4, cr1.getTruncationLimit());
                pstmt.setString(5, String.valueOf(cr1.getTruncationOrder()));
                rs = pstmt.executeQuery();
                List<ComplexRead1Result> results = new ArrayList<>();

                while (rs.next()) {
                    ComplexRead1Result rowResult = new ComplexRead1Result(
                            rs.getLong("otherId"),
                            rs.getInt("accountDistance"),
                            rs.getLong("mediumId"),
                            rs.getString("mediumType"));
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr1);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr2(id=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr2.getId());
                pstmt.setLong(2, cr2.getStartTime().getTime());
                pstmt.setLong(3, cr2.getEndTime().getTime());
                pstmt.setInt(4, cr2.getTruncationLimit());
                pstmt.setString(5, String.valueOf(cr2.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead2Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead2Result rowResult = new ComplexRead2Result(
                            rs.getLong("otherId"),
                            getRoundedDouble(rs,"sumLoanAmount"),
                            getRoundedDouble(rs,"sumLoanBalance")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr2);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }
    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr3(id1=?, id2=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr3.getId1());
                pstmt.setLong(2, cr3.getId2());
                pstmt.setLong(3, cr3.getStartTime().getTime());
                pstmt.setLong(4, cr3.getEndTime().getTime());

                rs = pstmt.executeQuery();
                List<ComplexRead3Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead3Result rowResult = new ComplexRead3Result(rs.getLong("shortestPathLength"));
                    results.add(rowResult);
                }

                resultReporter.report(results.size(), results, cr3);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }
    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr4(id1=?, id2=?, startTime=?, endTime=?)";
                long startTime = cr4.getStartTime().getTime();
                long endTime = cr4.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr4.getId1());
                pstmt.setLong(2, cr4.getId2());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);

                rs = pstmt.executeQuery();
                List<ComplexRead4Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead4Result rowResult = new ComplexRead4Result(
                            rs.getLong("otherId"),
                            rs.getLong("numEdge2"),
                            getRoundedDouble(rs, "sumEdge2Amount"),
                            getRoundedDouble(rs, "maxEdge2Amount"),
                            rs.getLong("numEdge3"),
                            getRoundedDouble(rs, "sumEdge3Amount"),
                            getRoundedDouble(rs, "maxEdge3Amount")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr4);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr5(id=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                long startTime = cr5.getStartTime().getTime();
                long endTime = cr5.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr5.getId());
                pstmt.setLong(2, startTime);
                pstmt.setLong(3, endTime);
                pstmt.setInt(4, cr5.getTruncationLimit());
                pstmt.setString(5, String.valueOf(cr5.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead5Result> results = new ArrayList<>();
                while (rs.next()) {
                    Array pathArray = rs.getArray("path");
                    Object[] arrayContent = (Object[]) pathArray.getArray();
                    for (Object element : arrayContent) {
                        String[] numberStrings = element.toString().split("->");
                        long[] longArray = new long[numberStrings.length];
                        for (int i = 0; i < numberStrings.length; i++) {
                            longArray[i] = Long.parseLong(numberStrings[i]);
                        }
                        Path path = new Path();
                        for (long num : longArray) {
                            path.addId(num);
                        }
                        ComplexRead5Result result = new ComplexRead5Result(path);
                        results.add(result);
                    }
                }
                resultReporter.report(results.size(), results, cr5);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }
    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr6(accountId=?, threshold1=?, threshold2=?, startTime=?, endTime=?, " +
                        "truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr6.getId());
                pstmt.setDouble(2, cr6.getThreshold1());
                pstmt.setDouble(3, cr6.getThreshold2());
                pstmt.setLong(4, cr6.getStartTime().getTime());
                pstmt.setLong(5, cr6.getEndTime().getTime());
                pstmt.setInt(6, cr6.getTruncationLimit());
                pstmt.setString(7, String.valueOf(cr6.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead6Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead6Result rowResult = new ComplexRead6Result(
                            rs.getLong("midId"),
                            getRoundedDouble(rs, "sumEdge1Amount"),
                            getRoundedDouble(rs, "sumEdge2Amount")
                            );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr6);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr7(accountId=?, threshold=?, startTime=?, endTime=?, " +
                        "truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr7.getId());
                pstmt.setDouble(2, cr7.getThreshold());
                pstmt.setLong(3, cr7.getStartTime().getTime());
                pstmt.setLong(4, cr7.getEndTime().getTime());
                pstmt.setInt(5, cr7.getTruncationLimit());
                pstmt.setString(6, String.valueOf(cr7.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead7Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead7Result rowResult = new ComplexRead7Result(
                            rs.getInt("numSrc"),
                            rs.getInt("numDst"),
                            rs.getFloat("inOutRatio"));
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr7);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr8(loanId=?, threshold=?, startTime=?, endTime=?, " +
                        "truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr8.getId());
                pstmt.setDouble(2, cr8.getThreshold());
                pstmt.setLong(3, cr8.getStartTime().getTime());
                pstmt.setLong(4, cr8.getEndTime().getTime());
                pstmt.setInt(5, cr8.getTruncationLimit());
                pstmt.setString(6, String.valueOf(cr8.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead8Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead8Result rowResult = new ComplexRead8Result(
                            rs.getLong("dstId"),
                            rs.getFloat("final_ratio"),
                            rs.getInt("distanceFromLoan"));
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr8);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr9(id=?, threshold=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                long startTime = cr9.getStartTime().getTime();
                long endTime = cr9.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr9.getId());
                pstmt.setDouble(2, cr9.getThreshold());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);
                pstmt.setInt(5, cr9.getTruncationLimit());
                pstmt.setString(6, String.valueOf(cr9.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead9Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead9Result rowResult = new ComplexRead9Result(
                            rs.getFloat("ratioRepay"),
                            rs.getFloat("ratioDeposit"),
                            rs.getFloat("ratioTransfer"));
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr9);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr10(pid1=?, pid2=?, startTime=?, endTime=?)";
                long startTime = cr10.getStartTime().getTime();
                long endTime = cr10.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr10.getPid1());
                pstmt.setLong(2, cr10.getPid2());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);

                rs = pstmt.executeQuery();
                List<ComplexRead10Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead10Result rowResult = new ComplexRead10Result(
                            rs.getFloat("jaccardSimilarity")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr10);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr11(id=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                long startTime = cr11.getStartTime().getTime();
                long endTime = cr11.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr11.getId());
                pstmt.setLong(2, startTime);
                pstmt.setLong(3, endTime);
                pstmt.setInt(4, cr11.getTruncationLimit());
                pstmt.setString(5, String.valueOf(cr11.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead11Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead11Result rowResult = new ComplexRead11Result(
                            getRoundedDouble(rs, "sumLoanAmount"),
                            rs.getInt("numLoans")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr11);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tcr12(id=?, startTime=?, endTime=?, truncationLimit=?, truncationOrder=?)";
                long startTime = cr12.getStartTime().getTime();
                long endTime = cr12.getEndTime().getTime();
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, cr12.getId());
                pstmt.setLong(2, startTime);
                pstmt.setLong(3, endTime);
                pstmt.setInt(4, cr12.getTruncationLimit());
                pstmt.setString(5, String.valueOf(cr12.getTruncationOrder()));

                rs = pstmt.executeQuery();
                List<ComplexRead12Result> results = new ArrayList<>();
                while (rs.next()) {
                    ComplexRead12Result rowResult = new ComplexRead12Result(
                            rs.getLong("compAccountId"),
                            getRoundedDouble(rs, "sumEdge2Amount")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, cr12);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr1(id=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr1.getId());

                rs = pstmt.executeQuery();
                List<SimpleRead1Result> results = new ArrayList<>();
                while (rs.next()) {
                    SimpleRead1Result rowResult = new SimpleRead1Result(
                            new Date(rs.getLong("createTime")),
                            rs.getBoolean("isBlocked"),
                            rs.getString("accountType"));
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, sr1);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr2(id=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                long startTime = sr2.getStartTime().getTime();
                long endTime = sr2.getEndTime().getTime();
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr2.getId());
                pstmt.setLong(2, startTime);
                pstmt.setLong(3, endTime);

                rs = pstmt.executeQuery();
                List<SimpleRead2Result> results = new ArrayList<>();
                while (rs.next()) {
                    SimpleRead2Result rowResult = new SimpleRead2Result(
                            getRoundedDouble(rs, "sumEdge1Amount"),
                            getRoundedDouble(rs, "maxEdge1Amount"),
                            rs.getLong("numEdge1"),
                            getRoundedDouble(rs, "sumEdge2Amount"),
                            getRoundedDouble(rs, "maxEdge2Amount"),
                            rs.getLong("numEdge2")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, sr2);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr3(dstId=?, threshold=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                long startTime = sr3.getStartTime().getTime();
                long endTime = sr3.getEndTime().getTime();
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr3.getId());
                pstmt.setDouble(2, sr3.getThreshold());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);

                rs = pstmt.executeQuery();
                List<SimpleRead3Result> results = new ArrayList<>();
                while (rs.next()) {
                    SimpleRead3Result rowResult = new SimpleRead3Result(
                            rs.getFloat("blockRatio")
                    );
                    results.add(rowResult);
                }
                resultReporter.report(results.size(), results, sr3);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr4(dstAccountId=?, threshold=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                long startTime = sr4.getStartTime().getTime();
                long endTime = sr4.getEndTime().getTime();
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr4.getId());
                pstmt.setDouble(2, sr4.getThreshold());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);

                rs = pstmt.executeQuery();
                List<SimpleRead4Result> results = new ArrayList<>();
                while (rs.next()) {
                    if (rs.getObject("@@result") == null) {
                        SimpleRead4Result rowResult = new SimpleRead4Result(
                                rs.getLong("dstId"),
                                rs.getInt("numEdges"),
                                getRoundedDouble(rs, "sumAmount")
                        );
                        results.add(rowResult);
                    }
                }
                resultReporter.report(results.size(), results, sr4);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr5(id=?, threshold=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                long startTime = sr5.getStartTime().getTime();
                long endTime = sr5.getEndTime().getTime();
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr5.getId());
                pstmt.setDouble(2, sr5.getThreshold());
                pstmt.setLong(3, startTime);
                pstmt.setLong(4, endTime);

                rs = pstmt.executeQuery();
                List<SimpleRead5Result> results = new ArrayList<>();
                while (rs.next()) {
                    if (rs.getObject("Nodes") == null) {
                        SimpleRead5Result rowResult = new SimpleRead5Result(
                                rs.getLong("srcId"),
                                rs.getInt("numEdges"),
                                getRoundedDouble(rs, "sumAmount")
                        );
                        results.add(rowResult);
                    }
                }
                resultReporter.report(results.size(), results, sr5);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
			PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tsr6(id=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                long startTime = sr6.getStartTime().getTime();
                long endTime = sr6.getEndTime().getTime();
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, sr6.getId());
                pstmt.setLong(2, startTime);
                pstmt.setLong(3, endTime);

                rs = pstmt.executeQuery();
                List<SimpleRead6Result> results = new ArrayList<>();
                while (rs.next()) {
                    if (rs.getObject("Nodes") == null) {
                        SimpleRead6Result rowResult = new SimpleRead6Result(
                                rs.getLong("dstId")
                        );
                        results.add(rowResult);
                    }
                }
                resultReporter.report(results.size(), results, sr6);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, rs);
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw1(personId=?, personName=?, isBlocked=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w1.getPersonId());
                pstmt.setString(2, w1.getPersonName());
                pstmt.setBoolean(3, w1.getIsBlocked());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw2(companyId=?, companyName=?, isBlocked=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w2.getCompanyId());
                pstmt.setString(2, w2.getCompanyName());
                pstmt.setBoolean(3, w2.getIsBlocked());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw3(mediumId=?, mediumType=?, isBlocked=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w3.getMediumId());
                pstmt.setString(2, w3.getMediumType());
                pstmt.setBoolean(3, w3.getIsBlocked());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw4(personId=?, accountId=?, time=?, accountBlocked=?, accountType=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w4.getPersonId());
                pstmt.setLong(2, w4.getAccountId());
                pstmt.setLong(3, w4.getTime().getTime());
                pstmt.setBoolean(4, w4.getAccountBlocked());
                pstmt.setString(5, w4.getAccountType());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw5(companyId=?, accountId=?, time=?, accountBlocked=?, accountType=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w5.getCompanyId());
                pstmt.setLong(2, w5.getAccountId());
                pstmt.setLong(3, w5.getTime().getTime());
                pstmt.setBoolean(4, w5.getAccountBlocked());
                pstmt.setString(5, w5.getAccountType());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw6(personId=?, loanId=?, loanAmount=?, balance=?, time=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w6.getPersonId());
                pstmt.setLong(2, w6.getLoanId());
                pstmt.setDouble(3, w6.getLoanAmount());
                pstmt.setDouble(4, w6.getBalance());
                pstmt.setLong(5, w6.getTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw7(companyId=?, loanId=?, loanAmount=?, balance=?, time=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w7.getCompanyId());
                pstmt.setLong(2, w7.getLoanId());
                pstmt.setDouble(3, w7.getLoanAmount());
                pstmt.setDouble(4, w7.getBalance());
                pstmt.setLong(5, w7.getTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw8(personId=?, companyId=?, time=?, ratio=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w8.getPersonId());
                pstmt.setLong(2, w8.getCompanyId());
                pstmt.setLong(3, w8.getTime().getTime());
                pstmt.setDouble(4, w8.getRatio());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw9(companyId1=?, companyId2=?, time=?, ratio=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w9.getCompanyId1());
                pstmt.setLong(2, w9.getCompanyId2());
                pstmt.setLong(3, w9.getTime().getTime());
                pstmt.setDouble(4, w9.getRatio());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw10(personId1=?, personId2=?, time=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w10.getPersonId1());
                pstmt.setLong(2, w10.getPersonId2());
                pstmt.setLong(3, w10.getTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw11(companyId1=?, companyId2=?, time=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w11.getCompanyId1());
                pstmt.setLong(2, w11.getCompanyId2());
                pstmt.setLong(3, w11.getTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw12(accountId1=?, accountId2=?, time=?, amount=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w12.getAccountId1());
                pstmt.setLong(2, w12.getAccountId2());
                pstmt.setLong(3, w12.getTime().getTime());
                pstmt.setDouble(4, w12.getAmount());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw13(accountId1=?, accountId2=?, time=?, amount=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w13.getAccountId1());
                pstmt.setLong(2, w13.getAccountId2());
                pstmt.setLong(3, w13.getTime().getTime());
                pstmt.setDouble(4, w13.getAmount());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw14(accountId=?, loanId=?, time=?, amount=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w14.getAccountId());
                pstmt.setLong(2, w14.getLoanId());
                pstmt.setLong(3, w14.getTime().getTime());
                pstmt.setDouble(4, w14.getAmount());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw15(loanId=?, accountId=?, time=?, amount=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w15.getLoanId());
                pstmt.setLong(2, w15.getAccountId());
                pstmt.setLong(3, w15.getTime().getTime());
                pstmt.setDouble(4, w15.getAmount());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw16(mediumId=?, accountId=?, time=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w16.getMediumId());
                pstmt.setLong(2, w16.getAccountId());
                pstmt.setLong(3, w16.getTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw17(accountId=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w17.getAccountId());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw18(accountId=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w18.getAccountId());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN tw19(personId=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, w19.getPersonId());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN trw1(srcId=?, dstId=?, time=?, amount=?, startTime=?, endTime=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, rw1.getSrcId());
                pstmt.setLong(2, rw1.getDstId());
                pstmt.setLong(3, rw1.getTime().getTime());
                pstmt.setDouble(4, rw1.getAmount());
                pstmt.setLong(5, rw1.getStartTime().getTime());
                pstmt.setLong(6, rw1.getEndTime().getTime());
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN trw2(srcId=?, dstId=?, time=?, startTime=?, endTime=?, " +
                        "amount=?, amountThreshold=?, ratioThreshold=?, " +
                        "truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, rw2.getSrcId());
                pstmt.setLong(2, rw2.getDstId());
                pstmt.setLong(3, rw2.getTime().getTime());
                pstmt.setLong(4, rw2.getStartTime().getTime());
                pstmt.setLong(5, rw2.getEndTime().getTime());
                pstmt.setDouble(6, rw2.getAmount());
                pstmt.setDouble(7, rw2.getAmountThreshold());
                pstmt.setDouble(8, rw2.getRatioThreshold());
                pstmt.setInt(9, rw2.getTruncationLimit());
                pstmt.setString(10, String.valueOf(rw2.getTruncationOrder()));
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, TigerGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, TigerGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = dbConnectionState.getPooledConn();
                String query = "RUN trw3(srcId=?, dstId=?, time=?, threshold=?, startTime=?, endTime=?, " +
                        "truncationLimit=?, truncationOrder=?)";
                pstmt = conn.prepareStatement(query);
                pstmt.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                pstmt.setLong(1, rw3.getSrcId());
                pstmt.setLong(2, rw3.getDstId());
                pstmt.setLong(3, rw3.getTime().getTime());
                pstmt.setDouble(4, rw3.getThreshold());
                pstmt.setLong(5, rw3.getStartTime().getTime());
                pstmt.setLong(6, rw3.getEndTime().getTime());
                pstmt.setInt(7, rw3.getTruncationLimit());
                pstmt.setString(8, String.valueOf(rw3.getTruncationOrder()));
                pstmt.executeQuery();
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
            } catch (SQLException e) {
                logger.error("Failed to createStatement: " + e);
            } finally {
                closeResources(conn, pstmt, null);
            }
        }
    }
}