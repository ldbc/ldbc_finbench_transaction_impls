package org.ldbcouncil.finbench.impls.gpstore;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.optimization.OptimizationSupport;
import org.ldbcouncil.finbench.driver.optimization.SyntheticExecutionResult;
import org.ldbcouncil.finbench.driver.optimization.SyntheticLoadSpec;
import org.pkumod.gpstore.Result;
import org.pkumod.gpstore.driver.Session;

/**
 * Optimization support for the GPStore SUT.
 *
 * <p>GPStore exposes the three optimization APIs through its gRPC endpoint
 * (GRPC/grpc_operation.h): getCurrentCpuCores, getCurrentMemoryBytes and
 * syntheticLoad. The SDK's {@code GpstoreOperation} enum does not cover these
 * operations yet, so the requests are sent as raw JSON through
 * {@link Session#send(String)}; the server dispatches on the "operation"
 * field of the request body and requires username/password in every request.
 */
public class GpstoreOptimizationSupport implements OptimizationSupport {

    private final GpstoreDbConnectionState connectionState;

    public GpstoreOptimizationSupport(GpstoreDbConnectionState connectionState) {
        this.connectionState = connectionState;
    }

    @Override
    public double getCurrentCpuCores() throws DbException {
        Result result = send("getCurrentCpuCores", "");
        return result.getJsonNode().path("cpu_cores").asDouble();
    }

    @Override
    public long getCurrentMemoryBytes() throws DbException {
        Result result = send("getCurrentMemoryBytes", "");
        return result.getJsonNode().path("memory_bytes").asLong();
    }

    @Override
    public SyntheticExecutionResult executeSyntheticLoad(SyntheticLoadSpec spec) throws DbException {
        String extraFields = ",\"operationName\":\"" + escapeJson(spec.operationName()) + "\""
                + ",\"targetCpuCores\":" + spec.targetCpuCores()
                + ",\"targetMemoryBytes\":" + spec.targetMemoryBytes()
                + ",\"targetDurationNanos\":" + spec.targetDurationNanos();
        Result result = send("syntheticLoad", extraFields);
        return new SyntheticExecutionResult(
                result.getJsonNode().path("actual_duration_nanos").asLong(),
                result.getJsonNode().path("peak_memory_bytes").asLong());
    }

    /**
     * Sends a JSON request to the GPStore server through the shared driver.
     */
    private Result send(String operation, String extraJsonFields) throws DbException {
        try (final Session session = connectionState.getSession()) {
            String body = "{\"operation\":\"" + operation + "\",\"username\":\""
                    + connectionState.getUsername() + "\",\"password\":\""
                    + connectionState.getPassword() + "\"" + extraJsonFields + "}";
            Result result = session.send(body);
            if (result == null || !result.success()) {
                String message = result == null ? "empty response" : result.statusMsg();
                throw new DbException("Optimization operation " + operation + " failed: " + message);
            }
            return result;
        } catch (DbException e) {
            throw e;
        } catch (Exception e) {
            throw new DbException("Optimization operation " + operation + " failed", e);
        }
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
