package org.ldbcouncil.finbench.impls.gpstore;

import org.ldbcouncil.finbench.driver.DbException;
import org.pkumod.gpstore.Result;
import org.pkumod.gpstore.driver.Session;

final class GpstoreRequestSupport {

    private GpstoreRequestSupport() {
    }

    static Result send(GpstoreDbConnectionState connectionState, String operation,
                       String extraJsonFields) throws DbException {
        try (Session session = connectionState.getSession()) {
            String body = "{\"operation\":\"" + operation + "\",\"username\":\""
                    + escapeJson(connectionState.getUsername()) + "\",\"password\":\""
                    + escapeJson(connectionState.getPassword()) + "\""
                    + extraJsonFields + "}";
            Result result = session.send(body);
            if (result == null || !result.success()) {
                String message = result == null ? "empty response" : result.statusMsg();
                throw new DbException("GPStore operation " + operation + " failed: " + message);
            }
            return result;
        } catch (DbException e) {
            throw e;
        } catch (Exception e) {
            throw new DbException("GPStore operation " + operation + " failed", e);
        }
    }

    static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
