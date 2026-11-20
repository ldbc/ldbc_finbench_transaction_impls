package org.ldbcouncil.finbench.impls.gpstore;

import org.ldbcouncil.finbench.driver.DbException;

final class GpstoreResetSupport {

    private static final Object SNAPSHOT_LOCK = new Object();
    private static boolean snapshotReady = false;

    private final GpstoreDbConnectionState connectionState;

    GpstoreResetSupport(GpstoreDbConnectionState connectionState) {
        this.connectionState = connectionState;
    }

    void resetDatabaseState() throws DbException {
        String dbName = connectionState.getDbName();
        if (dbName == null || dbName.trim().isEmpty()) {
            throw new DbException("GPStore dbname must be configured for database reset");
        }
        String extraFields = ",\"db_name\":\""
                + GpstoreRequestSupport.escapeJson(dbName) + "\"";

        synchronized (SNAPSHOT_LOCK) {
            if (!snapshotReady) {
                GpstoreRequestSupport.send(
                        connectionState, "createResetSnapshot", extraFields);
                snapshotReady = true;
            } else {
                GpstoreRequestSupport.send(
                        connectionState, "restoreResetSnapshot", extraFields);
            }
        }
    }

    static synchronized void resetSnapshotState() {
        snapshotReady = false;
    }
}
