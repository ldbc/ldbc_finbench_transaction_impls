package org.ldbcouncil.finbench.impls.common;

/**
 * Enum containing all query file names without extension.
 */

public enum QueryType {
    // transaction complex read queries
    TransactionComplexRead1("transaction-complex-read-1");


    private String name;

    QueryType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
