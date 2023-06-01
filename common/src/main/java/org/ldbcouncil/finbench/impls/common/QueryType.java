package org.ldbcouncil.finbench.impls.common;

/**
 * Enum containing all query file names without extension.
 */

public enum QueryType {
    // transaction complex read queries
    TransactionComplexRead1("transaction-complex-read-1"),
    TransactionComplexRead2("transaction-complex-read-2"),
    TransactionComplexRead3("transaction-complex-read-3"),
    TransactionComplexRead4("transaction-complex-read-4"),
    TransactionComplexRead5("transaction-complex-read-5"),
    TransactionComplexRead6("transaction-complex-read-6"),
    TransactionComplexRead7("transaction-complex-read-7"),
    TransactionComplexRead8("transaction-complex-read-8"),
    TransactionComplexRead9("transaction-complex-read-9"),
    TransactionComplexRead10("transaction-complex-read-10"),
    TransactionComplexRead11("transaction-complex-read-11"),
    TransactionComplexRead12("transaction-complex-read-12"),
    TransactionComplexRead13("transaction-complex-read-13"),

    // transaction simple read queries
    TransactionSimpleRead1("transaction-simple-read-1"),
    TransactionSimpleRead2("transaction-simple-read-2"),
    TransactionSimpleRead3("transaction-simple-read-3"),
    TransactionSimpleRead4("transaction-simple-read-4"),
    TransactionSimpleRead5("transaction-simple-read-5"),
    TransactionSimpleRead6("transaction-simple-read-6"),
    TransactionSimpleRead7("transaction-simple-read-7"),
    TransactionSimpleRead8("transaction-simple-read-8"),

    // transaction write queries
    TransactionWrite1("transaction-write-1"),
    TransactionWrite2("transaction-write-2"),
    TransactionWrite3("transaction-write-3"),
    TransactionWrite4("transaction-write-4"),
    TransactionWrite5("transaction-write-5"),
    TransactionWrite6("transaction-write-6"),
    TransactionWrite7("transaction-write-7"),
    TransactionWrite8("transaction-write-8"),
    TransactionWrite9("transaction-write-9"),
    TransactionWrite10("transaction-write-10"),
    TransactionWrite11("transaction-write-11"),
    TransactionWrite12("transaction-write-12"),
    TransactionWrite13("transaction-write-13"),
    TransactionWrite14("transaction-write-14"),
    TransactionWrite15("transaction-write-15"),
    TransactionWrite16("transaction-write-16"),
    TransactionWrite17("transaction-write-17"),
    TransactionWrite18("transaction-write-18"),
    TransactionWrite19("transaction-write-19"),

    // transaction read write queries
    TransactionReadWrite1("transaction-read-write-1"),
    TransactionReadWrite2("transaction-read-write-2"),
    TransactionReadWrite3("transaction-read-write-3");


    private String name;

    QueryType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
