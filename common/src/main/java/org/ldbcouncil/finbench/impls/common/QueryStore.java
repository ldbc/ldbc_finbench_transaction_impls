package org.ldbcouncil.finbench.impls.common;
/**
 * QueryStore.java
 * 
 * This class stores functions to query definition files and to retrieve the following used in operation handlers:
 * - Query definition strings
 * - Parameter map (Map<String, Object>), with as default String objects as values
 * - Prepared queries (using string substitution)
 * 
 * Implementations can extend this class and override functions to change e.g.
 * - ParameterPrefix ()
 * - ParameterPostfix (file extension of query files)
 * - Parameter map with different type stored than a string.
 */
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.finbench.impls.common.converter.Converter;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class QueryStore {

    /**
     * Converter used for converting result objects. 
     * @return
     */
    protected Converter getConverter() { return new Converter(); }

    /**
     * Parameter prefix used in query definitions. Defaults to '$'
     * @return
     */
    protected String getParameterPrefix() { return "$"; }

    /**
     * The file extension for query files. 
     * @return
     */
    protected String getParameterPostfix() { return ""; }

    /**
     * Stores the loaded queries.
     */
    protected Map<QueryType, String> queries = new HashMap<>();

    /**
     * Load a query file
     * @param path Path to the file
     * @param filename The name of the query file
     * @return Loaded query definition
     * @throws DbException
     */
    protected String loadQueryFromFile(String path, String filename) throws DbException {
        final String filePath = path + File.separator + filename;
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            System.err.printf("Unable to load query from file: %s", filePath);
            return null;
        }
    }

    /**
     * Prepares the parameterized query using the parameter map
     * @param queryType Type of query to prepare (QueryType)
     * @param parameterSubstitutions The parameter map containing the subsitute values
     * @return Prepared query string
     */
    protected String prepare(QueryType queryType, Map<String, Object> parameterSubstitutions) {
        String querySpecification = queries.get(queryType);
        for (String parameter : parameterSubstitutions.keySet()) {
            querySpecification = querySpecification.replace(
                    getParameterPrefix() + parameter + getParameterPostfix(),
                    (String) parameterSubstitutions.get(parameter)
            );
        }
        return querySpecification;
    }

    /**
     * 
     * @param queryType
     * @return
     */
    public String getParameterizedQuery(QueryType queryType) {
        return queries.get(queryType);
    }

    /**
     * Create QueryStore
     * @param path: Path to the directory containing query files
     * @param postfix The extension of the query files
     * @throws DbException
     */
    public QueryStore(String path, String postfix) throws DbException {
        for (QueryType queryType : QueryType.values()) {
            queries.put(queryType, loadQueryFromFile(path, queryType.getName() + postfix));
        }
    }

//    /**
//     * Get prepared Query1 string
//     * @param operation LdbcQuery1 operation containing parameter values
//     * @return Prepared Query1 string
//     */
//    public String getQuery1(LdbcQuery1 operation) {
//        return prepare(QueryType.InteractiveComplexQuery1, getQuery1Map(operation));
//    }
//
//    /**
//     * Get Query1 Map. This map contain the name of the parameter and the value as string.
//     * @param operation LdbcQuery1 operation containing parameter values
//     * @return Map with parameters and values as string.
//     */
//    public Map<String, Object> getQuery1Map(LdbcQuery1 operation) {
//        return new ImmutableMap.Builder<String, Object>()
//                .put(LdbcQuery1.PERSON_ID, getConverter().convertId(operation.getPersonIdQ1()))
//                .put(LdbcQuery1.FIRST_NAME, getConverter().convertString(operation.getFirstName()))
//                .build();
//    }
}
