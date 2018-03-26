package com.ericsson.eea.ark.sli.grouping.db;

import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection base use to maintain SQL basic information.
 *
 * <p>This class is extended by the SQLConnection Class which supports the SQL API interface.
 */
public class SQLConnectionBase {

   @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SQLConnectionBase.class);

    String    host;
    String    port;
    String    database;
    String    info;
    Statement sql;

    public SQLConnectionBase(String h, String p, String db) {
        host     = h;
        port     = p;
        database = db;
        info     = "";
    }

    public SQLConnectionBase(String h, String p, String db, String inf) {
        host     = h;
        port     = p;
        database = db;
        info     = inf;
    }

    public void SetStatement(Statement sql_i){

       sql = sql_i;
    }
}
