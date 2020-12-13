package com.salesforce.apollo.state.h2;

import org.h2.engine.Session.CDC;
import org.h2.result.Row;
import org.h2.table.Table;
 
@FunctionalInterface
public interface Cdc {

    void cdc(Table table, Row prev, CDC operation, Row row); 

}
 