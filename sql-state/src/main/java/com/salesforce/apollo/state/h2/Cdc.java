package com.salesforce.apollo.state.h2;

import org.h2.result.Row;
import org.h2.table.Table;
 
public interface Cdc {

    void log(Table table, short operation, Row row); 

}
 