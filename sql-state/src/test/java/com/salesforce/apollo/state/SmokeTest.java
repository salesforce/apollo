package com.salesforce.apollo.state;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

public class SmokeTest {

    @Test
    public void smokin() throws Exception {
        Config sqlParserConfig = SqlParser.configBuilder()
                                          .setParserFactory(SqlDdlParserImpl.FACTORY)
                                          .setConformance(SqlConformanceEnum.BABEL)
                                          .build();

        SqlParser parser = SqlParser.create("SELECT foo FROM bar where bar.baz = $1\n", sqlParserConfig);

        SqlNode exp = parser.parseQuery();
        System.out.println(exp);
    }
}
