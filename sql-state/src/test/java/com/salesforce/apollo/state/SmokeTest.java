package com.salesforce.apollo.state;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import com.google.common.io.CharStreams;

public class SmokeTest {

    @Test
    public void smokin() throws Exception {
        Config sqlParserConfig = SqlParser.configBuilder()
                                          .setParserFactory(SqlDdlParserImpl.FACTORY)
                                          .setConformance(SqlConformanceEnum.BABEL)
                                          .build();

        InputStream is = getClass().getResourceAsStream("/sql/smoke.sql");
        String sql = null;
        try (Reader reader = new InputStreamReader(is)) {
            sql = CharStreams.toString(reader);
        }
        
        SqlParser parser = SqlParser.create(sql, sqlParserConfig);

        SqlNode exp = parser.parseStmtList();
        System.out.println(exp);
    }
}
