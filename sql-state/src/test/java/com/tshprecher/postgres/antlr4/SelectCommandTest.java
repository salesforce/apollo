package com.tshprecher.postgres.antlr4;
 

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class SelectCommandTest extends CommandTest {

    @Test
    public void testSelect() throws IOException {
        super.test("SELECT", "/sql/select/");
    }

    @Test
    public void testSelectInto() throws IOException {
        super.test("SELECT INTO", "/sql/select_into/");
    }

}
