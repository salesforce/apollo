package com.tshprecher.postgres.antlr4;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class InsertCommandTest extends CommandTest {

    @Test
    public void testInsert() throws IOException {
        super.test("INSERT", "/sql/insert/");
    }

}
