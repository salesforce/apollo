package com.tshprecher.postgres.antlr4;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class UpdateCommandTest extends CommandTest {

    @Test
    public void testUpdate() throws IOException {
        super.test("UPDATE", "/sql/update/");
    }

}
