package com.tshprecher.postgres.antlr4;
 

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class DeleteCommandTest extends CommandTest {

    @Test
    public void testDelete() throws IOException {
        super.test("DELETE", "/sql/delete/");
    }

}
