package io.quantumdb.core.utiils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.quantumdb.core.utils.QueryBuilder;

public class QueryBuilderTest {

    @Test
    public void testAppendingMultipleEmptyStringsStillResultsInEmptyString() {
        QueryBuilder builder = new QueryBuilder();
        builder.append("");
        builder.append("");
        builder.append("");
        assertEquals("", builder.toString());
    }

    @Test
    public void testAppendingNonTrimmedStringsDoNotAddWhitespace() {
        QueryBuilder builder = new QueryBuilder();
        builder.append("SELECT ");
        builder.append(" * ");
        builder.append(" FROM users");
        assertEquals("SELECT * FROM users", builder.toString());
    }

    @Test
    public void testAppendingTrimmedStringsAddsWhitespace() {
        QueryBuilder builder = new QueryBuilder();
        builder.append("SELECT");
        builder.append("*");
        builder.append("FROM users");
        assertEquals("SELECT * FROM users", builder.toString());
    }

    @Test
    public void testConstructorWithBeginningOfQuery() {
        QueryBuilder builder = new QueryBuilder("SELECT");
        assertEquals("SELECT", builder.toString());
    }

    @Test
    public void testConstructorWithEmptyInputIsAccepted() {
        QueryBuilder builder = new QueryBuilder("");
        assertEquals("", builder.toString());
    }

    @Test
    public void testConstructorWithNoInputs() {
        QueryBuilder builder = new QueryBuilder();
        assertEquals("", builder.toString());
    }

    @Test
    public void testConstructorWithNullInputThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new QueryBuilder(null));
    }

    @Test
    public void testNullCannotBeAppended() {
        assertThrows(IllegalArgumentException.class, () -> {
            QueryBuilder builder = new QueryBuilder();
            builder.append(null);
        });
    }

}
