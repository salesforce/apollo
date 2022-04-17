package com.netflix.concurrency.limits.limit.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class Log10RootFunctionTest {
    @Test
    public void test0Index() {
        Function<Integer, Integer> func = Log10RootFunction.create(0);
        assertEquals(1, func.apply(0).intValue());
    }

    @Test
    public void testInRange() {
        Function<Integer, Integer> func = Log10RootFunction.create(0);
        assertEquals(2, func.apply(100).intValue());
    }

    @Test
    public void testOutofLookupRange() {
        Function<Integer, Integer> func = Log10RootFunction.create(0);
        assertEquals(4, func.apply(10000).intValue());
    }
}
