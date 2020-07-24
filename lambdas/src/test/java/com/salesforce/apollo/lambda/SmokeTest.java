package com.salesforce.apollo.lambda;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;
import org.junit.jupiter.api.Test;

public class SmokeTest {
    @Test
    public void test() throws IOException {
        Context.Builder contextBuilder = Context.newBuilder("wasm");
        byte[] binary;
        Source.Builder sourceBuilder = Source.newBuilder("wasm", ByteSequence.create(binary), "main");
        Source source = sourceBuilder.build();
        Context context = contextBuilder.build();
        context.eval(source);
        Value mainFunction = context.getBindings("wasm").getMember("main");
        Value result = mainFunction.execute();
        assertEquals(42, result.asInt());
    }
}
