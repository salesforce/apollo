/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.lang.reflect.Method;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.Test;

import com.salesforce.apollo.delphinius.g4.DatalogLexer;
import com.salesforce.apollo.delphinius.g4.DatalogParser;

/**
 * @author hal.hildebrand
 *
 */
public class SmokeTest {

    @Test
    public void smokin() throws Exception {
        String source_code = """
        % inserting facts
        parent(\"bill\", \"mary\").
        parent(\"mary\", \"john\").
        
        % Horn clauses
        ancestor(?x, ?y) :- parent(?x, ?y).
        ancestor(?x, ?y) :- parent(?x, ?z), ancestor(?z, ?y).
        
        % Query
        ?- ancestor("bill", ?x).
        """;

        CodePointCharStream stream_from_string = CharStreams.fromString(source_code);
        DatalogLexer lexer = new DatalogLexer(stream_from_string);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        DatalogParser parser = new DatalogParser(tokens);

        String startRuleName = "program";
        Method startRule = DatalogParser.class.getMethod(startRuleName);
        ParserRuleContext tree = (ParserRuleContext) startRule.invoke(parser, (Object[]) null);
        System.out.println(tree.toStringTree(parser));
    }
}
