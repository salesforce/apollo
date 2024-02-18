package com.salesforce.apollo.archipelago.server;

import com.macasaet.fernet.Token;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface FernetParser {

    CompletableFuture<Token> parseToValid(String token);
}
