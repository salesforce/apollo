package com.salesforce.apollo.archipelago.client;

import com.macasaet.fernet.Token;

@FunctionalInterface
public interface BlockingTokenProvider {
    Token get();
}
