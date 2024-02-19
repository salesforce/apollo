package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.cryptography.Digest;
import io.grpc.Context;
import io.grpc.Metadata;

public final class Constants {
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_AGENT_ID          = "agent.id";
    public static final Metadata.Key<String> METADATA_AGENT_KEY                                  = Metadata.Key.of(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_AGENT_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_AGENT_ID_SERVER   = "agent.id.server";
    public static final Context.Key<Digest>  SERVER_AGENT_ID_KEY                                 = Context.key(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_AGENT_ID_SERVER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_CONTEXT_ID        = "context.id";
    public static final Metadata.Key<String> METADATA_CONTEXT_KEY                                = Metadata.Key.of(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_CONTEXT_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_CONTEXT_ID_SERVER = "context.id.server";
    public static final Context.Key<Digest>  SERVER_CONTEXT_KEY                                  = Context.key(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_CONTEXT_ID_SERVER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID           = "from.id";
    public static final Metadata.Key<String> METADATA_CLIENT_ID_KEY                              = Metadata.Key.of(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID_CLIENT    = "from.id.client";
    public static final Context.Key<Digest>  CLIENT_CLIENT_ID_KEY                                = Context.key(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID_CLIENT);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID_SERVER    = "from.id.server";
    public static final Context.Key<Digest>  SERVER_CLIENT_ID_KEY                                = Context.key(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_FROM_ID_SERVER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_TO_ID             = "to.id";
    public static final Metadata.Key<String> METADATA_TARGET_KEY                                 = Metadata.Key.of(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_TO_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final String               COM_SALESFORCE_APOLLO_ARCHIPELAGO_TO_ID_SERVER      = "to.id.server";
    public static final Context.Key<Digest>  SERVER_TARGET_KEY                                   = Context.key(
    COM_SALESFORCE_APOLLO_ARCHIPELAGO_TO_ID_SERVER);
    public static       Metadata.Key<String> AuthorizationMetadataKey                            = Metadata.Key.of(
    "Authorization", Metadata.ASCII_STRING_MARSHALLER);

    private Constants() {
    }
}
