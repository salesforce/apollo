package io.quantumdb.core.schema.operations;

public interface Operation {

    enum Type {
        DDL, DML
    }

    Type getType();

}
