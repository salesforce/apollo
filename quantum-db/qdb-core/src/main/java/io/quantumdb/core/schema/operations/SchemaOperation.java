package io.quantumdb.core.schema.operations;

public interface SchemaOperation extends Operation {

    @Override
    default Type getType() {
        return Type.DDL;
    }

}
