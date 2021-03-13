package io.quantumdb.cli.utils;

public class CliException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CliException(String message) {
        super(message);
    }

    public CliException(String message, Throwable e) {
        super(message, e);
    }

}
