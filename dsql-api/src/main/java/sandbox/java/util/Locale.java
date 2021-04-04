package sandbox.java.util;

/**
 * This is a dummy class that implements just enough of {@link java.util.Locale}
 * to allow us to compile {@link sandbox.java.lang.String} and {@link sandbox.java.lang.DJVM}.
 */
public final class Locale extends sandbox.java.lang.Object {
    private static final String NOT_IMPLEMENTED = "Dummy class - not implemented";

    public sandbox.java.lang.String toLanguageTag() {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    public static Locale getDefault() {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }
}