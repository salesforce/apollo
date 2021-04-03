 
package sandbox.java.lang;

/**
 * This is a dummy class that implements just enough of {@link java.lang.Exception}
 * to allow us to compile {@link sandbox.java.security.PrivilegedActionException}.
 */
@SuppressWarnings("serial")
public class Exception extends Throwable {
    public Exception(String message, Throwable t) {
        super(message, t);
    }

    public Exception(String message) {
        super(message);
    }

    public Exception(Throwable t) {
        super(t);
    }

    public Exception() {
    }
}