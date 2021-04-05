package sandbox.java.lang;

import java.io.IOException;

/**
 * This is a dummy class that implements just enough of
 * {@link java.lang.Appendable}
 */
public interface Appendable {

    Appendable append(CharSequence csq, int start, int end) throws IOException;

    Appendable append(CharSequence csq) throws IOException;

    Appendable append(char c) throws IOException;

}
