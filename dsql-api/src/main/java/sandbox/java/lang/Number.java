package sandbox.java.lang;

import java.io.Serializable;

/**
 * This is a dummy class that implements just enough of {@link java.lang.Number}
 * to allow us to compile {@link sandbox.java.lang.Long}, etc.
 */ 
@SuppressWarnings("serial")
public abstract class Number extends Object implements Serializable {

    public abstract double doubleValue();
    public abstract float floatValue();
    public abstract long longValue();
    public abstract int intValue();
    public abstract short shortValue();
    public abstract byte byteValue();

}