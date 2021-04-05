
package sandbox.java.util;

import org.jetbrains.annotations.NotNull;

import sandbox.java.lang.Comparable;

/**
 * This is a dummy class that implements just enough of {@link java.util.Date}
 */
@SuppressWarnings("serial")
public class Date extends sandbox.java.lang.Object implements java.io.Serializable, Cloneable, Comparable<Date> {
    private static final String NOT_IMPLEMENTED = "Dummy class - not implemented";
 

    public Date(int year, int month, int date) {
        super(); 
    }

    public Date(long time) { 
    }
 
    public Date(int year, int month, int date, int hour, int minute, int second) {
        super();
    }

    @Override
    public int compareTo(@NotNull Date o) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    public int getDate() {
        return 0;
    }

    public int getMonth() {
        return 0;
    }

    public long getTime() {
        return 0;
    }

    public int getYear() {
        return 0;
    }

    public void setTime(long date) {
    }

    @Override
    @NotNull
    protected final java.util.Date fromDJVM() {
        return new java.util.Date(getTime());
    }

    public int getHours() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMinutes() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getSeconds() {
        // TODO Auto-generated method stub
        return 0;
    }
}
