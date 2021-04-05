package sandbox.java.time;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
 

/**
 * This is a dummy class that implements just enough of {@link java.time.Instant}
 * to allow us to compile {@link sandbox.java.lang.DJVM}.
 */ 
@SuppressWarnings("serial")
public final class Instant extends sandbox.java.lang.Object implements Serializable {
    private final long seconds;
    private final int nanos;

    private Instant(long epochSecond, int nano) {
        this.seconds = epochSecond;
        this.nanos = nano;
    }

    public long getEpochSecond() {
        return seconds;
    }

    public int getNano() {
        return nanos;
    }

    @Override
    @NotNull
    protected java.time.Instant fromDJVM() {
        return java.time.Instant.ofEpochSecond(seconds, nanos);
    }

    public static Instant ofEpochSecond(long epochSecond, long nanoAdjustment) {
        return null;
    }
}