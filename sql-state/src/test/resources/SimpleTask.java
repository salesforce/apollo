import java.util.function.Function;

/**
 * @author hal.hildebrand
 *
 */
public class SimpleTask implements Function<long[], Long> {
    @Override
    public Long apply(long[] input) {
        if (input == null) {
            return null;
        }
        long total = 0;
        for (long number : input) {
            total += number;
        }
        return total;
    }
}
