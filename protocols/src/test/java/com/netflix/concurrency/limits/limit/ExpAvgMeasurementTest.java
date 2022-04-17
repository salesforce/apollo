package com.netflix.concurrency.limits.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;

public class ExpAvgMeasurementTest {
    @Test
    public void testWarmup() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);

        double expected[] = new double[] { 10.0, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 14.5 };
        for (int i = 0; i < 10; i++) {
            @SuppressWarnings("unused")
            double value = avg.add(i + 10).doubleValue();
            assertEquals(expected[i], avg.get().doubleValue(), 0.01);
        }

        avg.add(100);
        assertEquals(16.2, avg.get().doubleValue(), 0.1);
    }
}
