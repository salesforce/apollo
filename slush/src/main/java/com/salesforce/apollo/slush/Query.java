/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.slush;

import java.util.function.Supplier;

/**
 * A simple holder Pojo because containment typing
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Query<T> implements Supplier<T> {
    public T value;

    public Query() {}

    public Query(T value) {
        this.value = value;
    }

    @Override
    public T get() {
        return value;
    }
}
