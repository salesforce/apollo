/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import liquibase.resource.AbstractResourceAccessor;
import liquibase.resource.InputStreamList;

/**
 * @author hal.hildebrand
 *
 */
public class NullResourceAccessor extends AbstractResourceAccessor implements AutoCloseable {

    @Override
    public void close() {
    }

    @Override
    public SortedSet<String> describeLocations() {
        return new TreeSet<String>();
    }

    @Override
    public SortedSet<String> list(String relativeTo, String path, boolean recursive, boolean includeFiles,
                                  boolean includeDirectories) throws IOException {
        return new TreeSet<String>();
    }

    @Override
    public InputStreamList openStreams(String relativeTo, String streamPath) throws IOException {
        return new InputStreamList();
    }
}
