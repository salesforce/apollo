/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.sql.ShardingKey;

/**
 * @author hal.hildebrand
 *
 */
public class ShardingKeyWrapper implements ShardingKey {
    private final java.sql.ShardingKey wrapped;

    public ShardingKeyWrapper(java.sql.ShardingKey wrapped) {
        this.wrapped = wrapped;
    }

    public java.sql.ShardingKey toJsShardingKey() {
        return wrapped;
    }
}
