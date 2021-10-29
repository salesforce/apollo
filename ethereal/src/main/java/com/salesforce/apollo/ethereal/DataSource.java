/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.concurrent.BlockingQueue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * @author hal.hildebrand
 *
 */
public interface DataSource {
    class BlockingDataSourceQueue implements DataSource{
        private final BlockingQueue<ByteString> queue;

        public BlockingDataSourceQueue(BlockingQueue<ByteString> queue) {
            this.queue = queue;
        }

        @Override
        public ByteString getData() { 
            try {
                return queue.take();
            } catch (InterruptedException e) {
                return null;
            }
        }
        
        public boolean offer(Message message) {
            return offer(message.toByteString());
        }
        
        public boolean offer(ByteString message) {
            return queue.offer(message );
        }
    }
    ByteString getData();
}
