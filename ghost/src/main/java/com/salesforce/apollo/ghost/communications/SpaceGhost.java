/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.ghost.proto.Bind;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostChat;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesfoce.apollo.ghost.proto.Lookup;
import com.salesfoce.apollo.utils.proto.CausalMessage;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost extends Link {
    static SpaceGhost localLoopbackFor(Member member, GhostService service) {
        return new SpaceGhost() {

            @Override
            public ListenableFuture<Empty> bind(Bind binding) {
                service.bind(binding, member.getId());
                SettableFuture<Empty> f = SettableFuture.create();
                f.set(Empty.getDefaultInstance());
                return f;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Content> get(Get cid) {
                SettableFuture<Content> f = SettableFuture.create();
                f.set(service.get(cid, member.getId()));
                return f;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<CausalMessage> ghosting(GhostChat chatter) {
                var response = service.ghosting(chatter, member.getId());
                SettableFuture<CausalMessage> f = SettableFuture.create();
                f.set(response);
                return f;
            }

            @Override
            public ListenableFuture<Entries> intervals(Intervals intervals) {
                SettableFuture<Entries> f = SettableFuture.create();
                f.set(Entries.getDefaultInstance());
                return f;
            }

            @Override
            public ListenableFuture<Binding> lookup(Lookup query) {
                Binding value = service.lookup(query, member.getId());
                SettableFuture<Binding> f = SettableFuture.create();
                f.set(value);
                return f;
            }

            @Override
            public ListenableFuture<Empty> purge(Get cid) {
                service.purge(cid, member.getId());
                SettableFuture<Empty> f = SettableFuture.create();
                f.set(Empty.getDefaultInstance());
                return f;
            }

            @Override
            public ListenableFuture<Empty> put(Entry content) {
                service.put(content, member.getId());
                SettableFuture<Empty> f = SettableFuture.create();
                f.set(Empty.getDefaultInstance());
                return f;
            }

            @Override
            public ListenableFuture<Empty> remove(Lookup query) {
                service.remove(query, member.getId());
                SettableFuture<Empty> f = SettableFuture.create();
                f.set(Empty.getDefaultInstance());
                return f;
            }
        };
    }

    ListenableFuture<Empty> bind(Bind binding);

    ListenableFuture<Content> get(Get cid);

    ListenableFuture<CausalMessage> ghosting(GhostChat chatter);

    ListenableFuture<Entries> intervals(Intervals intervals);

    ListenableFuture<Binding> lookup(Lookup query);

    ListenableFuture<Empty> purge(Get cid);

    ListenableFuture<Empty> put(Entry content);

    ListenableFuture<Empty> remove(Lookup query);
}
