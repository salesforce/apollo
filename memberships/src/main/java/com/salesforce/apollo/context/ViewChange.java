package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;

import java.util.Collection;

public record ViewChange(Context context, Digest diadem, Collection<Digest> joining, Collection<Digest> leaving) {
}
