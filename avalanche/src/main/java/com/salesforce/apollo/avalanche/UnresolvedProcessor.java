/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.avalanche;

/**
 * @author hhildebrand
 */
public class UnresolvedProcessor extends Exception {

    private static final long serialVersionUID = 1L;

    public UnresolvedProcessor(String processor, Throwable e) {
        super("Unresolved entry processor class: " + processor, e);
    }

}
