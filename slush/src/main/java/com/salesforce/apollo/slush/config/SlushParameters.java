/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.slush.config;

/**
 * The parameters of the Slush protocol
 */
public class SlushParameters extends Parameters {
    /**
     * The number of rounds - this determines the diffusion of the queries across the member network
     */
    public int rounds; 
}