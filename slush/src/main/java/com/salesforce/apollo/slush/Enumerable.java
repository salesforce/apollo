/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.slush;

import java.util.Iterator;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Enumerable<T> {

      Iterator<Color> enumerate();
      
      T value();
}
