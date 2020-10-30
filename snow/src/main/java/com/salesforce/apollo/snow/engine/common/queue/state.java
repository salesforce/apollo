/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common.queue;

import java.util.Collection;

import com.salesforce.apollo.snow.database.Database;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class state {
    private Jobs jobs;

    public void setInt(Database database, byte[] key, int size) {
        
    }
    
    public void addID(Database db, byte[] prefix , ID key  )   {
        if (key.IsZero()) {
            throw new IllegalArgumentException("Must be non zero");
        }
        pdb = prefixdb.NewNested(prefix, db)
         pdb.Put(key.Bytes(), nil);
    }

    public Collection<ID> ids(Database db, byte[] array) {
        // TODO Auto-generated method stub
        return null;
    }

    public void removeID(Database db, byte[] array, ID blocked) {
        // TODO Auto-generated method stub
        
    }

    public void delete(byte[] array) {
        // TODO Auto-generated method stub
        
    }

    public Job job(Database db, byte[] array) {
        // TODO Auto-generated method stub
        return null;
    }

    public void setJob(Database db, byte[] array, Job job) {
        // TODO Auto-generated method stub
        
    }

    public int getInt(Database db, byte[] stacksize) {
        // TODO Auto-generated method stub
        return 0;
    }
}
