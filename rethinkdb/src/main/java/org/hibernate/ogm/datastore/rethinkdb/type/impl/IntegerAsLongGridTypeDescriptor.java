/*
 * Copyright (C) 2016 Hibernate.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.hibernate.ogm.datastore.rethinkdb.type.impl;

import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.type.descriptor.impl.GridTypeDescriptor;
import org.hibernate.ogm.type.descriptor.impl.GridValueBinder;
import org.hibernate.ogm.type.descriptor.impl.GridValueExtractor;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;

/**
 *
 * @author dani
 */
public class IntegerAsLongGridTypeDescriptor implements GridTypeDescriptor {

    public static final IntegerAsLongGridTypeDescriptor INSTANCE = new IntegerAsLongGridTypeDescriptor();
    
    @Override
    public <X> GridValueBinder<X> getBinder(JavaTypeDescriptor<X> javaTypeDescriptor) {
        return new GridValueBinder<X>() {
            @Override
            public void bind(Tuple resultset, X value, String[] names) {
                for( String name : names) {
                    resultset.put( name, ((Integer)value).longValue());
                }
            }
        };
    }

    @Override
    public <X> GridValueExtractor<X> getExtractor(JavaTypeDescriptor<X> javaTypeDescriptor) {
        return new GridValueExtractor<X>() {
            @Override
            public X extract(Tuple resultset, String name) {
                Object value = resultset.get( name);
                
                if( value == null) {
                    return null;
                }
                
                return (X)(Integer)((Long)value).intValue();
            }
        };
    }
}
