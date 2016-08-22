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

import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;

/**
 *
 * @author dani
 */
public class IntegerAsLongTypeDescriptor extends AbstractTypeDescriptor<Integer> {
    public static final IntegerAsLongTypeDescriptor INSTANCE = new IntegerAsLongTypeDescriptor();

    public IntegerAsLongTypeDescriptor() {
        super( Integer.class );
    }

    @Override
    public String toString(Integer value) {
        return (value != null) ? Integer.toString( value) : "";
    }

    @Override
    public Integer fromString(String string) {
        return Integer.parseInt( string);
    }

    @Override
    public <X> X unwrap(Integer value, Class<X> type, WrapperOptions options) {
        return (X) ((value != null) ? new Long( value.longValue()) : null);
    }

    @Override
    public <X> Integer wrap( X value, WrapperOptions options) {
        Long v = (Long)value;
        
        return (v != null) ? new Integer( v.intValue()) : null;
    }
}
