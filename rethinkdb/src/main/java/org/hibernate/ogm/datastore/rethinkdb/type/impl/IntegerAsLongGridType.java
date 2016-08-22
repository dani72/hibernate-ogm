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

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.impl.AbstractGenericBasicType;

/**
 *
 * @author dani
 */
public class IntegerAsLongGridType extends AbstractGenericBasicType<Integer> {
    public static final IntegerAsLongGridType INSTANCE = new IntegerAsLongGridType();

    public IntegerAsLongGridType() {
            super( IntegerAsLongGridTypeDescriptor.INSTANCE, IntegerAsLongTypeDescriptor.INSTANCE );
    }

    @Override
    public int getColumnSpan(Mapping mapping) throws MappingException {
        return 1;
    }

    @Override
    public String getName() {
        return "integer_long";
    }
}
