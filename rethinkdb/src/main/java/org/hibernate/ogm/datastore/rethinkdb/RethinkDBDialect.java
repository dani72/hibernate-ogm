/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb;

import static org.hibernate.ogm.datastore.document.impl.DotPatternMapHelpers.getColumnSharedPrefixOfAssociatedEntityLink;
import static org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkDBTupleSnapshot.SnapshotType.INSERT;
import static org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkDBTupleSnapshot.SnapshotType.UPDATE;
import static org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkHelpers.hasField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;
import org.hibernate.HibernateException;
import org.hibernate.annotations.common.AssertionFailure;
import org.hibernate.ogm.datastore.document.association.impl.DocumentHelpers;
import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;
import org.hibernate.ogm.datastore.document.impl.DotPatternMapHelpers;
import org.hibernate.ogm.datastore.document.impl.EmbeddableStateFinder;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.document.options.MapStorageType;
import org.hibernate.ogm.datastore.document.options.spi.AssociationStorageOption;
import org.hibernate.ogm.datastore.rethinkdb.configuration.impl.RethinkDBConfiguration;
import org.hibernate.ogm.datastore.rethinkdb.dialect.impl.AssociationStorageStrategy;
import org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkDBAssociationSnapshot;
import org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkDBTupleSnapshot;
import org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkDBTupleSnapshot.SnapshotType;
import org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkHelpers;
import org.hibernate.ogm.datastore.rethinkdb.impl.RethinkDBDatastoreProvider;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.Log;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType;
import org.hibernate.ogm.datastore.rethinkdb.options.impl.AssociationDocumentStorageOption;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.nativequery.impl.RethinkDBQueryDescriptorBuilder;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.nativequery.impl.NativeQueryParser;
import org.hibernate.ogm.dialect.batch.spi.BatchableGridDialect;
import org.hibernate.ogm.dialect.batch.spi.InsertOrUpdateAssociationOperation;
import org.hibernate.ogm.dialect.batch.spi.InsertOrUpdateTupleOperation;
import org.hibernate.ogm.dialect.batch.spi.Operation;
import org.hibernate.ogm.dialect.batch.spi.OperationsQueue;
import org.hibernate.ogm.dialect.batch.spi.RemoveAssociationOperation;
import org.hibernate.ogm.dialect.batch.spi.RemoveTupleOperation;
import org.hibernate.ogm.dialect.identity.spi.IdentityColumnAwareGridDialect;
import org.hibernate.ogm.dialect.multiget.spi.MultigetGridDialect;
import org.hibernate.ogm.dialect.optimisticlock.spi.OptimisticLockingAwareGridDialect;
import org.hibernate.ogm.dialect.query.spi.BackendQuery;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.query.spi.NoOpParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.QueryParameters;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.dialect.spi.AssociationTypeContext;
import org.hibernate.ogm.dialect.spi.BaseGridDialect;
import org.hibernate.ogm.dialect.spi.DuplicateInsertPreventionStrategy;
import org.hibernate.ogm.dialect.spi.ModelConsumer;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.AssociationType;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKey;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.model.spi.TupleOperation;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;
import org.parboiled.Parboiled;
import org.parboiled.errors.ErrorUtils;
import org.parboiled.parserunners.RecoveringParseRunner;
import org.parboiled.support.ParsingResult;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;
import org.hibernate.ogm.datastore.rethinkdb.type.impl.IntegerAsLongGridType;

/**
 * Each Tuple entry is stored as a property in a MongoDB document.
 *
 * Each association is stored in an association document containing three
 * properties: - the association table name (optionally) - the RowKey column
 * names and values - the tuples as an array of elements
 *
 * Associations can be stored as: - one MongoDB collection per association
 * class. The collection name is prefixed. - one MongoDB collection for all
 * associations (the association table name property in then used) - embed the
 * collection info in the owning entity document is planned but not supported at
 * the moment (OGM-177)
 *
 * Collection of embeddable are stored within the owning entity document under
 * the unqualified collection role
 *
 * In MongoDB is possible to batch operations but only for the creation of new
 * documents and only if they don't have invalid characters in the field name.
 * If these conditions are not met, the MongoDB mechanism for batch operations
 * is not going to be used.
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Alan Fitton &lt;alan at eth0.org.uk&gt;
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 * @author Thorsten Möller &lt;thorsten.moeller@sbi.ch&gt;
 */
public class RethinkDBDialect extends BaseGridDialect implements QueryableGridDialect<RethinkDBQueryDescriptor>, BatchableGridDialect, IdentityColumnAwareGridDialect, MultigetGridDialect, OptimisticLockingAwareGridDialect {

    public static final String ID_FIELDNAME = "id";
    public static final String PROPERTY_SEPARATOR = ".";
    public static final String ROWS_FIELDNAME = "rows";
    public static final String TABLE_FIELDNAME = "table";
    public static final String ASSOCIATIONS_COLLECTION_PREFIX = "associations_";

    private static final Log log = LoggerFactory.getLogger();

    private static final List<String> ROWS_FIELDNAME_LIST = Collections.singletonList(ROWS_FIELDNAME);

    /**
     * Pattern used to recognize a constraint violation on the primary key.
     *
     * MongoDB returns an exception with {@code .$_id_ } or {@code  _id_ } while
     * Fongo returns an exception with {@code ._id }
     */
    private static final Pattern PRIMARY_KEY_CONSTRAINT_VIOLATION_MESSAGE = Pattern.compile(".*[. ]\\$?_id_? .*");

    private final RethinkDBDatastoreProvider provider;
    private final Db currentDB;

    public RethinkDBDialect(RethinkDBDatastoreProvider provider) {
        this.provider = provider;
        this.currentDB = this.provider.getDatabase();
    }

    @Override
    public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
        Map<String, Object> found = this.getObject(key, tupleContext);
        return createTuple(key, tupleContext, found);
    }

    @Override
    public List<Tuple> getTuples(EntityKey[] keys, TupleContext tupleContext) {
        if (keys.length == 0) {
            return Collections.emptyList();
        }

        Object[] searchObjects = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            searchObjects[i] = prepareIdObjectValue(keys[i].getColumnNames(), keys[i].getColumnValues());
        }

        Cursor cursor = this.getObjects(keys[0].getMetadata(), searchObjects, tupleContext);
        try {
            return tuplesResult(keys, searchObjects, tupleContext, cursor);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /*
	 * This method assumes that the entries in the cursor might not be in the same order as the keys and some keys might
	 * not have a matching result in the db.
     */
    private static List<Tuple> tuplesResult(EntityKey[] keys, Object[] searchObjects, TupleContext tupleContext, Cursor cursor) {
        // The list is initialized with null because some keys might not have a corresponding value in the cursor
        Tuple[] tuples = new Tuple[searchObjects.length];
        for (Object object : cursor) {
            MapObject dbObject = (MapObject) object;
            for (int i = 0; i < searchObjects.length; i++) {
                if (dbObject.get(ID_FIELDNAME).equals(searchObjects[i])) {
                    tuples[i] = createTuple(keys[i], tupleContext, dbObject);
                    // We assume there are no duplicated keys
                    break;
                }
            }
        }
        return Arrays.asList(tuples);
    }

    private static Tuple createTuple(EntityKey key, TupleContext tupleContext, Map<String, Object> found) {
        if (found != null) {
            return new Tuple(new RethinkDBTupleSnapshot(found, key.getMetadata(), UPDATE));
        } else if (isInTheQueue(key, tupleContext)) {
            // The key has not been inserted in the db but it is in the queue
            return new Tuple(new RethinkDBTupleSnapshot(prepareIdObject(key), key.getMetadata(), INSERT));
        } else {
            return null;
        }
    }

    private static boolean isInTheQueue(EntityKey key, TupleContext tupleContext) {
        OperationsQueue queue = tupleContext.getOperationsQueue();
        return queue != null && queue.contains(key);
    }

    @Override
    public Tuple createTuple(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
        return new Tuple(new RethinkDBTupleSnapshot(com.rethinkdb.RethinkDB.r.hashMap(), entityKeyMetadata, SnapshotType.INSERT));
    }

    @Override
    public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
        MapObject toSave = prepareIdObject(key);
        return new Tuple(new RethinkDBTupleSnapshot(toSave, key.getMetadata(), SnapshotType.INSERT));
    }

    /**
     * Returns a {@link DBObject} representing the entity which embeds the
     * specified association.
     */
    private Map<String, Object> getEmbeddingEntity(AssociationKey key, AssociationContext associationContext) {
        Map<String, Object> embeddingEntityDocument = associationContext.getEntityTuple() != null ? ((RethinkDBTupleSnapshot) associationContext.getEntityTuple().getSnapshot()).getDbObject() : null;

        if (embeddingEntityDocument != null) {
            return embeddingEntityDocument;
        } else {
            Table collection = getCollection(key.getEntityKey());
            MapObject searchObject = prepareIdObject(key.getEntityKey());
            String[] projection = getProjection(key, true);

            return collection.get( searchObject)/*.pluck( projection)*/.run( this.provider.getConnection());
        }
    }

    private Map<String, Object> getObject(EntityKey key, TupleContext tupleContext) {
        Table collection = getCollection(key);
        Object id = prepareIdObjectValue(key.getColumnNames(), key.getColumnValues());
        String[] projection = getProjection(tupleContext);

        return collection.get( id).run( this.provider.getConnection());
    }

    private Cursor getObjects(EntityKeyMetadata entityKeyMetadata, Object[] searchObjects, TupleContext tupleContext) {
        Table collection = getCollection(entityKeyMetadata);
        String[] projection = getProjection(tupleContext);
        MapObject query = com.rethinkdb.RethinkDB.r.hashMap();

        query.put(ID_FIELDNAME, com.rethinkdb.RethinkDB.r.hashMap("$in", searchObjects));
        
        return collection.getAll( query)/*.pluck( projection)*/.run( this.provider.getConnection());
    }

    private static String[] getProjection(TupleContext tupleContext) {
        return getProjection(tupleContext.getSelectableColumns());
    }

    /**
     * Returns a projection object for specifying the fields to retrieve during
     * a specific find operation.
     */
    private static String[] getProjection(List<String> fieldNames) {
        return fieldNames.toArray( new String[ fieldNames.size()]);
    }

    /**
     * Create a DBObject which represents the _id field. In case of simple id
     * objects the json representation will look like {_id: "theIdValue"} In
     * case of composite id objects the json representation will look like {_id:
     * {author: "Guillaume", title: "What this method is used for?"}}
     *
     * @param key
     *
     * @return the DBObject which represents the id field
     */
    private static MapObject prepareIdObject(EntityKey key) {
        return prepareIdObject(key.getColumnNames(), key.getColumnValues());
    }

    private static MapObject prepareIdObject(IdSourceKey key) {
        return prepareIdObject(key.getColumnNames(), key.getColumnValues());
    }

    private static MapObject prepareIdObject(String[] columnNames, Object[] columnValues) {
        return com.rethinkdb.RethinkDB.r.hashMap(ID_FIELDNAME, prepareIdObjectValue(columnNames, columnValues));
    }

    private static Object prepareIdObjectValue(String[] columnNames, Object[] columnValues) {
        if (columnNames.length == 1) {
            return columnValues[0];
        } else {
            MapObject idObject = com.rethinkdb.RethinkDB.r.hashMap();
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                Object columnValue = columnValues[i];

                if (columnName.contains(PROPERTY_SEPARATOR)) {
                    int dotIndex = columnName.indexOf(PROPERTY_SEPARATOR);
                    String shortColumnName = columnName.substring(dotIndex + 1);
                    idObject.put(shortColumnName, columnValue);
                } else {
                    idObject.put(columnNames[i], columnValue);
                }

            }
            return idObject;
        }
    }

    private Table getCollection(String table) {
        return currentDB.table(table);
    }

    private Table getCollection(EntityKey key) {
        return getCollection(key.getTable());
    }

    private Table getCollection(EntityKeyMetadata entityKeyMetadata) {
        return getCollection(entityKeyMetadata.getTable());
    }

    private Table getAssociationCollection(AssociationKey key, AssociationStorageStrategy storageStrategy) {
        if (storageStrategy == AssociationStorageStrategy.GLOBAL_COLLECTION) {
            return getCollection(RethinkDBConfiguration.DEFAULT_ASSOCIATION_STORE);
        } else {
            return getCollection(ASSOCIATIONS_COLLECTION_PREFIX + key.getTable());
        }
    }

    private static MapObject getSubQuery(String operator, MapObject query) {
        return query.get(operator) != null ? (MapObject) query.get(operator) : com.rethinkdb.RethinkDB.r.hashMap();
    }

    private static void addSubQuery(String operator, MapObject query, String column, Object value) {
        MapObject subQuery = getSubQuery(operator, query);
//???		query.append( operator, subQuery.append( column, value ) );
    }

    @Override
    public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) {
        MapObject idObject = prepareIdObject(key);
        MapObject updater = objectForUpdate(tuple, idObject, tupleContext);

            getCollection(key).update( updater).run( this.provider.getConnection());
//            getCollection(key).update(idObject, updater, true, false);
    }

    @Override
    //TODO deal with dotted column names once this method is used for ALL / Dirty optimistic locking
    public boolean updateTupleWithOptimisticLock(EntityKey entityKey, Tuple oldLockState, Tuple tuple, TupleContext tupleContext) {
        MapObject idObject = prepareIdObject(entityKey);

        for (String versionColumn : oldLockState.getColumnNames()) {
            idObject.put(versionColumn, oldLockState.get(versionColumn));
        }

        MapObject updater = objectForUpdate(tuple, idObject, tupleContext);
        MapObject doc = getCollection(entityKey).update(updater).run( this.provider.getConnection());

        return doc != null;
    }

    @Override
    public void insertTuple(EntityKeyMetadata entityKeyMetadata, Tuple tuple, TupleContext tupleContext) {
        Map<String, Object> objectWithId = insertDBObject(entityKeyMetadata, tuple);
        String idColumnName = entityKeyMetadata.getColumnNames()[0];
        tuple.put(idColumnName, objectWithId.get(ID_FIELDNAME));
    }

    /*
	 * Insert the tuple and return an object containing the id in the field ID_FIELDNAME
     */
    private Map<String, Object> insertDBObject(EntityKeyMetadata entityKeyMetadata, Tuple tuple) {
        Map<String, Object> dbObject = objectForInsert(tuple, ((RethinkDBTupleSnapshot) tuple.getSnapshot()).getDbObject());
        getCollection(entityKeyMetadata).insert(dbObject);
        return dbObject;
    }

    /**
     * Creates a DBObject that can be passed to the MongoDB batch insert
     * function
     */
    private static Map<String, Object> objectForInsert(Tuple tuple, Map<String, Object> dbObject) {
        RethinkDBTupleSnapshot snapshot = (RethinkDBTupleSnapshot) tuple.getSnapshot();
        for (TupleOperation operation : tuple.getOperations()) {
            String column = operation.getColumn();
            if (notInIdField(snapshot, column)) {
                switch (operation.getType()) {
                    case PUT:
                        RethinkHelpers.setValue(dbObject, column, operation.getValue());
                        break;
                    case PUT_NULL:
                    case REMOVE:
                        RethinkHelpers.resetValue(dbObject, column);
                        break;
                }
            }
        }
        return dbObject;
    }

    private static MapObject objectForUpdate(Tuple tuple, MapObject idObject, TupleContext tupleContext) {
        RethinkDBTupleSnapshot snapshot = (RethinkDBTupleSnapshot) tuple.getSnapshot();
        EmbeddableStateFinder embeddableStateFinder = new EmbeddableStateFinder(tuple, tupleContext);
        Set<String> nullEmbeddables = new HashSet<>();

        MapObject updater = com.rethinkdb.RethinkDB.r.hashMap();
        for (TupleOperation operation : tuple.getOperations()) {
            String column = operation.getColumn();
            if (notInIdField(snapshot, column)) {
                switch (operation.getType()) {
                    case PUT:
                        addSubQuery("$set", updater, column, operation.getValue());
                        break;
                    case PUT_NULL:
                    case REMOVE:
                        // try and find if this column is within an embeddable and if that embeddable is null
                        // if true, unset the full embeddable
                        String nullEmbeddable = embeddableStateFinder.getOuterMostNullEmbeddableIfAny(column);
                        if (nullEmbeddable != null) {
                            // we have a null embeddable
                            if (!nullEmbeddables.contains(nullEmbeddable)) {
                                // we have not processed it yet
                                addSubQuery("$unset", updater, nullEmbeddable, Integer.valueOf(1));
                                nullEmbeddables.add(nullEmbeddable);
                            }
                        } else {
                            // simply unset the column
                            addSubQuery("$unset", updater, column, Integer.valueOf(1));
                        }
                        break;
                }
            }
        }
        /*
		* Needed because in case of an object with only an ID field
		* the "_id" won't be persisted properly.
		* With this adjustment, it will work like this:
		*	if the object (from snapshot) doesn't exist, create the one represented by updater
		*	so if at this moment the "_id" is not enforced properly, an ObjectID will be created by the server instead
		*	of the custom id
         */
        if (updater.size() == 0) {
            return idObject;
        }
        return updater;
    }

    private static boolean notInIdField(RethinkDBTupleSnapshot snapshot, String column) {
        return !column.equals(ID_FIELDNAME) && !column.endsWith(PROPERTY_SEPARATOR + ID_FIELDNAME) && !snapshot.isKeyColumn(column);
    }

    @Override
    public void removeTuple(EntityKey key, TupleContext tupleContext) {
        Table collection = getCollection(key);
        MapObject toDelete = prepareIdObject(key);

        collection.deleteAt( toDelete);
    }

    @Override
    public boolean removeTupleWithOptimisticLock(EntityKey entityKey, Tuple oldLockState, TupleContext tupleContext) {
        MapObject toDelete = prepareIdObject(entityKey);

        for (String versionColumn : oldLockState.getColumnNames()) {
            toDelete.put(versionColumn, oldLockState.get(versionColumn));
        }

        Table collection = getCollection(entityKey);
        MapObject deleted = null; //collection.findAndRemove(toDelete);

        return deleted != null;
    }

    //not for embedded
    private MapObject findAssociation(AssociationKey key, AssociationContext associationContext, AssociationStorageStrategy storageStrategy) {
        final MapObject associationKeyObject = associationKeyToObject(key, storageStrategy);

        return null; //getAssociationCollection(key, storageStrategy).findOne(associationKeyObject, getProjection(key, false));
    }

    private static String[] getProjection(AssociationKey key, boolean embedded) {
        if (embedded) {
            return getProjection(Collections.singletonList(key.getMetadata().getCollectionRole()));
        } else {
            return getProjection(ROWS_FIELDNAME_LIST);
        }
    }

    private static boolean isInTheQueue(EntityKey key, AssociationContext associationContext) {
        OperationsQueue queue = associationContext.getOperationsQueue();
        return queue != null && queue.contains(key);
    }

    @Override
    public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
        AssociationStorageStrategy storageStrategy = getAssociationStorageStrategy(key, associationContext);

        if (isEmbeddedAssociation(key) && isInTheQueue(key.getEntityKey(), associationContext)) {
            // The association is embedded and the owner of the association is in the insertion queue
            MapObject idObject = prepareIdObject(key.getEntityKey());
            return new Association(new RethinkDBAssociationSnapshot(idObject, key, storageStrategy));
        }

        // We need to execute the previous operations first or it won't be able to find the key that should have
        // been created
        executeBatch(associationContext.getOperationsQueue());
        if (storageStrategy == AssociationStorageStrategy.IN_ENTITY) {
            Map<String, Object> entity = getEmbeddingEntity(key, associationContext);

            if (entity != null && hasField(entity, key.getMetadata().getCollectionRole())) {
                return new Association(new RethinkDBAssociationSnapshot(entity, key, storageStrategy));
            } else {
                return null;
            }
        }
        final MapObject result = findAssociation(key, associationContext, storageStrategy);
        if (result == null) {
            return null;
        } else {
            return new Association(new RethinkDBAssociationSnapshot(result, key, storageStrategy));
        }
    }

    private static boolean isEmbeddedAssociation(AssociationKey key) {
        return AssociationKind.EMBEDDED_COLLECTION == key.getMetadata().getAssociationKind();
    }

    @Override
    public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
        AssociationStorageStrategy storageStrategy = getAssociationStorageStrategy(key, associationContext);

        Map<String, Object> document = storageStrategy == AssociationStorageStrategy.IN_ENTITY
                ? getEmbeddingEntity(key, associationContext)
                : associationKeyToObject(key, storageStrategy);

        return new Association(new RethinkDBAssociationSnapshot(document, key, storageStrategy));
    }

    /**
     * Returns the rows of the given association as to be stored in the
     * database. The return value is one of the following:
     * <ul>
     * <li>A list of plain values such as {@code String}s, {@code int}s etc. in
     * case there is exactly one row key column which is not part of the
     * association key (in this case we don't need to persist the key name as it
     * can be restored from the association key upon loading) or</li>
     * <li>A list of {@code DBObject}s with keys/values for all row key columns
     * which are not part of the association key</li>
     * <li>A {@link DBObject} with a key for each entry in case the given
     * association has exactly one row key column which is of type
     * {@code String} (e.g. a hash map) and
     * {@link DocumentStoreProperties#MAP_STORAGE} is not set to
     * {@link MapStorageType#AS_LIST}. The map values will either be plain
     * values (in case it's single values) or another {@code DBObject}.
     * </ul>
     */
    private static Object getAssociationRows(Association association, AssociationKey key, AssociationContext associationContext) {
        boolean organizeByRowKey = DotPatternMapHelpers.organizeAssociationMapByRowKey(association, key, associationContext);

        // transform map entries such as ( addressType='home', address_id=123) into the more
        // natural ( { 'home'=123 }
        if (organizeByRowKey) {
            String rowKeyColumn = organizeByRowKey ? key.getMetadata().getRowKeyIndexColumnNames()[0] : null;
            MapObject rows = com.rethinkdb.RethinkDB.r.hashMap();

            for (RowKey rowKey : association.getKeys()) {
                MapObject row = (MapObject) getAssociationRow(association.get(rowKey), key);

                String rowKeyValue = (String) row.remove(rowKeyColumn);

                // if there is a single column on the value side left, unwrap it
                if (row.keySet().size() == 1) {
//???                    rows.put(rowKeyValue, row.toMap().values().iterator().next());
                } else {
                    rows.put(rowKeyValue, row);
                }
            }

            return rows;
        } // non-map rows can be taken as is
        else {
            List<Object> rows = new ArrayList<>();

            for (RowKey rowKey : association.getKeys()) {
                rows.add(getAssociationRow(association.get(rowKey), key));
            }

            return rows;
        }
    }

    private static Object getAssociationRow(Tuple row, AssociationKey associationKey) {
        String[] rowKeyColumnsToPersist = associationKey.getMetadata().getColumnsWithoutKeyColumns(row.getColumnNames());

        // return value itself if there is only a single column to store
        if (rowKeyColumnsToPersist.length == 1) {
            return row.get(rowKeyColumnsToPersist[0]);
        } // otherwise a DBObject with the row contents
        else {
            // if the columns are only made of the embedded id columns, remove the embedded id property prefix
            // collectionrole: [ { id: { id1: "foo", id2: "bar" } } ] becomes collectionrole: [ { id1: "foo", id2: "bar" } ]
            String prefix = getColumnSharedPrefixOfAssociatedEntityLink(associationKey);

            MapObject rowObject = com.rethinkdb.RethinkDB.r.hashMap();
            for (String column : rowKeyColumnsToPersist) {
                Object value = row.get(column);
                if (value != null) {
                    // remove the prefix if present
                    String columnName = column.startsWith(prefix) ? column.substring(prefix.length()) : column;
                    RethinkHelpers.setValue(rowObject, columnName, value);
                }
            }
            return rowObject;
        }
    }

    @Override
    public void insertOrUpdateAssociation(AssociationKey key, Association association, AssociationContext associationContext) {
        Table collection;
        MapObject query;
        RethinkDBAssociationSnapshot assocSnapshot = (RethinkDBAssociationSnapshot) association.getSnapshot();
        String associationField;

        AssociationStorageStrategy storageStrategy = getAssociationStorageStrategy(key, associationContext);

        Object rows = getAssociationRows(association, key, associationContext);
        Object toStore = key.getMetadata().getAssociationType() == AssociationType.ONE_TO_ONE ? ((List<?>) rows).get(0) : rows;

        if (storageStrategy == AssociationStorageStrategy.IN_ENTITY) {
            collection = this.getCollection(key.getEntityKey());
            query = prepareIdObject(key.getEntityKey());
            associationField = key.getMetadata().getCollectionRole();

            //TODO would that fail if getCollectionRole has dots?
            ((RethinkDBTupleSnapshot) associationContext.getEntityTuple().getSnapshot()).getDbObject().put(key.getMetadata().getCollectionRole(), toStore);
        } else {
            collection = getAssociationCollection(key, storageStrategy);
            query = assocSnapshot.getQueryObject();
            associationField = ROWS_FIELDNAME;
        }

        MapObject update = com.rethinkdb.RethinkDB.r.hashMap("$set", com.rethinkdb.RethinkDB.r.hashMap(associationField, toStore));

//???        collection.update(query, update, true, false);
    }

    @Override
    public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
        AssociationStorageStrategy storageStrategy = getAssociationStorageStrategy(key, associationContext);

        if (storageStrategy == AssociationStorageStrategy.IN_ENTITY) {
            MapObject entity = prepareIdObject(key.getEntityKey());
            if (entity != null) {
                MapObject updater = com.rethinkdb.RethinkDB.r.hashMap();
                addSubQuery("$unset", updater, key.getMetadata().getCollectionRole(), Integer.valueOf(1));
                Map<String, Object> dbObject = getEmbeddingEntity(key, associationContext);
                if (dbObject != null) {
                    dbObject.remove(key.getMetadata().getCollectionRole());
//???                    getCollection(key.getEntityKey()).update(entity, updater, true, false);
                }
            }
        } else {
            Table collection = getAssociationCollection(key, storageStrategy);
            MapObject query = associationKeyToObject(key, storageStrategy);

//???            int nAffected = collection.remove(query).getN();
//???            log.removedAssociation(nAffected);
        }
    }

    @Override
    public Number nextValue(NextValueRequest request) {
//        Table currentCollection = getCollection(request.getKey().getTable());
//        MapObject query = prepareIdObject(request.getKey());
//        //all columns should match to find the value
//
//        String valueColumnName = request.getKey().getMetadata().getValueColumnName();
//
//        MapObject update = com.rethinkdb.RethinkDB.r.hashMap();
//        //FIXME how to set the initialValue if the document is not present? It seems the inc value is used as initial new value
//        Integer incrementObject = Integer.valueOf(request.getIncrement());
//        addSubQuery("$inc", update, valueColumnName, incrementObject);
//        MapObject result = currentCollection.findAndModify(query, null, null, false, update, false, true);
//        Object idFromDB;
//        idFromDB = result == null ? null : result.get(valueColumnName);
//        if (idFromDB == null) {
//            //not inserted yet so we need to add initial value to increment to have the right next value in the DB
//            //FIXME that means there is a small hole as when there was not value in the DB, we do add initial value in a non atomic way
//            MapObject updateForInitial = com.rethinkdb.RethinkDB.r.hashMap();
//            addSubQuery("$inc", updateForInitial, valueColumnName, request.getInitialValue());
//            currentCollection.findAndModify(query, null, null, false, updateForInitial, false, true);
//            idFromDB = request.getInitialValue(); //first time we ask this value
//        } else {
//            idFromDB = result.get(valueColumnName);
//        }
//        if (idFromDB.getClass().equals(Integer.class) || idFromDB.getClass().equals(Long.class)) {
//            Number id = (Number) idFromDB;
//            //idFromDB is the one used and the BD contains the next available value to use
//            return id;
//        } else {
            throw new HibernateException("Cannot increment a non numeric field");
//        }
    }

    @Override
    public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
        return getAssociationStorageStrategy(associationKeyMetadata, associationTypeContext) == AssociationStorageStrategy.IN_ENTITY;
    }

    @Override
    public GridType overrideType(Type type) {
        // Override handling of calendar types
        if( type == StandardBasicTypes.INTEGER) {
            return IntegerAsLongGridType.INSTANCE;
        }

        return null; // all other types handled as in hibernate-ogm-core
    }

    @Override
    public void forEachTuple(ModelConsumer consumer, TupleContext tupleContext, EntityKeyMetadata entityKeyMetadata) {
        Db db = provider.getDatabase();
        Table collection = db.table(entityKeyMetadata.getTable());
        for (MapObject dbObject : (List<MapObject>)collection.getAll( com.rethinkdb.RethinkDB.r.hashMap()).run( this.provider.getConnection())) {
            consumer.consume(new Tuple(new RethinkDBTupleSnapshot(dbObject, entityKeyMetadata, UPDATE)));
        }
    }

    @Override
    public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<RethinkDBQueryDescriptor> backendQuery, QueryParameters queryParameters, TupleContext tupleContext) {
        RethinkDBQueryDescriptor queryDescriptor = backendQuery.getQuery();

        EntityKeyMetadata entityKeyMetadata
                = backendQuery.getSingleEntityMetadataInformationOrNull() == null ? null
                        : backendQuery.getSingleEntityMetadataInformationOrNull().getEntityKeyMetadata();

        String collectionName = getCollectionName(backendQuery, queryDescriptor, entityKeyMetadata);
        Table collection = provider.getDatabase().table(collectionName);

        if (!queryParameters.getPositionalParameters().isEmpty()) { // TODO Implement binding positional parameters.
            throw new UnsupportedOperationException("Positional parameters are not yet supported for MongoDB native queries.");
        }

        switch (queryDescriptor.getOperation()) {
            case FIND:
                return doFind(queryDescriptor, queryParameters, collection, entityKeyMetadata);
            case FINDONE:
                return doFindOne(queryDescriptor, collection, entityKeyMetadata);
            case FINDANDMODIFY:
                return doFindAndModify(queryDescriptor, collection, entityKeyMetadata);
            case AGGREGATE:
                return doAggregate(queryDescriptor, queryParameters, collection, entityKeyMetadata);
            case COUNT:
                return doCount(queryDescriptor, collection);
            case INSERT:
            case REMOVE:
            case UPDATE:
                throw log.updateQueryMustBeExecutedViaExecuteUpdate(queryDescriptor);
            default:
                throw new IllegalArgumentException("Unexpected query operation: " + queryDescriptor);
        }
    }

    @Override
    public int executeBackendUpdateQuery(final BackendQuery<RethinkDBQueryDescriptor> backendQuery, final QueryParameters queryParameters, final TupleContext tupleContext) {
        RethinkDBQueryDescriptor queryDescriptor = backendQuery.getQuery();

        EntityKeyMetadata entityKeyMetadata
                = backendQuery.getSingleEntityMetadataInformationOrNull() == null ? null
                        : backendQuery.getSingleEntityMetadataInformationOrNull().getEntityKeyMetadata();

        String collectionName = getCollectionName(backendQuery, queryDescriptor, entityKeyMetadata);
        Table collection = provider.getDatabase().table(collectionName);

        if (!queryParameters.getPositionalParameters().isEmpty()) { // TODO Implement binding positional parameters.
            throw new UnsupportedOperationException("Positional parameters are not yet supported for MongoDB native queries.");
        }

        switch (queryDescriptor.getOperation()) {
            case INSERT:
                return doInsert(queryDescriptor, collection);
            case REMOVE:
                return doRemove(queryDescriptor, collection);
            case UPDATE:
                return doUpdate(queryDescriptor, collection);
            case FIND:
            case FINDONE:
            case FINDANDMODIFY:
            case AGGREGATE:
            case COUNT:
                throw log.readQueryMustBeExecutedViaGetResultList(queryDescriptor);
            default:
                throw new IllegalArgumentException("Unexpected query operation: " + queryDescriptor);
        }
    }

    @Override
    public RethinkDBQueryDescriptor parseNativeQuery(String nativeQuery) {
        NativeQueryParser parser = Parboiled.createParser(NativeQueryParser.class);
        ParsingResult<RethinkDBQueryDescriptorBuilder> parseResult = new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>(parser.Query())
                .run(nativeQuery);
        if (parseResult.hasErrors()) {
            throw new IllegalArgumentException("Unsupported native query: " + ErrorUtils.printParseErrors(parseResult.parseErrors));
        }

        return parseResult.resultValue.build();
    }

    @Override
    public DuplicateInsertPreventionStrategy getDuplicateInsertPreventionStrategy(EntityKeyMetadata entityKeyMetadata) {
        return DuplicateInsertPreventionStrategy.NATIVE;
    }

    private static ClosableIterator<Tuple> doAggregate(RethinkDBQueryDescriptor query, QueryParameters queryParameters, Table collection, EntityKeyMetadata entityKeyMetadata) {
        List<MapObject> pipeline = new ArrayList<>();

        pipeline.add(stage("$match", query.getCriteria()));
        pipeline.add(stage("$project", query.getProjection()));

        if (query.getUnwinds() != null && !query.getUnwinds().isEmpty()) {
            for (String field : query.getUnwinds()) {
                pipeline.add(stage("$unwind", "$" + field));
            }
        }

        if (query.getOrderBy() != null) {
            pipeline.add(stage("$sort", query.getOrderBy()));
        }

        // apply firstRow/maxRows if present
        if (queryParameters.getRowSelection().getFirstRow() != null) {
            pipeline.add(stage("$skip", queryParameters.getRowSelection().getFirstRow()));
        }

        if (queryParameters.getRowSelection().getMaxRows() != null) {
            pipeline.add(stage("$limit", queryParameters.getRowSelection().getMaxRows()));
        }

//???        AggregationOutput output = collection.aggregate(pipeline);
//        return new MongoDBAggregationOutput(null /*output*/, entityKeyMetadata);
        return null;
    }

    private static MapObject stage(String key, Object value) {
        MapObject stage = com.rethinkdb.RethinkDB.r.hashMap();
        stage.put(key, value);
        return stage;
    }

    private static ClosableIterator<Tuple> doFind(RethinkDBQueryDescriptor query, QueryParameters queryParameters, Table collection, EntityKeyMetadata entityKeyMetadata) {
//        Cursor cursor = collection.getAll(query.getCriteria()).pluck(query.getProjection()).run( this.provider.getConnection());
//        if (query.getOrderBy() != null) {
//            cursor.sort(query.getOrderBy());
//        }
//
//        // apply firstRow/maxRows if present
//        if (queryParameters.getRowSelection().getFirstRow() != null) {
//            cursor.skip(queryParameters.getRowSelection().getFirstRow());
//        }
//
//        if (queryParameters.getRowSelection().getMaxRows() != null) {
//            cursor.limit(queryParameters.getRowSelection().getMaxRows());
//        }
//
//        return new MongoDBResultsCursor(cursor, entityKeyMetadata);
        return null;
    }

    private static ClosableIterator<Tuple> doFindOne(final RethinkDBQueryDescriptor query, final Table collection,
           final EntityKeyMetadata entityKeyMetadata) {
//
//        final MapObject theOne = collection.get(query.getCriteria()).pluck( query.getProjection()).run( this.provider.getConnection());
//        return new SingleTupleIterator(theOne, collection, entityKeyMetadata);
            
            return null;
    }

    private static ClosableIterator<Tuple> doFindAndModify(final RethinkDBQueryDescriptor queryDesc, final Table collection,
            final EntityKeyMetadata entityKeyMetadata) {

        MapObject query = (MapObject) queryDesc.getCriteria().get("query");
        MapObject fields = (MapObject) queryDesc.getCriteria().get("fields");
        MapObject sort = (MapObject) queryDesc.getCriteria().get("sort");
        Boolean remove = (Boolean) queryDesc.getCriteria().get("remove");
        MapObject update = (MapObject) queryDesc.getCriteria().get("update");
        Boolean nevv = (Boolean) queryDesc.getCriteria().get("new");
        Boolean upsert = (Boolean) queryDesc.getCriteria().get("upsert");
        Boolean bypass = (Boolean) queryDesc.getCriteria().get("bypassDocumentValidation");
        MapObject o = (MapObject) queryDesc.getCriteria().get("writeConcern");

//        final MapObject theOne = collection.findAndModify(query, fields, sort, (remove != null ? remove : false),
//                update, (nevv != null ? nevv : false), (upsert != null ? upsert : false), (bypass != null ? bypass : false),
//                0, TimeUnit.MILLISECONDS);
//        return new SingleTupleIterator(theOne, collection, entityKeyMetadata);

        return null;
    }

    @SuppressWarnings("unchecked")
    private static int doInsert(final RethinkDBQueryDescriptor queryDesc, final Table collection) {
//        MapObject insert = queryDesc.getUpdateOrInsert();
//        MapObject options = queryDesc.getOptions();
//        Boolean ordered = FALSE;
//        if (options != null) {
//            ordered = (Boolean) options.get("ordered");
//            ordered = (ordered != null) ? ordered : FALSE;
//            MapObject o = (MapObject) options.get("writeConcern");
//        }
//
//        // Need to use BulkWriteOperation here rather than collection.insert(..) because the WriteResult returned
//        // by the latter returns 0 for getN() even if the insert was successful (which is bizarre, but that's the way it
//        // is defined...)
//        BulkWriteOperation bo = (ordered) ? collection.initializeOrderedBulkOperation() : collection.initializeUnorderedBulkOperation();
//        if (insert instanceof List<?>) {
//            for (MapObject i : (List<MapObject>) insert) {
//                bo.insert(i);
//            }
//        } else {
//            bo.insert(insert);
//        }
//
//        final BulkWriteResult result = bo.execute((wc != null ? wc : collection.getWriteConcern()));
//
//        if (result.isAcknowledged()) {
//            return result.getInsertedCount();
//        }
        return -1; // Not sure if we should throw an exception instead?
    }

    private static int doRemove(final RethinkDBQueryDescriptor queryDesc, final Table collection) {
//        MapObject query = queryDesc.getCriteria();
//        MapObject options = queryDesc.getOptions();
//        Boolean justOne = FALSE;
//
//        if (options != null) {
//            justOne = (Boolean) options.get("justOne");
//            justOne = (justOne != null) ? justOne : FALSE;
//            if (justOne) { // IMPROVE See https://jira.mongodb.org/browse/JAVA-759
//                throw new UnsupportedOperationException("Using 'justOne' in a remove query is not yet supported.");
//            }
//            MapObject o = (MapObject) options.get("writeConcern");
//        }
//
//        final WriteResult result = collection.remove(query, (wc != null ? wc : collection.getWriteConcern()));
//        if (result.wasAcknowledged()) {
//            return result.getN();
//        }
        return -1; // Not sure if we should throw an exception instead?
    }

    private static int doUpdate(final RethinkDBQueryDescriptor queryDesc, final Table collection) {
//        MapObject query = queryDesc.getCriteria();
//        MapObject update = queryDesc.getUpdateOrInsert();
//        MapObject options = queryDesc.getOptions();
//        Boolean upsert = FALSE;
//        Boolean multi = FALSE;
//
//        if (options != null) {
//            upsert = (Boolean) options.get("upsert");
//            upsert = (upsert != null) ? upsert : FALSE;
//            multi = (Boolean) options.get("multi");
//            multi = (multi != null) ? multi : FALSE;
//            MapObject o = (MapObject) options.get("writeConcern");
//        }
//
//        final WriteResult result = collection.update(query, update, upsert, multi, (wc != null ? wc : collection.getWriteConcern()));
//        if (result.wasAcknowledged()) {
//            // IMPROVE How could we return result.getUpsertedId() if it was an upsert, or isUpdateOfExisting()?
//            // I see only a possibility by using javax.persistence.StoredProcedureQuery in the application
//            // and then using getOutputParameterValue(String) to get additional result values.
//            return result.getN();
//        }
        return -1; // Not sure if we should throw an exception instead?
    }

    private static ClosableIterator<Tuple> doCount(RethinkDBQueryDescriptor query, Table collection) {
//        long count = collection.count(query.getCriteria());
//        MapTupleSnapshot snapshot = new MapTupleSnapshot(Collections.<String, Object>singletonMap("n", count));
//        return CollectionHelper.newClosableIterator(Collections.singletonList(new Tuple(snapshot)));

        return null;
    }

    /**
     * Returns the name of the MongoDB collection to execute the given query
     * against. Will either be retrieved
     * <ul>
     * <li>from the given query descriptor (in case the query has been
     * translated from JP-QL or it is a native query using the extended syntax
     * {@code db.<COLLECTION>.<OPERATION>(...)}</li>
     * <li>or from the single mapped entity type if it is a native query using
     * the criteria-only syntax
     *
     * @param customQuery the original query to execute
     * @param queryDescriptor descriptor for the query
     * @param entityKeyMetadata meta-data in case this is a query with exactly
     * one entity return
     * @return the name of the MongoDB collection to execute the given query
     * against
     */
    private static String getCollectionName(BackendQuery<?> customQuery, RethinkDBQueryDescriptor queryDescriptor, EntityKeyMetadata entityKeyMetadata) {
        if (queryDescriptor.getCollectionName() != null) {
            return queryDescriptor.getCollectionName();
        } else if (entityKeyMetadata != null) {
            return entityKeyMetadata.getTable();
        } else {
            throw log.unableToDetermineCollectionName(customQuery.getQuery().toString());
        }
    }

    private static MapObject associationKeyToObject(AssociationKey key, AssociationStorageStrategy storageStrategy) {
        if (storageStrategy == AssociationStorageStrategy.IN_ENTITY) {
            throw new AssertionFailure(RethinkHelpers.class.getName()
                    + ".associationKeyToObject should not be called for associations embedded in entity documents");
        }
        Object[] columnValues = key.getColumnValues();
        MapObject columns = com.rethinkdb.RethinkDB.r.hashMap();

        // if the columns are only made of the embedded id columns, remove the embedded id property prefix
        // _id: [ { id: { id1: "foo", id2: "bar" } } ] becomes _id: [ { id1: "foo", id2: "bar" } ]
        String prefix = DocumentHelpers.getColumnSharedPrefix(key.getColumnNames());
        prefix = prefix == null ? "" : prefix + ".";
        int i = 0;
        for (String name : key.getColumnNames()) {
            RethinkHelpers.setValue(columns, name.substring(prefix.length()), columnValues[i++]);
        }

        MapObject idObject = com.rethinkdb.RethinkDB.r.hashMap();

        if (storageStrategy == AssociationStorageStrategy.GLOBAL_COLLECTION) {
            columns.put(RethinkDBDialect.TABLE_FIELDNAME, key.getTable());
        }
        idObject.put(RethinkDBDialect.ID_FIELDNAME, columns);
        return idObject;
    }

    private static AssociationStorageStrategy getAssociationStorageStrategy(AssociationKey key, AssociationContext associationContext) {
        return getAssociationStorageStrategy(key.getMetadata(), associationContext.getAssociationTypeContext());
    }

    /**
     * Returns the {@link AssociationStorageStrategy} effectively applying for
     * the given association. If a setting is given via the option mechanism,
     * that one will be taken, otherwise the default value as given via the
     * corresponding configuration property is applied.
     */
    private static AssociationStorageStrategy getAssociationStorageStrategy(AssociationKeyMetadata keyMetadata, AssociationTypeContext associationTypeContext) {
        AssociationStorageType associationStorage = associationTypeContext
                .getOptionsContext()
                .getUnique(AssociationStorageOption.class);

        AssociationDocumentStorageType associationDocumentStorageType = associationTypeContext
                .getOptionsContext()
                .getUnique(AssociationDocumentStorageOption.class);

        return AssociationStorageStrategy.getInstance(keyMetadata, associationStorage, associationDocumentStorageType);
    }

    @Override
    public void executeBatch(OperationsQueue queue) {
        if (!queue.isClosed()) {
            Operation operation = queue.poll();
            Map<Table, BatchInsertionTask> inserts = new HashMap<>();

            List<RethinkDBTupleSnapshot> insertSnapshots = new ArrayList<>();

            while (operation != null) {
                if (operation instanceof InsertOrUpdateTupleOperation) {
                    InsertOrUpdateTupleOperation update = (InsertOrUpdateTupleOperation) operation;
                    executeBatchUpdate(inserts, update);
                    RethinkDBTupleSnapshot snapshot = (RethinkDBTupleSnapshot) update.getTuple().getSnapshot();
                    if (snapshot.getSnapshotType() == INSERT) {
                        insertSnapshots.add(snapshot);
                    }
                } else if (operation instanceof RemoveTupleOperation) {
                    RemoveTupleOperation tupleOp = (RemoveTupleOperation) operation;
                    executeBatchRemove(inserts, tupleOp);
                } else if (operation instanceof InsertOrUpdateAssociationOperation) {
                    InsertOrUpdateAssociationOperation update = (InsertOrUpdateAssociationOperation) operation;
                    executeBatchUpdateAssociation(inserts, update);
                } else if (operation instanceof RemoveAssociationOperation) {
                    RemoveAssociationOperation remove = (RemoveAssociationOperation) operation;
                    removeAssociation(remove.getAssociationKey(), remove.getContext());
                } else {
                    throw new UnsupportedOperationException("Operation not supported on MongoDB: " + operation.getClass().getName());
                }
                operation = queue.poll();
            }
            flushInserts( this.provider.getConnection(), inserts);

            for (RethinkDBTupleSnapshot insertSnapshot : insertSnapshots) {
                insertSnapshot.setSnapshotType(UPDATE);
            }
            queue.clear();
        }
    }

    private void executeBatchRemove(Map<Table, BatchInsertionTask> inserts, RemoveTupleOperation tupleOperation) {
        EntityKey entityKey = tupleOperation.getEntityKey();
        Table collection = getCollection(entityKey);
        BatchInsertionTask batchedInserts = inserts.get(collection);

        if (batchedInserts != null && batchedInserts.containsKey(entityKey)) {
            batchedInserts.remove(entityKey);
        } else {
            removeTuple(entityKey, tupleOperation.getTupleContext());
        }
    }

    private void executeBatchUpdate(Map<Table, BatchInsertionTask> inserts, InsertOrUpdateTupleOperation tupleOperation) {
        EntityKey entityKey = tupleOperation.getEntityKey();
        Tuple tuple = tupleOperation.getTuple();
        RethinkDBTupleSnapshot snapshot = (RethinkDBTupleSnapshot) tupleOperation.getTuple().getSnapshot();

        if (INSERT == snapshot.getSnapshotType()) {
            prepareForInsert(inserts, snapshot, entityKey, tuple);
        } else {
            // Object already exists in the db or has invalid fields:
            insertOrUpdateTuple(entityKey, tuple, tupleOperation.getTupleContext());
        }
    }

    private void executeBatchUpdateAssociation(Map<Table, BatchInsertionTask> inserts, InsertOrUpdateAssociationOperation updateOp) {
        AssociationKey associationKey = updateOp.getAssociationKey();
        if (isEmbeddedAssociation(associationKey)) {
            Table collection = getCollection(associationKey.getEntityKey());
            BatchInsertionTask batchInserts = inserts.get(collection);
            if (batchInserts != null && batchInserts.containsKey(associationKey.getEntityKey())) {
                // The owner of the association is in the insertion queue,
                // we are going to update it with the collection of elements
                BatchInsertionTask insertTask = getOrCreateBatchInsertionTask(inserts, associationKey.getEntityKey().getMetadata(), collection);
                Map<String, Object> documentForInsertion = insertTask.get(associationKey.getEntityKey());
                Object embeddedElements = getAssociationRows(updateOp.getAssociation(), updateOp.getAssociationKey(), updateOp.getContext());
                String collectionRole = associationKey.getMetadata().getCollectionRole();
                RethinkHelpers.setValue(documentForInsertion, collectionRole, embeddedElements);
            } else {
                insertOrUpdateAssociation(updateOp.getAssociationKey(), updateOp.getAssociation(), updateOp.getContext());
            }
        } else {
            insertOrUpdateAssociation(updateOp.getAssociationKey(), updateOp.getAssociation(), updateOp.getContext());
        }
    }

    @Override
    public ParameterMetadataBuilder getParameterMetadataBuilder() {
        return NoOpParameterMetadataBuilder.INSTANCE;
    }

    private void prepareForInsert(Map<Table, BatchInsertionTask> inserts, RethinkDBTupleSnapshot snapshot, EntityKey entityKey, Tuple tuple) {
        Table collection = getCollection(entityKey);
        BatchInsertionTask batchInsertion = getOrCreateBatchInsertionTask(inserts, entityKey.getMetadata(), collection);
        Map<String, Object> document = getCurrentDocument(snapshot, batchInsertion, entityKey);
        Map<String, Object> newDocument = objectForInsert(tuple, document);
        inserts.get(collection).put(entityKey, newDocument);
    }

    private static Map<String, Object> getCurrentDocument(RethinkDBTupleSnapshot snapshot, BatchInsertionTask batchInsert, EntityKey entityKey) {
        Map<String, Object> fromBatchInsertion = batchInsert.get(entityKey);
        return fromBatchInsertion != null ? fromBatchInsertion : snapshot.getDbObject();
    }

    private static BatchInsertionTask getOrCreateBatchInsertionTask(Map<Table, BatchInsertionTask> inserts, EntityKeyMetadata entityKeyMetadata, Table collection) {
        BatchInsertionTask insertsForCollection = inserts.get(collection);

        if (insertsForCollection == null) {
            insertsForCollection = new BatchInsertionTask(entityKeyMetadata);
            inserts.put(collection, insertsForCollection);
        }

        return insertsForCollection;
    }

    private static void flushInserts( Connection connection, Map<Table, BatchInsertionTask> inserts) {
        for (Map.Entry<Table, BatchInsertionTask> entry : inserts.entrySet()) {
            Table collection = entry.getKey();
            if (entry.getValue().isEmpty()) {
                // has been emptied due to subsequent removals before flushes
                continue;
            }

//            try {
                
            for( Map<String, Object> entity : entry.getValue().getAll()) {

                collection.insert( entity).run( connection);
            }
//            } catch (DuplicateKeyException dke) {
//                // This exception is used by MongoDB for all the unique indexes violation, not only the primary key
//                // so we determine if it concerns the primary key by matching on the message
//                if (PRIMARY_KEY_CONSTRAINT_VIOLATION_MESSAGE.matcher(dke.getMessage()).matches()) {
//                    throw new TupleAlreadyExistsException(entry.getValue().getEntityKeyMetadata(), null, dke);
//                } else {
//                    throw log.constraintViolationOnFlush(dke.getMessage(), dke);
//                }
//            }
        }
        inserts.clear();
    }

//    private static class MongoDBAggregationOutput implements ClosableIterator<Tuple> {
//
//        private final Iterator<MapObject> results;
//        private final EntityKeyMetadata metadata;
//
//        public MongoDBAggregationOutput(AggregationOutput output, EntityKeyMetadata metadata) {
//            this.results = output.results().iterator();
//            this.metadata = metadata;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return results.hasNext();
//        }
//
//        @Override
//        public Tuple next() {
//            MapObject dbObject = results.next();
//            return new Tuple(new RethinkDBTupleSnapshot(dbObject, metadata, UPDATE));
//        }
//
//        @Override
//        public void remove() {
//            results.remove();
//        }
//
//        @Override
//        public void close() {
//            // Nothing to do
//        }
//    }
//
//    private static class MongoDBResultsCursor implements ClosableIterator<Tuple> {
//
//        private final Cursor<MapObject> cursor;
//        private final EntityKeyMetadata metadata;
//
//        public MongoDBResultsCursor(Cursor cursor, EntityKeyMetadata metadata) {
//            this.cursor = cursor;
//            this.metadata = metadata;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return cursor.hasNext();
//        }
//
//        @Override
//        public Tuple next() {
//            MapObject dbObject = cursor.next();
//            return new Tuple(new RethinkDBTupleSnapshot(dbObject, metadata, UPDATE));
//        }
//
//        @Override
//        public void remove() {
//            cursor.remove();
//        }
//
//        @Override
//        public void close() {
//            cursor.close();
//        }
//    }

    private static class SingleTupleIterator implements ClosableIterator<Tuple> {

        private MapObject theOne;
        private final EntityKeyMetadata metadata;
        private final Table collection;

        public SingleTupleIterator(MapObject theOne, Table collection, EntityKeyMetadata metadata) {
            this.theOne = theOne;
            this.collection = collection;
            this.metadata = metadata;
        }

        @Override
        public boolean hasNext() {
            return theOne != null;
        }

        @Override
        public Tuple next() {
            if (theOne == null) {
                throw new NoSuchElementException(); // Seemingly a programming error if this line is ever reached.
            }

            Tuple t = new Tuple(new RethinkDBTupleSnapshot(theOne, metadata, UPDATE));
            theOne = null;
            return t;
        }

        @Override
        public void remove() {
            if (theOne == null) {
                throw new IllegalStateException(); // Seemingly a programming error if this line is ever reached.
            }

            collection.deleteAt(theOne).run( null);
            theOne = null;
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    private static class BatchInsertionTask {

        private final EntityKeyMetadata entityKeyMetadata;
        private final Map<EntityKey, Map<String, Object>> inserts;

        public BatchInsertionTask(EntityKeyMetadata entityKeyMetadata) {
            this.entityKeyMetadata = entityKeyMetadata;
            this.inserts = new HashMap<>();
        }

        public EntityKeyMetadata getEntityKeyMetadata() {
            return entityKeyMetadata;
        }

        public List<Map<String, Object>> getAll() {
            return new ArrayList<>(inserts.values());
        }

        public Map<String, Object> get(EntityKey entityKey) {
            return inserts.get(entityKey);

        }

        public boolean containsKey(EntityKey entityKey) {
            return inserts.containsKey(entityKey);
        }

        public Map<String, Object> remove(EntityKey entityKey) {
            return inserts.remove(entityKey);
        }

        public void put(EntityKey entityKey, Map<String, Object> object) {
            inserts.put(entityKey, object);
        }

        public boolean isEmpty() {
            return inserts.isEmpty();
        }
    }
}
