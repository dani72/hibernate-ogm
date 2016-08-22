/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.query.parsing.nativequery;

import static org.fest.assertions.Assertions.assertThat;

import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor.Operation;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.nativequery.impl.RethinkDBQueryDescriptorBuilder;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.nativequery.impl.NativeQueryParser;
import org.hibernate.ogm.utils.TestForIssue;
import org.junit.Test;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.RecoveringParseRunner;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

/**
 * Unit test for {@link NativeQueryParser}.
 *
 * @author Gunnar Morling
 */
public class NativeQueryParserTest {

//	@Test
//	public void shouldParseSimplifiedFindQuery() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "{ \"foo\" : true }" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isNull();
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseSimpleQuery() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find({\"foo\":true})" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseSimpleQueryUsingSingleQuotes() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { 'foo' : true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryWithEmptyFind() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find({})" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( new BasicDBObject() );
//	}
//
//	@Test
//	public void shouldParseQueryInsertSingleDocument() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.insert( { 'item': 'card', 'qty': 15 } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.INSERT );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse( "{ 'item': 'card', 'qty': 15 }" ) );
//	}
//
//	@Test
//	public void shouldParseQueryInsertSingleDocumentAndOptions() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.insert( { 'item': 'card', 'qty': 15 }, { 'ordered': true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.INSERT );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse( "{ 'item': 'card', 'qty': 15 }" ) );
//		assertThat( queryDescriptor.getOptions() ).isEqualTo( JSON.parse( "{ ordered: true })" ) );
//	}
//
//	@Test
//	public void shouldParseQueryInsertMultipleDocuments() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.insert( [ { '_id': 11, 'item': 'pencil', 'qty': 50, 'type': 'no.2' }, { 'item': 'pen', 'qty': 20 }, { 'item': 'eraser', 'qty': 25 } ] )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.INSERT );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse(
//				"[ { '_id': 11, 'item': 'pencil', 'qty': 50, 'type': 'no.2' }, { 'item': 'pen', 'qty': 20 }, { 'item': 'eraser', 'qty': 25 } ]" ) );
//	}
//
//	@Test
//	public void shouldParseQueryInsertMultipleDocumentsAndOptions() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.insert( [ { '_id': 11, 'item': 'pencil', 'qty': 50, 'type': 'no.2' }, { 'item': 'pen', 'qty': 20 }, { 'item': 'eraser', 'qty': 25 } ], { 'ordered': true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.INSERT );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse(
//				"[ { '_id': 11, 'item': 'pencil', 'qty': 50, 'type': 'no.2' }, { 'item': 'pen', 'qty': 20 }, { 'item': 'eraser', 'qty': 25 } ]" ) );
//		assertThat( queryDescriptor.getOptions() ).isEqualTo( JSON.parse( "{ ordered: true })" ) );
//	}
//
//	@Test
//	public void shouldParseQueryWithEmptyRemove() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.remove( 	{\n 	}\n 	)" ); // Include superfluous whitespace.
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.REMOVE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( new BasicDBObject() );
//	}
//
//	@Test
//	public void shouldParseQueryWithEmptyRemoveAndOptionalJustOne() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.remove({},true)" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.REMOVE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( new BasicDBObject() );
//		assertThat( queryDescriptor.getOptions() ).isEqualTo( JSON.parse( "{ \"justOne\" : true }" ) );
//	}
//
//	@Test
//	public void shouldParseQueryWithEmptyRemoveAndOptions() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.remove( { }, { 'justOne': true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.REMOVE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( new BasicDBObject() );
//		assertThat( queryDescriptor.getOptions() ).isEqualTo( JSON.parse( "{ \"justOne\" : true }" ) );
//	}
//
//	@Test
//	public void shouldParseQueryUpdate() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.update( { 'name': 'Andy' }, { 'rating': 1, 'score': 1 } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.UPDATE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ 'name': 'Andy' }" ) );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse( "{ 'rating': 1, 'score': 1 }" ) );
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryUpdateWithOptions() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.update( { 'name': 'Andy' }, { 'rating': 1, 'score': 1 }, { 'upsert': true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.UPDATE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ 'name': 'Andy' }" ) );
//		assertThat( queryDescriptor.getUpdateOrInsert() ).isEqualTo( JSON.parse( "{ 'rating': 1, 'score': 1 }" ) );
//		assertThat( queryDescriptor.getOptions() ).isEqualTo( JSON.parse( "{ 'upsert': true }" ) );
//	}
//
//	@Test
//	public void shouldParseQueryFindAndModify() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.findAndModify( { 'query': { 'name': 'Andy' }, 'sort': { 'rating': 1 }, 'update': { '$inc': { 'score': 1 } }, 'upsert': true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FINDANDMODIFY );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse(
//				"{ 'query': { 'name': 'Andy' }, 'sort': { 'rating': 1 }, 'update': { '$inc': { 'score': 1 } }, 'upsert': true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryFindOneWithoutCriteriaNorProjection() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.findOne(  )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FINDONE );
//		assertThat( queryDescriptor.getCriteria() ).isNull();
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryFindOneWithoutProjection() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.findOne( { \"foo\" : true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FINDONE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryFindOneWithCriteriaAndProjection() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.findOne( { \"foo\" : true }, { \"foo\" : 1 } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FINDONE );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isEqualTo( JSON.parse( "{ \"foo\" : 1 }" ) );
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryWithProjection() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { \"foo\" : true }, { \"foo\" : 1 } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isEqualTo( JSON.parse( "{ \"foo\" : 1 }" ) );
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryWithWhitespace() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "  db  .  Order  .  find  (  {  \"  foo  \"  :  true  }  ,  {  \"foo\"  :  1  }  )  " );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//		assertThat( run.hasErrors() ).isFalse();
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"  foo  \" : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isEqualTo( JSON.parse( "{ \"foo\" : 1 }" ) );
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseQueryWithSeveralConditions() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ReportingParseRunner<RethinkDBQueryDescriptorBuilder> runner = new ReportingParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  runner
//				.run( "db.Order.find( { \"foo\" : true, \"bar\" : 42, \"baz\" : \"qux\" } )" );
//
//		assertThat( run.hasErrors() ).isFalse();
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ \"foo\" : true, \"bar\" : 42, \"baz\" : \"qux\" }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQuery() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count()" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isNull();
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQueryWithCriteria() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count( { 'foo' : true } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ 'foo' : true }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQueryWithLogicalOperatorOR() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count( { '$or': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$or': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQueryWithLogicalOperatorAND() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count( { '$and': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$and': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQueryWithLogicalOperatorNOR() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count( { '$nor': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$nor': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseCountQueryWithLogicalOperatorNOT() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.count( { '$not': { 'foo' : false } } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$not': { 'foo' : false } } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseFindQueryWithLogicalOperatorOR() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { '$or': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$or': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldParseFindQueryWithLogicalOperatorAND() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { '$and': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$and': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldFindCountQueryWithLogicalOperatorNOR() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { '$nor': [ { 'foo' : true }, { 'bar' : '42' } ] } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$nor': [ { 'foo' : true }, { 'bar' : '42' } ] } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	public void shouldFindeCountQueryWithLogicalOperatorNOT() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.Order.find( { '$not': { 'foo' : false } } )" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "Order" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.FIND );
//		assertThat( queryDescriptor.getCriteria() ).isEqualTo( JSON.parse( "{ '$not': { 'foo' : false } } }" ) );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//
//	@Test
//	@TestForIssue(jiraKey = "OGM-900")
//	public void shouldSupportDotInCollectionName() {
//		NativeQueryParser parser = Parboiled.createParser( NativeQueryParser.class );
//		ParsingResult<RethinkDBQueryDescriptorBuilder> run =  new RecoveringParseRunner<RethinkDBQueryDescriptorBuilder>( parser.Query() )
//				.run( "db.POEM.COM.count()" );
//
//		RethinkDBQueryDescriptor queryDescriptor = run.resultValue.build();
//
//		assertThat( queryDescriptor.getCollectionName() ).isEqualTo( "POEM.COM" );
//		assertThat( queryDescriptor.getOperation() ).isEqualTo( Operation.COUNT );
//		assertThat( queryDescriptor.getProjection() ).isNull();
//		assertThat( queryDescriptor.getOrderBy() ).isNull();
//	}
//

}
