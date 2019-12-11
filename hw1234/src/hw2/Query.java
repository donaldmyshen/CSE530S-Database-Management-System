 
package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.Tuple;
import hw1.TupleDesc;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute() {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();

		// your code here
		Catalog catalog = Database.getCatalog();
		ColumnVisitor colVisitor = new ColumnVisitor();
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tbList = tablesNamesFinder.getTableList(statement);

		int tbId = catalog.getTableId(tbList.get(0));
		ArrayList<Tuple> tupleList = catalog.getDbFile(tbId).getAllTuples();
		TupleDesc originTd = catalog.getTupleDesc(tbId);
		Relation curRel = new Relation(tupleList, originTd);
		
		// join
		Relation joinRel = curRel;
		List<Join> joinList = sb.getJoins();

		if (joinList != null) {
			for (Join join : joinList) {
				// get current relation
				String tableName = join.getRightItem().toString();
				TupleDesc joinTupleDesc = catalog.getTupleDesc(catalog.getTableId(tableName));
				ArrayList<Tuple> joinTupleList = catalog.getDbFile(catalog.getTableId(tableName)).getAllTuples();
				Relation newJoinRel = new Relation(joinTupleList, joinTupleDesc);
				
				// deal the input with regular expression
				String[] deal = join.getOnExpression().toString().split("=");
				String[] t1Field = deal[0].trim().split("\\.");
				String[] t2Field = deal[1].trim().split("\\.");

				String tableName2 = t2Field[0], fieldName1 = t1Field[1], fieldName2 = t2Field[1];
				
				// if name not equal, swap
				if (!tableName.toLowerCase().equals(tableName2.toLowerCase())) {
					String temp = fieldName1;
					fieldName1 = fieldName2;
					fieldName2 = temp;
				}
				
				int fieldIndex1 = joinRel.getDesc().nameToId(fieldName1);
				int fieldIndex2 = newJoinRel.getDesc().nameToId(fieldName2);
				joinRel = joinRel.join(newJoinRel, fieldIndex1, fieldIndex2);
			}
		}

		// where
		Relation whereRel = joinRel;
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		if (sb.getWhere() != null) {
			sb.getWhere().accept(whereVisitor);
			whereRel = joinRel.select(joinRel.getDesc().nameToId(
					whereVisitor.getLeft()),
					whereVisitor.getOp(),
					whereVisitor.getRight());
		}

		// select
		Relation selectRel = whereRel;
		List<SelectItem> selectList = sb.getSelectItems();

		ArrayList<Integer> projectFields = new ArrayList<Integer>();

		for (SelectItem item : selectList) {
			item.accept(colVisitor);
			
			String selectCol = colVisitor.isAggregate() ?  colVisitor.getColumn() : item.toString(); 

			if (selectCol.equals("*")){
				for (int i = 0; i < whereRel.getDesc().numFields(); i++) {
					projectFields.add(i);
				}
				break;
			} 
			int field = selectCol.equals("*") && colVisitor.isAggregate() ?
						0 : whereRel.getDesc().nameToId(selectCol);
			if (!projectFields.contains(field)) projectFields.add(field);
		}
		selectRel = whereRel.project(projectFields);

		// aggregate
		boolean groupByFlag = sb.getGroupByColumnReferences() != null;
		
		Relation aggreated = colVisitor.isAggregate() ? 
				selectRel.aggregate(colVisitor.getOp(), groupByFlag) : selectRel;

		return aggreated;
	}
}