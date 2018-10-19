package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.util.StringUtil;

public class DruidDeleteParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlDeleteStatement delete = (MySqlDeleteStatement)stmt;
		String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
		ctx.addTable(tableName);
		/*
		为分片的插入查询条件
		 */
		if(schema.getAllDataNodes().size() > 1)
		{
			TableConfig tc = schema.getTables().get(tableName);
			String partitionColumn = tc.getPartitionColumn();		SQLBinaryOpExpr where = (SQLBinaryOpExpr) delete.getWhere();

			String shardingValue =  this.getValue(where.getLeft(), where.getRight(), partitionColumn);

			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			routeCalculateUnit.addShardingExpr(tableName, partitionColumn, shardingValue);
			ctx.addRouteCalculateUnit(routeCalculateUnit);
		}
	}


	//因为mycat分片delete只支持mysql,所以重写visitorParse，覆盖成空方法，然后通过statementPparse去解析
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException {

	}
	/**
	 * 获取分片值
	 * @param left
	 * @param right
	 * @param value
	 * @return
	 */
	public String getValue(SQLExpr left , SQLExpr right , String value) {

		String  shardingValue = "";
		if(left instanceof  SQLBinaryOpExpr && right instanceof  SQLBinaryOpExpr)
		{
			SQLBinaryOpExpr leftNow = (SQLBinaryOpExpr) left;

			SQLBinaryOpExpr rightNow = (SQLBinaryOpExpr) right;

			SQLIdentifierExpr leftInRight = (SQLIdentifierExpr) rightNow.getLeft();
			String leftValue = leftInRight.getName().toUpperCase();
			if(leftValue.equals(value))
			{

				return  rightNow.getRight().toString();
			}
			else{
				shardingValue =  this.getValue(leftNow.getLeft(),leftNow.getRight(),value);
			}
		}
		if(left instanceof  SQLIdentifierExpr )
		{


			String leftValue = ((SQLIdentifierExpr) left).getName().toUpperCase();
			if(leftValue.equals(value))
			{

				return right.toString();
			}
			else
			{
				return "";
			}

		}
		return shardingValue;

	}


}

