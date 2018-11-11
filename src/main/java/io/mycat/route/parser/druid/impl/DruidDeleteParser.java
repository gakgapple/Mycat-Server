package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

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
	public String getValue(SQLExpr left , SQLExpr right , String value) throws SQLSyntaxErrorException {

		//先分析right 如果right不包含 分区列，则递归left

		//而且分区列一定是SQLBinaryOpExpr 所以 SQLInList这种的都不考虑进去

		if (right instanceof SQLBinaryOpExpr) {
			//获取right
			SQLBinaryOpExpr rightNow = (SQLBinaryOpExpr) right;
			//获取获取key
			SQLIdentifierExpr leftInRight = (SQLIdentifierExpr) rightNow.getLeft();
			String rightValue = ((SQLIdentifierExpr) leftInRight).getName().toUpperCase();
			//如果key=关键列则返回key对应的value
			if (rightValue.equals(value)) {
				return rightNow.getRight().toString();
			}
			//再去查下left是否符合条件，符合条件的话直接返回。
		} else if (left instanceof SQLIdentifierExpr) {

			String leftValue = ((SQLIdentifierExpr) left).getName().toUpperCase();
			if (leftValue.equals(value)) {

				return right.toString();
			} else {
				return "";
			}

		}

		if (left instanceof SQLBinaryOpExpr) {
			SQLBinaryOpExpr leftNow = (SQLBinaryOpExpr) left;
			return this.getValue(leftNow.getLeft(), leftNow.getRight(), value);
		} else {
			String msg = "分区列格式不正确 ";
			LOGGER.warn(msg);
			throw new SQLSyntaxErrorException(msg);
		}

	}


}

