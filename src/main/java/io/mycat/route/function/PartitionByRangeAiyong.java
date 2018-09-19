package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;

/**
 * 爱用分库分表的算法 首先由columns来确认分片id，然后根据分片id，在确定分库，最后确认是哪个库的哪张具体的表
 */
public class PartitionByRangeAiyong extends AbstractPartitionAlgorithm implements RuleAlgorithm {

	/**
	 * 配置文件位置和读取
	 */
	private String mapFile;
	private int count;
	private int subTablesCount;
	private LongRange[] longRanges;

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void setSubTablesCount(int subTablesCount) {
		this.subTablesCount = subTablesCount;
	}

	@Override
	public void init() {
		initialize();
	}

	/**
	 * 读取磁盘配置文件
	 */
	@SuppressWarnings("Duplicates")
	private void initialize() {
		BufferedReader br = null;
		try {
			//从本地磁盘上读取配置文件
			InputStream fin = this.getClass().getClassLoader().getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file " + mapFile);
			}
			br = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();
			for (String line = null; (line = br.readLine()) != null; ) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :" + line);
					continue;
				}
				String pairs[] = line.substring(0, ind).trim().split("-");
				long longStart = NumberParseUtil.parseLong(pairs[0].trim());
				long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
				int nodeId = Integer.parseInt(line.substring(ind + 1).trim());
				longRangeList.add(new LongRange(nodeId, longStart, longEnd));
			}
			longRanges = longRangeList.toArray(new LongRange[longRangeList.size()]);
		} catch (Exception e) {
			e.printStackTrace();
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (Exception ignore) {
			}
		}
	}

	/**
	 * 计算分库，到底在哪个分库上, 下来的问题是怎么看分表呢
	 * @param columnValue 分库字段的值
	 */
	@Override
	public Integer calculate(String columnValue) {
		Integer partitionValue = Integer.parseInt(columnValue);
		try {
			int nodeIndex = 0;
			for (LongRange longRang : this.longRanges) {
				if (partitionValue <= longRang.valueEnd && partitionValue >= longRang.valueStart) {
					return longRang.nodeId;
				}
			}
			//数据超过范围，暂时使用配置的默认节点
			return nodeIndex;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(String
				.format("columnValue: %s Please eliminate any quote and non number within it. ",
					columnValue), e);
		}
	}

	/**
	 * 返回分表数，返回null表示没有分表
	 * @param colValue 分表字段
	 * @return
	 */
	public Integer calculateTables(String colValue){
		int tableIndex = Integer.parseInt(colValue);
		if(tableIndex >= this.subTablesCount){
			throw new IllegalArgumentException(String
				.format("columnValue: %s 分表值超出分表范围. ",
					colValue));
		}else{
			return tableIndex;
		}
	}

	@Override
	public int getPartitionNum() {
		int nPartition = this.count;
		return nPartition;
	}


	static class LongRange {
		final int nodeId;
		final long valueStart;
		final long valueEnd;

		LongRange(int nodeId, long valueStart, long valueEnd) {
			super();
			this.nodeId = nodeId;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}
	}
}
