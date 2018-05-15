package Main;

public class ReadColumn {
	private String columnName;
	private String colData;
	private String dataType;
	private byte code;
	private String isPrimaryKey;
	private String isNotNullable;
	private boolean isSelected;
	private boolean isWhere;
	int selectIndex;
	public int getSelectIndex() {
		return selectIndex;
	}
	public void setSelectIndex(int selectIndex) {
		this.selectIndex = selectIndex;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColData() {
		return colData;
	}
	public void setColData(String colData) {
		this.colData = colData;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public byte getCode() {
		return code;
	}
	public void setCode(byte code) {
		this.code = code;
	}
	public String getIsPrimaryKey() {
		return isPrimaryKey;
	}
	public void setIsPrimaryKey(String isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}
	public String getIsNotNullable() {
		return isNotNullable;
	}
	public void setIsNotNullable(String isNotNullable) {
		this.isNotNullable = isNotNullable;
	}
	public boolean getIsSelected() {
		return isSelected;
	}
	public void setIsSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}
	public boolean isWhere() {
		return isWhere;
	}
	public void setWhere(boolean isWhere) {
		this.isWhere = isWhere;
	}
	
}
