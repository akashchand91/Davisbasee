package common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import Main.ReadColumn;

public class ResultSet {

	ArrayList<String> result;
	ArrayList<String> restructuredResult;
	ArrayList<Integer> recordPointers;
	public ArrayList<String> getResult() {
		return result;
	}
	public void setResult(ArrayList<String> result) {
		this.result = result;
	}
	public ArrayList<Integer> getRecordPointers() {
		return recordPointers;
	}
	public void setRecordPointers(ArrayList<Integer> recordPointers2) {
		this.recordPointers = recordPointers2;
	}
	public void displayResult(String selectCols, ArrayList<ReadColumn> colDTypeList) {
		restructure(selectCols, colDTypeList);
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		String header[] =selectCols.split(",");
		formatter.format("%30s%20s", "RowID", "|");
		for(String colHeader : header) {
			formatter.format("%5s%20s", colHeader.trim(), "|");
		}
		System.out.println(sb);
		sb= new StringBuilder();
		formatter = new Formatter(sb);
		for(String row:restructuredResult) {
			String[] cols = row.split(" ");
			for(String x : cols) {
				formatter.format("%5s%20s", x.trim(), "|");
			}
			System.out.println(sb);
			sb= new StringBuilder();
			formatter = new Formatter(sb);
		}
	}
	public void restructure(String selectCols,ArrayList<ReadColumn> colDTypeList) {
	
		restructuredResult = new ArrayList<>();
		ArrayList<Integer> indices = new ArrayList<>();
		String header[] =selectCols.split(",");
		for (String col : header  ) {
			for(ReadColumn rc : colDTypeList) {
				if(rc.getColumnName().equals(col.trim())) {
					indices.add(colDTypeList.indexOf(rc)+1);
				}
			}
		}
		int min = Collections.min(indices);
		
		for(String row:result) {
			String temp[] = row.trim().split(" ");
			ArrayList <String>splitRow = new ArrayList<>();
			for(String x:temp) {
				if(!x.trim().equals(""))
					splitRow.add(x);
			}
			String newRow = new String();
			for(int index : indices) {
				index=index-min+1;
				newRow=newRow+splitRow.get(index).trim()+" ";
			}
			newRow = splitRow.get(0)+" "+newRow;
			restructuredResult.add(newRow);
		}
	}
}
