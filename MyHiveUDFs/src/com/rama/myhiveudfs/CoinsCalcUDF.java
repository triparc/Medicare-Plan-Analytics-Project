package com.rama.myhiveudfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class CoinsCalcUDF extends UDF {
	private DoubleWritable result = new DoubleWritable(0);
	public CoinsCalcUDF(){
	result = new DoubleWritable(0);
	}
	public DoubleWritable evaluate(Text input){
	    String name = input.toString();
	    String part1 = "";
	    double mainprem = 0;
	    		    
	    // ignoring null or empty input
	    if (name == null || name =="" || name.isEmpty()|| name =="No." || name == "<b>You pay nothing</b>") {
	    	result = new DoubleWritable(0);
	    	}
	    else if(StringUtils.containsIgnoreCase(name, "<b>%")){
	    		part1 = StringUtils.substringBetween(name, "<b>%","</b>");
	    		if (StringUtils.contains(part1, " ")|| StringUtils.contains(part1, "-")) mainprem = 0;
	    		else mainprem = Double.parseDouble(part1);
	    		result = new DoubleWritable(mainprem);
	    	 }
	    	else {
	    		result = new DoubleWritable(0);
	    		}
	    return (result);
	    }
}