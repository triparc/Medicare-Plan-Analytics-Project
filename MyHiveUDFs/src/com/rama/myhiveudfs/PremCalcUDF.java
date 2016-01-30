package com.rama.myhiveudfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class PremCalcUDF extends UDF {
	private DoubleWritable result = new DoubleWritable(0);
	public PremCalcUDF(){
	result = new DoubleWritable(0);
	}
	public DoubleWritable evaluate(Text input){
	    String name = input.toString();
	    String part1 = "";
	    String part2 = "";
	    String part3 ="";
	    double mainprem = 0;
	    double partBprem = 0;
	    		    
	    // ignoring null or empty or no payment input
	    if (name == null || name =="" || name.isEmpty()|| name =="No." || name == "<b>You pay nothing</b>") {
	    	result = new DoubleWritable(0);
	    	}
	    else if (StringUtils.containsIgnoreCase(name, "<b>$")){
	    				part1 = StringUtils.substringBetween(name, "<b>$","</b>");
	    				part2 = StringUtils.substringAfter(name, "</b>");
	    				if (StringUtils.contains(part1, " ")|| StringUtils.contains(part1, "-")) mainprem = 0;
	    				else mainprem = Double.parseDouble(part1);
	    				if (StringUtils.containsIgnoreCase(part2, "Part B")){
	    					partBprem = 104.90; //In 2015 the monthly Part B Standard Premium is <b>$104.90</b>
	    					part2 = StringUtils.substringAfter(part2, "Part B");
	    				}
	    				if(StringUtils.containsIgnoreCase(part2, "<b>$")){
	    					part3 = StringUtils.substringBetween(name, "<b>$","</b>");
	    					if (StringUtils.contains(part3, " ")|| StringUtils.contains(part3, "-")) part3 = ""; 
	    					mainprem = mainprem + Double.parseDouble(part3);
	    					}
	    				if(StringUtils.containsIgnoreCase(part2, "year"))result = new DoubleWritable(mainprem/12+ partBprem);
	    				else result = new DoubleWritable(mainprem + partBprem);
	    	}
	    else result = new DoubleWritable(0);
	    return (result);
	    }
}
	  