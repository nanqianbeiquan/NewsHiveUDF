package info.lengjing.bdg.news.hive.udf;

import java.io.IOException;
import java.io.InputStreamReader;
import org.nlpcn.commons.lang.tire.SmartGetWord;
import org.nlpcn.commons.lang.tire.domain.SmartForest;
import java.io.LineNumberReader;

public class CompanyNameMap {
	static private SmartForest<String> forest = new SmartForest<String>();
	static {
		try {
			LineNumberReader lr;
			lr = new LineNumberReader(new InputStreamReader(CompanyNameMap.class.getResourceAsStream("/companyname.txt"), "UTF-8"));
			String l = "";
			while((l = lr.readLine()) != null){
				String[] ts = l.split("\t");
				String name = ts[0].trim();
				forest.add(name, name);
				//System.out.println(name + " " + ts);
				for(int i = 1; i < ts.length; i++){
					forest.add(ts[i].trim(), name);
					
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static String getFullCompanyName(String name){
		SmartGetWord<String> w = forest.getWord(name);
		String temp = null;
		String companyName = null;
		if((temp = w.getFrontWords()) != null){
			companyName = w.getParam();
		}
		return companyName;
	}
	
	public static void main(String[] args){
		/*
		System.out.println(CompanyNameMap.getFullCompanyName("吉祥航空"));
		System.out.println(CompanyNameMap.getFullCompanyName("读者传媒"));
		System.out.println(CompanyNameMap.getFullCompanyName("读者传"));
		*/
	}
	
}
