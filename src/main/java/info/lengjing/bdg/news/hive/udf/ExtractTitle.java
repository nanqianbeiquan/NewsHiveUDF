package info.lengjing.bdg.news.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import cn.edu.hfut.dmic.contentextractor.ContentExtractor;

public class ExtractTitle extends UDF {
	public String evaluate(String title, String html){

		if(!title.endsWith("...")){
			return title;
		} else {
			try {
				return ContentExtractor.getNewsByHtml(html).getTitle();
			} catch (Exception e) {
				return title;
			}
		}
	}
}
