package info.lengjing.bdg.news.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import cn.edu.hfut.dmic.contentextractor.ContentExtractor;

@Description(name="extractContent", value="_FUNC_(content) - extract content from raw html.")
public class ExtractContent extends UDF {
	public String evaluate(String html){
		StringBuffer buf = new StringBuffer();
		try {
			Elements es = ContentExtractor.getContentElementByHtml(html).select("p");
			for(Element e: es){
				buf.append(e.outerHtml());
			}
			return buf.toString();
		} catch (Exception e) {
			return html;
		}
	}
}
