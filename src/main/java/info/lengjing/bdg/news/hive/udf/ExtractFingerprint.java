package info.lengjing.bdg.news.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import org.nlpcn.commons.lang.finger.SimHashService;
import org.nlpcn.commons.lang.finger.FingerprintService;

@Description(name="extractFingerprint", value="_FUNC_(content) - extract fingerprint from raw html.")
public class ExtractFingerprint extends UDF {
	public String evaluate(String html){
		FingerprintService fps = new FingerprintService();
		try {
			String content = ContentExtractor.getContentByHtml(html);
			return fps.fingerprint(content);
		} catch (Exception e) {
			return fps.fingerprint(html);
		}
	}
}
