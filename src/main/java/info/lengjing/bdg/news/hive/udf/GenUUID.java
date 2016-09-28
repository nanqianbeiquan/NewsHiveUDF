package info.lengjing.bdg.news.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.UUID;

public class GenUUID extends UDF {
	public String evaluate(String url){
		return UUID.randomUUID().toString();
	}
}
