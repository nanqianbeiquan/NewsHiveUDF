package info.lengjing.bdg.news.hive.udf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NewsClassifier extends UDF {

	private static Map<String, String> classifiers = new HashMap<String, String>();
	static {
		try {
			LineNumberReader lr;
			lr = new LineNumberReader(new InputStreamReader(NewsClassifier.class.getResourceAsStream("/cls.txt"), "UTF-8"));
			String l = "";
			while((l = lr.readLine()) != null){
				String[] ts = l.split("\t");
				String cls = ts[0].trim();
				String[] fs = ts[1].split(" ");
				for(int i = 0; i < fs.length; i++){
					classifiers.put(fs[i], cls);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static List<String> classify(String title, String content){
		List<String> cls = new ArrayList<String>();
		for(Map.Entry<String, String> c: classifiers.entrySet()){
			if(title.contains(c.getKey()))
				cls.add(c.getValue());
		}
		if(cls.size() > 0) 
			return cls;
		else
			return null;
	}
	public String evaluate(String title, String content){
		List<String> cls = classify(title, content);
		if(cls != null){
			return cls.get(0);
		} else 
			return null;
	}
	public static void main(String[] args) {
		//System.out.println(new NewsClassifier().evaluate("滴滴打车获得A轮融资", "滴滴打车获得A轮融资"));
	}

}
