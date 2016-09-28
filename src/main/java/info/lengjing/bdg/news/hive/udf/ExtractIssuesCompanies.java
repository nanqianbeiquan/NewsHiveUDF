package info.lengjing.bdg.news.hive.udf;

import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.nlpcn.commons.lang.tire.GetWord;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.library.Library;
import org.nlpcn.commons.lang.util.StringUtil;

import cn.edu.hfut.dmic.contentextractor.ContentExtractor;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

@Description(name="extractIssuesCompanies", value="extract issues and companies")
public class ExtractIssuesCompanies extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private ListObjectInspector loi;
	private StructObjectInspector elOI;
	private StringObjectInspector titleOI;
	private StringObjectInspector contentOI;
	static {
		try {
			LineNumberReader lr = new LineNumberReader(new InputStreamReader(ExtractIssuesCompanies.class.getResourceAsStream("/company.dic"), "UTF-8"));
			String l = "";
			while((l = lr.readLine()) != null){
				String[] ts = l.split("\t");
				String keyword = ts[0].trim();
				String nature = ts[1].trim();
				int freq = Integer.parseInt(ts[2].trim());
				UserDefineLibrary.insertWord(keyword, nature, freq);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> extractCompanyNames(String title, String content){
		List<String> names = new ArrayList<String>();
		Map<String, Integer> nameCnt = new HashMap<String, Integer>();
		for(Term t: NlpAnalysis.parse(title)){
			if(t.getNatureStr().equals("nt")){
				String fullName = CompanyNameMap.getFullCompanyName(t.getName());
				if(fullName != null){
					if(nameCnt.containsKey(fullName)){
						nameCnt.put(fullName, nameCnt.get(fullName) + 1);
					} else {
						nameCnt.put(fullName, 1);
					}
				}
			}
		}
		for(Term t: NlpAnalysis.parse(content)){
			if(t.getNatureStr().equals("nt")){
				String fullName = CompanyNameMap.getFullCompanyName(t.getName());
				if(fullName != null){
					if(nameCnt.containsKey(fullName)){
						nameCnt.put(fullName, nameCnt.get(fullName) + 1);
					} else {
						nameCnt.put(fullName, 1);
					}
				}
			}
		}
		if(nameCnt.size() == 0) return null;
		String maxName = "";
		int maxCnt = 0;
		for(Map.Entry<String, Integer> e: nameCnt.entrySet()){
			if(e.getValue() > maxCnt){
				maxCnt = e.getValue();
				maxName = e.getKey();
			}
		}
		names.add(maxName);
		if(maxCnt > 3){
			return names;
		} else {
			return null;
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		argumentOIs = arguments;
		
		if(arguments.length != 2){
			throw new UDFArgumentException("2 arguments needed, found " + arguments.length);
		}
		titleOI = (StringObjectInspector)arguments[0];
		contentOI = (StringObjectInspector)arguments[1];
		List<String> structFieldNames = new ArrayList<String>();
		structFieldNames.add("companyid");
		structFieldNames.add("issuetype");
		structFieldNames.add("issueprob");
		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
		elOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
		return ObjectInspectorFactory.getStandardListObjectInspector(elOI);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(arguments.length != 2){
			return null;
		}
		try {
			String title = titleOI.getPrimitiveWritableObject(arguments[0].get()).toString();
			String rawContent = contentOI.getPrimitiveWritableObject(arguments[1].get()).toString();
			String content = "";
			try{
				content = ContentExtractor.getContentByHtml(rawContent);
			} catch (Exception e) {
				content = rawContent;
			}
			
			List<String> cls = NewsClassifier.classify(title, content);
			List<String> names = extractCompanyNames(title, content);
			if(cls == null || names == null) {
				return null;
			}
			int size = cls.size() * names.size();
			if(size == 0) return null;
			Object[][] r = new Object[size][3];
			int k = 0;
			for(int i = 0; i < names.size(); i++){
				for(int j = 0; j < cls.size(); j++){
					r[k][0] = new Text(names.get(i));
					r[k][1] = new Text(cls.get(j));
					r[k][2] = new DoubleWritable(1.0);
					k++;
				}
			}
			return r;
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return null;
	}

}
