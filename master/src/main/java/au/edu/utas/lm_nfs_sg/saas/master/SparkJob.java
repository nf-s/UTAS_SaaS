package au.edu.utas.lm_nfs_sg.saas.master;

import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SparkJob extends Job {
	SparkJob(String jobId) {
		super(jobId);

		setJobImageId("26e87817-068b-4221-85a6-e5658aaa12a3");
		setJobRequiresOwnWorker(true);
	}

	@Override
	void setJobParamsJsonString(String params) {
		super.setJobParamsJsonString(params);

		try {
			Document document = new Document();
			Element root = new Element("operation");

			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(params);

			for (Map.Entry<String, String> entry : ((HashMap<String, String>)obj).entrySet())
			{
				Element input = new Element("input");
				input.setAttribute(new Attribute("globalname", entry.getKey()));
				input.addContent(entry.getValue());
				root.addContent(input);
			}

			document.setContent(root);

			FileWriter writer = new FileWriter(getJobParamsDirectory()+"/job_params.xml");
			XMLOutputter outputter = new XMLOutputter();
			outputter.setFormat(Format.getPrettyFormat());
			outputter.output(document, writer);
			writer.close(); // close writer

		} catch (ParseException |IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	JSONObject processJobResourcesDir() {
		for (File file : getJobResourcesDirectory().listFiles()) {
			if (Objects.equals(FilenameUtils.getExtension(file.getName()), "xml")) {
				try {
					SAXBuilder saxBuilder = new SAXBuilder();
					Document document = null;

					System.out.println("send XML");
					document = saxBuilder.build(file);
					Element classElement = document.getRootElement();

					if (classElement.getName().equals("operation")) {

						List<Element> list = classElement.getChildren();

						JSONObject obj = new JSONObject();

						for (Element e : list) {
							obj.put(e.getAttribute("globalname").getValue(), e.getValue());
						}

						File newFilename = new File(getJobParamsDirectory() + "/" + file.getName());

						if (newFilename.exists() && !newFilename.isDirectory()) {
							newFilename.delete();
						} else {
							file.renameTo(newFilename);
						}


						return obj;
					}
				} catch (JDOMException | IOException e) {
					e.printStackTrace();
				}
			} else if (Objects.equals(FilenameUtils.getExtension(file.getName()), "json")) {

			}
		}
		return null;
	}
}
