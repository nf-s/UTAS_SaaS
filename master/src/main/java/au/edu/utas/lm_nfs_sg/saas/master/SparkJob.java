package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FilenameUtils;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SparkJob extends Job {
	static {
		jobClassStringMap.put(SparkJob.class.toString(), "CSIRO Spark");
	}

	SparkJob(String jobId) {
		super(jobId);

		setJobImageId("26e87817-068b-4221-85a6-e5658aaa12a3");
		setCanRunOnSharedWorker(false);
	}

	@Override
	void setJobConfigJsonString(String config) {
		super.setJobConfigJsonString(config);

		// Create Spark XML File
		try {
			Document document = new Document();
			Element root = new Element("operation");

			JsonParser parser = new JsonParser();
			JsonObject obj = (JsonObject) parser.parse(config);

			for (Map.Entry<String, JsonElement> entry : obj.entrySet())
			{
				Element input = new Element("input");
				input.setAttribute(new Attribute("globalname", entry.getKey()));
				input.addContent(entry.getValue().getAsString());
				root.addContent(input);
			}

			document.setContent(root);

			String fileName = getJobConfigDirectory()+java.io.File.separator +"job_config.xml";

			FileWriter writer = new FileWriter(fileName);
			XMLOutputter outputter = new XMLOutputter();
			outputter.setFormat(Format.getPrettyFormat());
			outputter.output(document, writer);
			writer.close(); // close writer

			setJobConfigFile(new File(fileName));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	JsonObject processJobResourcesDir() {
		// Go through job resources directory and see if any Spark XML files have been uploaded
		for (File file : getJobResourcesDirectory().listFiles()) {
			if (Objects.equals(FilenameUtils.getExtension(file.getName()), "xml")) {
				try {
					SAXBuilder saxBuilder = new SAXBuilder();
					Document document = null;

					System.out.println("send XML");
					document = saxBuilder.build(file);
					Element classElement = document.getRootElement();

					// This is the test to see if the XML file is a Spark XML File
					// I.e. is the root tag <operation>
					if (classElement.getName().equals("operation")) {

						List<Element> list = classElement.getChildren();

						JsonObject obj = new JsonObject();

						for (Element e : list) {
							obj.add(e.getAttribute("globalname").getValue(), new JsonPrimitive(e.getValue()));
						}

						File newFilename = new File(getJobConfigDirectory() + "/" + file.getName());

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
