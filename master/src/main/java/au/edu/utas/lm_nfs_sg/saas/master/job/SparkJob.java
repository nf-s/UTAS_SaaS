package au.edu.utas.lm_nfs_sg.saas.master.job;

import au.edu.lm_nf_sg.saas.common.job.JobType;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FilenameUtils;

import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class SparkJob extends Job {
	static {
		jobClassStringMap.put(SparkJob.class.toString(), "CSIRO Spark");
	}

	public SparkJob(String jobId) {
		super(jobId);

		setWorkerType(WorkerType.PRIVATE);
		setJobType(JobType.BOUNDED);
	}

	@Override
	public void updateConfigFromJsonString(String config) {
		super.updateConfigFromJsonString(config);

		// Create Spark XML File
		try {
			Document document = new Document();
			Element root = new Element("operation");

			JsonParser parser = new JsonParser();
			JsonObject obj = (JsonObject) parser.parse(config);

			obj.entrySet().forEach(entry -> {
				Element input = new Element("input");
				input.setAttribute(new Attribute("globalname", entry.getKey()));
				input.addContent(entry.getValue().getAsString());
				root.addContent(input);
			});

			document.setContent(root);

			String fileName = getConfigDirectory()+java.io.File.separator +"job_config.xml";

			FileWriter writer = new FileWriter(fileName);
			XMLOutputter outputter = new XMLOutputter();
			outputter.setFormat(Format.getPrettyFormat());
			outputter.output(document, writer);
			writer.close(); // close writer

			setConfigFile(Paths.get(fileName));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public JsonObject processNewUploadedFilesInResourcesDir() {
		JsonObject returnJsonObject = new JsonObject();
		// Go through job resources directory and see if any Spark XML files have been uploaded
		try {
			Files.list(getResourcesDirectory()).forEach(file-> {
				if (Objects.equals(FilenameUtils.getExtension(file.getFileName().toString()), "xml")) {
					try {
						SAXBuilder saxBuilder = new SAXBuilder();
						Document document = null;

						System.out.println("send XML");
						document = saxBuilder.build(file.toFile());
						Element classElement = document.getRootElement();

						// This is the test to see if the XML file is a Spark XML File
						// I.e. is the root tag <operation>
						if (classElement.getName().equals("operation")) {

							classElement.getChildren()
									.forEach(e->returnJsonObject
											.add(e.getAttribute("globalname").getValue(), new JsonPrimitive(e.getValue())));

							Path newFilename = Paths.get(getConfigDirectory() + "/" + file.getFileName().toString());

							Files.deleteIfExists(newFilename);
							Files.move(file, newFilename);
						}
					} catch (JDOMException | IOException e) {
						e.printStackTrace();
					}
				} else if (Objects.equals(FilenameUtils.getExtension(file.getFileName().toString()), "json")) {

				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return returnJsonObject;
	}


	@Override
	Long estimateExecutionTimeInMs(Flavor instanceFlavour) {
		switch (getDescription()) {
			case "small-test(94)":
				switch (instanceFlavour.getName()) {
					case "m2.small":
						return (long)107670;
					case "m2.medium":
						return (long)70640;
					case "m2.large":
						return (long)31580;
				}
				break;
			case "medium-test(Forcett)":
				switch (instanceFlavour.getName()) {
					case "m2.small":
						return (long)199180;
					case "m2.medium":
						return (long)132280;
					case "m2.large":
						return (long)54710;
				}
				break;
			case "large-test(Wangary)":
				switch (instanceFlavour.getName()) {
					case "m2.small":
						return (long)3760000;
					case "m2.medium":
						return (long)2345540;
					case "m2.large":
						return (long)948820;
				}
				break;
		}


		return super.estimateExecutionTimeInMs(instanceFlavour);
	}

}
