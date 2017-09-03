package au.edu.utas.lm_nfs_sg.saas.master_rest;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

//https://www.geekmj.org/jersey/jax-rs-multiple-files-upload-example-408/
@Path("upload")
public class FileUpload {

	@Path("/file")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@DefaultValue("") @FormDataParam("tags") String tags,
							   @FormDataParam("file") InputStream file,
							   @FormDataParam("file") FormDataContentDisposition fileDisposition) {

		String fileName = fileDisposition.getFileName();

		saveFile(file, fileName);

		String fileDetails = " File uploaded successfully: " + fileName;

		System.out.println(fileDetails);

		return Response.ok(fileDetails).build();
	}

	@Path("/files")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFiles2(@DefaultValue("") @FormDataParam("tags") String tags,
								 @FormDataParam("files") List<FormDataBodyPart> bodyParts,
								 @FormDataParam("files") FormDataContentDisposition fileDispositions) {

		StringBuffer fileDetails = new StringBuffer("");

		/* Save multiple files */

		for (int i = 0; i < bodyParts.size(); i++) {
			/*
			 * Casting FormDataBodyPart to BodyPartEntity, which can give us
			 * InputStream for uploaded file
			 */
			BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyParts.get(i).getEntity();
			String fileName = bodyParts.get(i).getContentDisposition().getFileName();

			saveFile(bodyPartEntity.getInputStream(), fileName);

			fileDetails.append(" File uploaded successfully: ").append(fileName).append("<br/>");
		}

		System.out.println(fileDetails);

		return Response.ok(fileDetails.toString()).build();
	}

	private void saveFile(InputStream file, String name) {
		try {
			File dir = new File("temp");
			boolean directoryCreated = dir.mkdirs();
			/* Change directory path */
			java.nio.file.Path path = FileSystems.getDefault().getPath("temp/"+name);
			/* Save InputStream as file */
			Files.copy(file, path);
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}

}