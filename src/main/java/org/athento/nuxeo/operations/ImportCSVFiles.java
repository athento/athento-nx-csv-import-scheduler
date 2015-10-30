/**
 * 
 */
package org.athento.nuxeo.operations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.impl.blob.FileBlob;
import org.nuxeo.ecm.core.api.impl.blob.InputStreamBlob;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.core.storage.StorageBlob;
import org.nuxeo.ecm.csv.CSVImportLog;
import org.nuxeo.ecm.csv.CSVImporter;
import org.nuxeo.ecm.csv.CSVImporterImpl;
import org.nuxeo.ecm.csv.CSVImporterOptions;

/**
 * @author athento
 *
 */

@Operation(id = ImportCSVFiles.ID, category = "Athento", label = "ImportCSVFiles", description = "Import CSV files specified in Folderish argument")
public class ImportCSVFiles {

	public static final String ID = "Athento.ImportCSVFiles";
	public static final String XPATH_FILE_CONTENT = "file:content";

	@OperationMethod
	public Blob run() throws Exception {
		PathRef pref = new PathRef(folderToCheck);
		boolean sessionMustBeClosed = false;
		JSONArray array = new JSONArray();
		JSONArray includedFiles = new JSONArray();
		JSONArray excludedFiles = new JSONArray();
		if (_log.isInfoEnabled()) {
			_log.info("Importing CSV Files from Ref: " + pref);
		}

		try {
			if (session == null) {
				String principal = "Administrator";
				if (_log.isDebugEnabled()) {
					_log.debug("Session is null. A new one must be opened for user: "
							+ principal);
				}
				session = CoreInstance.openCoreSession("default", principal);
				sessionMustBeClosed = true;
			}
			DocumentModel folder = session.getDocument(pref);
			if (folder == null) {
				throw new OperationException(
					"Null folder received. Folder to check is: " + folderToCheck);
			}
			if (!folder.isFolder()) {
				throw new OperationException(
					"The document received is NOT folderish");
			}
			DocumentModel folderDestiny = session.getDocument(new PathRef(folderToPut));
			if (folderDestiny == null) {
				throw new OperationException(
					"Null folder received. Folder to put is: " + folderToPut);
			}

			CSVImporterOptions options = CSVImporterOptions.DEFAULT_OPTIONS;
			CSVImporter csvImporter = new CSVImporterImpl();
			DocumentModelList children = session.getChildren(folder.getRef());
			if (!children.isEmpty()) {
				_log.debug("Folder [" + folder + "] has: " + children.size()
						+ " children");
				
				for (DocumentModel child : children) {
					List<String> errors = new ArrayList<String>();
					JSONObject object = new JSONObject();
					if (_log.isInfoEnabled()) {
						_log.info(" > Reading: " + child.getPathAsString());
					}
					try {
						StorageBlob childContent = (StorageBlob) child
							.getPropertyValue(ImportCSVFiles.XPATH_FILE_CONTENT);
						if (child.getName().contains("_.trashed")) {
							throw new PropertyNotFoundException("Ignoring trashed file: " 
								+ child.getName());
						}
						FileBlob fb = new FileBlob(childContent.getStream());
						if (_log.isDebugEnabled()) {
							_log.debug("   importing CSV data from: " 
								+ child.getPathAsString());
						}
						String importId = csvImporter.launchImport(session,
							folderDestiny.getPathAsString(), fb.getFile(),
							child.getPathAsString(), options);
						Thread.currentThread().sleep(3000);
						if (_log.isDebugEnabled()) {
							_log.debug("   ...done with id [" + importId 
								+ "] status: " 
								+ csvImporter.getImportStatus(importId));
						}
						List<CSVImportLog> importLogs = csvImporter
							.getImportLogs(importId);
						if (_log.isDebugEnabled()) {
							_log.debug("   importLogs size: " 
								+ importLogs.size());
						}
						int total = 0;
						for (CSVImportLog log: importLogs) {
							if (_log.isDebugEnabled()) {
								_log.debug("    > log: " + log.getMessage());
							}
							if (log.isSuccess()) {
								total = total + 1;
							} else if (log.isError()) {
								_log.error("Document load failed: " + log.getMessage());
								errors.add(log.getMessage());
							} else if (log.isSkipped()) {
								_log.warn("Document skipped: " + log.getMessage());
								errors.add(log.getMessage());
							}
						}
						JSONArray jsonErrors = new JSONArray();
						if (!jsonErrors.addAll(errors)) {
							object.put(child.getPathAsString(), "" + total 
								+ " documents loaded with no errors");
						} else {
							object.put(child.getPathAsString(), "" + total
								+ " documents loaded with errors: " + jsonErrors);
						}
						if (_log.isDebugEnabled()) {
							_log.debug("   Loaded data for child: " 
								+ object);
						}
						includedFiles.add(object);
					} catch(PropertyNotFoundException ex) {
						errors.add(ex.getMessage());
						JSONArray jsonErrors = new JSONArray();
						jsonErrors.addAll(errors);
						object.put(child.getPathAsString(), jsonErrors);
						_log.error("Ignoring invalid File: " + object + ": " 
								+ ex.getMessage());
						excludedFiles.add(object);
					}
					if (!errors.isEmpty()) {
						_log.warn("-- Some errors (" + errors.size() 
							+ ") found for child: " + child.getPathAsString());
					}
				}
			} else {
				if (_log.isDebugEnabled()) {
					_log.debug("-- the folder is empty. Nothing to do.");
				}
			}
		} catch (Exception e) {
			_log.error("Unexpected exception",e);
			throw e;
		} finally {
			if (sessionMustBeClosed) {
				if (_log.isDebugEnabled()) {
					_log.debug("Closing opened session");
				}
				CoreInstance.closeCoreSession(session);
			}
		}

		JSONObject obj = new JSONObject();
		obj.put("includedDocuments",includedFiles);
		obj.put("excludedDocuments",excludedFiles);
		array.add(obj);
		if (_log.isDebugEnabled()) {
			_log.debug("Preparing response: " + array);
		}
		return new InputStreamBlob(new ByteArrayInputStream(array.toString()
				.getBytes("UTF-8")), "application/json");
	}

	@Param(name = "folderToCheck", required = true)
	protected String folderToCheck;

	@Param(name = "folderToPut", required = true)
	protected String folderToPut;

	@Context
	protected CoreSession session;

	@Context
	protected OperationContext ctx;
	private static final Log _log = LogFactory.getLog(ImportCSVFiles.class);
}
