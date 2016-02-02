/**
 * 
 */
package org.athento.nuxeo.operations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

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
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.SimplePrincipal;
import org.nuxeo.ecm.core.api.Sorter;
import org.nuxeo.ecm.core.api.impl.blob.FileBlob;
import org.nuxeo.ecm.core.api.impl.blob.InputStreamBlob;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.core.api.tree.DefaultDocumentTreeSorter;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventProducer;
import org.nuxeo.ecm.core.event.impl.UnboundEventContext;
import org.nuxeo.ecm.core.lifecycle.LifeCycleService;
import org.nuxeo.ecm.core.storage.StorageBlob;
import org.nuxeo.ecm.csv.CSVImportLog;
import org.nuxeo.ecm.csv.CSVImportStatus;
import org.nuxeo.ecm.csv.CSVImporter;
import org.nuxeo.ecm.csv.CSVImporterImpl;
import org.nuxeo.ecm.csv.CSVImporterOptions;
import org.nuxeo.ecm.user.invite.AlreadyProcessedRegistrationException;
import org.nuxeo.runtime.api.Framework;

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
		boolean sessionMustBeClosed = false;
		JSONArray array = new JSONArray();
		JSONArray includedFiles = new JSONArray();
		JSONArray excludedFiles = new JSONArray();

		PathRef pref = new PathRef(folderToCheck);
		PathRef prefDst = new PathRef(folderToPut);
		if (_log.isInfoEnabled()) {
			_log.info("=================================");
			_log.info("Importing CSV Files from Ref: " + pref);
			_log.info("Importing CSV Files to   Ref: " + prefDst);
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
			registerEvent("beginProcess","Beginning CSV import files from " 
				+ pref + " to " + prefDst, session.getPrincipal());
			DocumentModel folder = session.getDocument(pref);
			if (folder == null) {
				throw new OperationException(
						"Null folder received. Folder to check is: "
								+ folderToCheck);
			}
			if (!folder.isFolder()) {
				throw new OperationException(
						"The document received is NOT folderish");
			}
			DocumentModel folderDestiny = session.getDocument(prefDst);
			if (folderDestiny == null) {
				throw new OperationException(
						"Null folder received. Folder to put is: "
								+ folderToPut);
			}

			String basePath = folder.getPathAsString();
			DocumentModel folderProcessing = getOrCreateFolder(basePath,
					"processing");
			DocumentModel folderProcessed = getOrCreateFolder(basePath,
					"processed");
			DocumentModel folderProcessedOk = getOrCreateFolder(
					folderProcessed.getPathAsString(), "processed_ok");
			DocumentModel folderProcessedKo = getOrCreateFolder(
					folderProcessed.getPathAsString(), "processed_with_errors");

			CSVImporterOptions options = CSVImporterOptions.DEFAULT_OPTIONS;
			CSVImporter csvImporter = new CSVImporterImpl();
			DocumentModelList children = session.getChildren(folder.getRef());
			List<DocumentModel> processingChildren = new ArrayList<DocumentModel>();
			if (!children.isEmpty()) {
				if (_log.isInfoEnabled()) {
					_log.info("=== Folder [" + folder + "] has: " + children.size()
						+ " children. Moving files to processing folder...");
				}
				for (DocumentModel child : children) {
					List<String> errors = new ArrayList<String>();
					try {
						
						if ("deleted".equals(child.getCurrentLifeCycleState())) {
							if (_log.isDebugEnabled()) {
								_log.debug("... ignoring deleted file: "
									+ child.getPathAsString());
							}
						}else if (child.isFolder()) {
							if (_log.isDebugEnabled()) {
								_log.debug("... ignoring folder: "
									+ child.getPathAsString());
							}
						} else {
							if (_log.isInfoEnabled()) {
								_log.info(" > Moving: "
										+ child.getPathAsString());
							}
							registerEvent(
								"fileReady","File prepared for processing: " 
									+ child, session.getPrincipal());
							DocumentModel copy = session.move(child.getRef(),
									folderProcessing.getRef(), null);
							if (_log.isInfoEnabled()) {
								_log.info(" \\__ moved to: "
										+ copy.getPathAsString());
							}
							if (processingChildren.add(copy)) {
								if (_log.isDebugEnabled()) {
									_log.debug("  + new file to process: "
										+ copy);
								}
							} else {
								_log.error("Document will not be processed: " + copy);
							}
						}
					} catch (PropertyNotFoundException ex) {
						errors.add(ex.getMessage());
						JSONArray jsonErrors = new JSONArray();
						jsonErrors.addAll(errors);
						JSONObject object = new JSONObject();
						object.put(child.getPathAsString(), jsonErrors);
						excludedFiles.add(object);
					}
					if (!errors.isEmpty()) {
						_log.warn("-- Some errors (" + errors.size()
								+ ") found for child: "
								+ child.getPathAsString());
					}

				}
				if (_log.isInfoEnabled()) {
					_log.info("--- " + processingChildren.size()+ " files moved");
				}
				processFiles(
					processingChildren,
					folderDestiny, folderProcessedOk, folderProcessedKo, 
					csvImporter, options, includedFiles);
			} else {
				if (_log.isDebugEnabled()) {
					_log.debug("-- the folder is empty. Nothing to do.");
				}
			}
		} catch (Exception e) {
			_log.error("Unexpected exception", e);
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
		obj.put("includedDocuments", includedFiles);
		obj.put("excludedDocuments", excludedFiles);
		array.add(obj);
		if (_log.isDebugEnabled()) {
			_log.debug("Preparing response: " + array);
		}
		registerEvent("endProcess","Ending CSV import files from " 
			+ pref + " to " + prefDst, session.getPrincipal());

		return new InputStreamBlob(new ByteArrayInputStream(array.toString()
				.getBytes("UTF-8")), "application/json");
	}

	private void processFiles(
		List<DocumentModel> processingChildren, 
		DocumentModel folderDestiny,
		DocumentModel folderProcessedOk, DocumentModel folderProcessedKo, 
		CSVImporter csvImporter, CSVImporterOptions options,
		JSONArray includedFiles) throws IOException, InterruptedException {
		if (_log.isDebugEnabled()) {
			_log.debug("=== importing " + processingChildren.size() + " CSV files");
		}
		DefaultDocumentTreeSorter sorter = new DefaultDocumentTreeSorter();
		sorter.setSortPropertyPath("dc:created");
		Collections.sort(processingChildren, sorter);
		JSONObject object = new JSONObject();
		for (DocumentModel processingChild: processingChildren){
			int total = 0;
			long estimatedTime = 0;
			List<String> errors = new ArrayList<String>();
			StorageBlob childContent = (StorageBlob) processingChild
				.getPropertyValue(ImportCSVFiles.XPATH_FILE_CONTENT);
			if (childContent == null) {
				errors.add("No file:content found in: " + processingChild);
			} else {
				long startTime = System.nanoTime();
				FileBlob fb = new FileBlob(childContent.getStream());
				if (_log.isDebugEnabled()) {
					_log.debug(" |+| importing CSV data from: "
							+ processingChild.getPathAsString());
				}
				registerEvent(
					"CSVparse_begin","File: " 
						+ processingChild, session.getPrincipal());
				if (_log.isInfoEnabled()) {
					_log.info("  >> importing CSV: "
							+ processingChild.getPathAsString());
				}
	
				String importId = csvImporter.launchImport(session,
					folderDestiny.getPathAsString(),
					fb.getFile(), processingChild.getPathAsString(),
					options);
	
				waitForImport(importId, csvImporter, processingChild);

				estimatedTime = System.nanoTime() - startTime;
	
				List<CSVImportLog> importLogs = csvImporter
					.getImportLogs(importId);
				if (_log.isDebugEnabled()) {
					_log.debug("   importLogs size: "
						+ importLogs.size());
				}
				for (CSVImportLog log : importLogs) {
					if (_log.isDebugEnabled()) {
						_log.debug("    > log: " + log.getMessage());
					}
					if (log.isSuccess()) {
						total = total + 1;
					} else if (log.isError()) {
						_log.error("Document load failed: "
								+ log.getMessage());
						errors.add(log.getMessage());
					} else if (log.isSkipped()) {
						_log.warn("Document skipped: "
								+ log.getMessage());
						errors.add(log.getMessage());
					}
				}
			}
			DocumentModel folderToMove = null;
			JSONArray jsonErrors = new JSONArray();
			if (!jsonErrors.addAll(errors)) {
				object.put(processingChild.getPathAsString(), "" + total
						+ " documents loaded with no errors");
				folderToMove = folderProcessedOk;
			} else {
				object.put(processingChild.getPathAsString(), "" + total
						+ " documents loaded with errors: "
						+ jsonErrors);
				folderToMove = folderProcessedKo;
			}
			if (_log.isDebugEnabled()) {
				_log.debug(" --- Loaded data for child: "
						+ object);
			}
			includedFiles.add(object);
			if (_log.isInfoEnabled()) {
				_log.info(" < Ending. Moving from: "
						+ processingChild.getPathAsString() 
						+ " to " + folderDestiny);
			}
			DocumentModel copy2 = session.move(processingChild.getRef(),
					folderToMove.getRef(), null);
			registerEvent(
				"CSVparse_end","File: " 
					+ copy2.getRef() + " time spent: " + estimatedTime + " ns", session.getPrincipal());
			if (_log.isInfoEnabled()) {
				_log.info(" [ok] moved to: "
						+ copy2.getPathAsString());
			}
		}
		if (_log.isDebugEnabled()) {
			_log.debug("=== importing done");
		}
	}

	private DocumentModel getOrCreateFolder(String basePath, String folderName) {
		if (_log.isDebugEnabled()) {
			_log.debug("Searching document: " + folderName);
			_log.debug(" in folder: " + basePath);
		}
		PathRef pref = new PathRef(basePath + "/" + folderName);
		DocumentModel folder = null;
		try {
			folder = session.getDocument(pref);
		} catch (Exception e) {
			if (_log.isDebugEnabled()) {
				_log.debug("NOT FOUND! " + folderName + " in " + basePath);
			}
			_log.info(" => creating folder: " + pref);
			folder = session
					.createDocumentModel(basePath, folderName, "Folder");
			folder.setPropertyValue("dc:title", folderName);
			folder = session.createDocument(folder);
		}
		return folder;
	}

	private int readIntegerProperty(String property) {
		String prop = Framework.getProperty(property);
		int value = 0;
		if (prop != null) {
			try {
				value = Integer.parseInt(prop);
			} catch (NumberFormatException e) {
				_log.error("Unable to parse [" + property
						+ "] value to integer: " + prop);
			}
		}
		return value;
	}

	private boolean registerEvent(String eventId, String comment, Principal principal) {
		LoginContext loginContext = null;
		String category = "eventWorkflowCategory";
		if (_log.isDebugEnabled()) {
			_log.debug("** Registering event: " + eventId);
			_log.debug("          comment: " + comment);
		}

		try {
			try {
				loginContext = Framework.login();
			} catch (LoginException e) {
				_log.error("Unable to log in in order to log Login event"
						+ e.getMessage());
				return false;
			}

			EventProducer evtProducer = null;
			try {
				evtProducer = Framework.getService(EventProducer.class);
			} catch (Exception e) {
				_log.error("Unable to get Event producer: " + e.getMessage());
				return false;
			}
			Map<String, Serializable> props = new HashMap<String, Serializable>();
			props.put("category", category);
			props.put("comment", comment);

			EventContext ctx = new UnboundEventContext(session, principal, props);
			try {
				evtProducer.fireEvent(ctx.newEvent(eventId));
			} catch (ClientException e) {
				_log.error("Unable to send authentication event", e);
			}
			return true;
		} finally {
			if (loginContext != null) {
				try {
					loginContext.logout();
				} catch (LoginException e) {
					_log.error("Unable to logout: " + e.getMessage());
				}
			}
		}

	}

	private void waitForImport(
		String importId, CSVImporter csvImporter, DocumentModel processingChild) {
		CSVImportStatus status = csvImporter.getImportStatus(importId);
		while (status != null && !status.isComplete()) {
			if (_log.isDebugEnabled()) {
				_log.debug("   waiting for [" + processingChild.getPathAsString() +"] CSVImportId [" + importId + "] status: " + status.getState());
			}
			try {
				Thread.currentThread().sleep(500);
			} catch (InterruptedException e) {
				_log.error("unable to sleep for 500 ms");
			}
			status = csvImporter.getImportStatus(importId);
		}
		if (_log.isInfoEnabled()) {
			_log.info("   << CSV import done with id [" + importId
				+ "] status: " + (status!=null?status.getState():" none"));
		}
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
