# Athento Nx CSV Import Scheduler

#Synopsis

This plugin reads special CSV files (https://doc.nuxeo.com/display/NXDOC/Nuxeo+CSV) from path /default/default-domain/workspaces/CSV/src and import Document contained in /default/default-domain/workspaces/CSV/dst every 1 hour.

Also provides an operation to invoke it manually from REST client:

http://localhost:8080/nuxeo/site/automation/Athento.ImportCSVFiles
```json
{
  "params": {
      "folderToCheck":"/default-domain/workspaces/CSV/src",
      "folderToPut":"/default-domain/workspaces/CSV/dst"
  }
} 
```
Some example documents provided in https://github.com/nuxeo/nuxeo-csv/tree/master/src/test/resources

#Installation

You just have to compile the pom.xml using Maven and deploy the plugin in. To do this, you must use the following script:

	cd athento-nx-csv-import-scheduler
	mvn clean install
	cp target/athento-nx-csv-import-scheduler*.jar $NUXEO_HOME/nxserver/plugins

Restart your nuxeo server and enjoy.

#Release notes
1.0 - contributes a lifecycle-state "processed" for File documents using transition from "project" to "processed" to avoid duplicate processing.
2.0 - changes avoid duplicates policy. Now moves documents from src folder to processing subfolder and, when processed, to processed subfolder.

#To Do
- [done] Extend automation chain to move processed files to a diferent folder to avoid reprocessing.

