/*
 * TeradataLoaders.java Created on Oct 08, 2008.
 *
 * Copyright 2008 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionProperties;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This sample program performs the following
 *
 * 1. Create a mapping as follows
 *
 *    Teradata source --> DSQ --> Expr Transform --> Teradata target
 *
 *    Expr Transform finds average of two fields MAX_SALARY & MIN_SALARY
 *
 * 2. Create a session using this mapping and overrides the relational
 *    reader/writer and connection information to use teradata fast
 *    export reader and fastload writer
 *
 * 3. Create workflow using the session created in step 2.
 *
 *
 * @author nagarwal
 */
public class TeradataLoaders extends Base {

	private Mapping mapping = null;
	private Source jobTeradataSrc = null;
	private Target teradataTgt = null;

	/*
	 * Create a teradata source
	 */
	protected void createSources() {
		jobTeradataSrc = this.createTeradataJobSource("JobInfo_Teradata_Source");
		folder.addSource(jobTeradataSrc);
	}

	/*
	 * Create a teradata target
	 */
	protected void createTargets() {
		teradataTgt = this.createRelationalTarget(
				SourceTargetType.Teradata, "Teradata_target");
	}

	/*
	 * Create a mapping as follows:
	 *
	 *  Teradata Source --> DSQ --> Expression --> Teradata Target
	 */
	protected void createMappings() throws Exception {
		// create a mapping object
		mapping = new Mapping(
				"TeradataLoaders",
				"TeradataLoaders",
				"Mapping containing teradata sources, targets " +
				"and an expression transformation.");
		setMapFileName(mapping);

		// create transform helper
		TransformHelper helper = new TransformHelper(mapping);

		// create DSQ
		RowSet dsqRowSet = (RowSet) helper.sourceQualifier(this.jobTeradataSrc)
				.getRowSets().get(0);

		// create an expression transformation with a output port "AVG_SALARY".,
		// of type decimal, which provides average of MIN_SALARY and MAX_SALARY
		String expr = "decimal (10,0)AVG_SALARY = (MIN_SALARY+MAX_SALARY)/2";
		TransformField outField = new TransformField(expr);
		List<TransformField> fields = new ArrayList<TransformField>();
		fields.add(outField);
		RowSet expRS = (RowSet) helper.expression(dsqRowSet, fields,
				"exp_transform").getRowSets().get(0);

		// write to target
		mapping.writeTarget(expRS, this.teradataTgt);
		folder.addMapping(mapping);
	}

	/*
	 * This function performs the following:
	 * 1. Overrides the relation reader to use teradata fast export reader.
	 *    Override the connection type to application and use a fast export
	 *    connection. Override some connection attribute at session level
	 * 2. Overrides the relation writer to use file writer. Override the
	 *    connection type to external loader and use a teradata fastload
	 *    external loader. Override some connection attribute at session level
	 */
	private void overrideRelReaderWriter() {

		// set connection properties (session extension for teradata source)
		// to use teradata fast export reader
		ConnectionInfo srcObjConnInfo = this.jobTeradataSrc.getConnInfo();
		ConnectionProperties srcConnProps = srcObjConnInfo.getConnProps();
		srcConnProps.setProperty(
				ConnectionPropsConstants.SESSION_EXTENSION_NAME,
				"Teradata FastExport Reader");
		srcConnProps.setProperty(ConnectionPropsConstants.CONNECTIONTYPE,
				"Application");
		srcConnProps.setProperty(ConnectionPropsConstants.CONNECTIONSUBTYPE,
				"Teradata FastExport Connection");
		srcConnProps.setProperty(ConnectionPropsConstants.CONNECTIONNAME,
				"Teradata_FastExport_Con_test");

		// override the teradaa source connection properties at session level
		Properties fastExportConnSessOverrideAttr = srcObjConnInfo
				.getSessLevelOverrideConnAttributes();
		fastExportConnSessOverrideAttr.setProperty("TDPID", "TD_1234");
		fastExportConnSessOverrideAttr.setProperty("Sleep", "10");
		fastExportConnSessOverrideAttr.setProperty("Executable Name",
				"fast_export_exe");

		// set connection properties (session extension for teradata target)
		// to use teradata fastload
		ConnectionInfo tgtObjConnInfo = this.teradataTgt.getConnInfo();
		ConnectionProperties tgtConnProps = tgtObjConnInfo.getConnProps();
		tgtConnProps.setProperty(
				ConnectionPropsConstants.SESSION_EXTENSION_NAME, "File Writer");
		tgtConnProps.setProperty(ConnectionPropsConstants.CONNECTIONTYPE,
				"External Loader");
		tgtConnProps.setProperty(ConnectionPropsConstants.CONNECTIONSUBTYPE,
				"Teradata FastLoad External Loader");
		tgtConnProps.setProperty(ConnectionPropsConstants.CONNECTIONNAME,
				"Teradata_FastLoad_External_Loader");

		// override the teradaa target connection properties at session level
		Properties loaderConnSessOverrideAttr = tgtObjConnInfo
				.getSessLevelOverrideConnAttributes();
		loaderConnSessOverrideAttr.setProperty("Database Name", "TD_Database");
		loaderConnSessOverrideAttr.setProperty("Sleep", "20");
		loaderConnSessOverrideAttr.setProperty("External Loader Executable",
				"fastload_exe");
	}


	/*
	 * Create a session with teradata mapping. Override the relational
	 * reader and writer to use teradata fast export reader and
	 * fastload writer
	 */
	protected void createSession() throws Exception {
		session = new Session("TeradataLoadersSession",
				"TeradataLoadersSession",
				"Teradata session with reader/writer overriden to use "
						+ " teradatafast export and loaders");
		session.setMapping(mapping);
		// override the relational reader/writer to use teradata fastexport
		// reader and fastload writer
		overrideRelReaderWriter();
	}

	/*
	 * Create workflow using teradata session
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("TeradataLoaderWorkflow",
				"TeradataLoaderWorkflow", "Workflow for teradata session");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/*
	 * Main function to execute this sample program
	 */
	public static void main(String[] args) {
		try {
			TeradataLoaders teradataLoaders = new TeradataLoaders();
			if (args.length > 0) {
				if (teradataLoaders.validateRunMode(args[0])) {
					teradataLoaders.execute();
				}
			} else {
				teradataLoaders.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
