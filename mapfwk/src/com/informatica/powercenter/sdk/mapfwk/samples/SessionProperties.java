/*
 * SessionProperties.java Created on Oct 08, 2008.
 *
 * Copyright 2008 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SessionPropsConstants;
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
 *    Oracle source --> DSQ --> Expr Transform --> Oracle target
 *
 *    Expr Transform finds average of two fields MAX_SALARY & MIN_SALARY
 *
 * 2. Create a session using this mapping
 * 3. Set various config and session properties
 * 4. Create workflow using this session

 * @author nagarwal
 */
public class SessionProperties extends Base {

	private Mapping mapping = null;
	private Source jobOracleSrc = null;
	private Target oracleTgt = null;

	/*
	 * Create a oracle source
	 */
	protected void createSources() {
		jobOracleSrc = this.createOracleJobSource("JobInfo_Oracle_Source");
		folder.addSource(jobOracleSrc);
	}

	/*
	 * Create a oracle target
	 */
	protected void createTargets() {
		oracleTgt = this.createRelationalTarget(
				SourceTargetType.Oracle, "Oracle_target");
	}

	/*
	 * Create a mapping as follows:
	 *
	 *  Oracle Source --> DSQ --> Expression --> Oracle Target
	 */
	protected void createMappings() throws Exception {
		// create a mapping object
		mapping = new Mapping(
				"ExprWithSessPropOverriden",
				"ExprWithSessPropOverriden",
				"Mapping containing oracle src, tgt and an expr." +
				"Session properties are overriden.");
		setMapFileName(mapping);

		// create transform helper
		TransformHelper helper = new TransformHelper(mapping);

		// create DSQ
		RowSet dsqRowSet = (RowSet) helper.sourceQualifier(this.jobOracleSrc)
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
		mapping.writeTarget(expRS, this.oracleTgt);
		folder.addMapping(mapping);
	}

	/**
	 * Set session and config properties
	 */
	private void setSessionAndConfigProperties() {
		Properties sessProps = this.session.getProperties();
		
		// set session properties
		
		// set the "Enable High Precision" to "Yes"
		sessProps.setProperty(SessionPropsConstants.ENABLE_HIGH_PRECISION, "Yes");
		// set the "Treat source rows as" to "Update"
		sessProps.setProperty(SessionPropsConstants.TREAT_SOURCE_ROWS_AS, "Update");
		// set the "Write Backward Compatible Session Log File" to "Yes"
		sessProps.setProperty(SessionPropsConstants.WRITE_BACKWARD_COMPATIBLE_SESSION_LOG_FILE, "Yes");
		// set the name of session log file
		sessProps.setProperty(SessionPropsConstants.SESSION_LOG_FILE_NAME, "mysession.log");
		// set the session log file directory
		sessProps.setProperty(SessionPropsConstants.SESSION_LOG_FILE_DIR, "c:\\MySessLog\\");
		// set the enable test load to "Yes"
		sessProps.setProperty(SessionPropsConstants.ENABLE_TEST_LOAD , "Yes");
		// set the number of rows to be tested to 99
		sessProps.setProperty(SessionPropsConstants.NUMBER_OF_ROWS_TO_TEST , "99");
		// set the pushdown optimization to "Full"
		sessProps.setProperty(SessionPropsConstants.PUSHDOWN_OPTIMIZATION , "Full");
		
		
		// set config properties
		
		// set "Stop on errors" to 5 (default is 0)
		sessProps.setProperty(SessionPropsConstants.CFG_STOP_ON_ERRORS, "5");
		// set the "Override tracing" to "Verbose Data"
		sessProps.setProperty(SessionPropsConstants.CFG_OVERRIDE_TRACING, "Verbose Data");
		// memory settings
        sessProps.setProperty(SessionPropsConstants.CFG_MAXIMUM_MEMORY_ALLOWED_FOR_AUTO_MEMORY_ATTRIBUTES , "512MB");
        sessProps.setProperty(SessionPropsConstants.CFG_MAXIMUM_PERCENTAGE_OF_TOTAL_MEMORY_ALLOWED_FOR_AUTO_MEMORY_ATTRIBUTES , "15");
        // set a different datetime format
        sessProps.setProperty(SessionPropsConstants.CFG_DATETIME_FORMAT_STRING , "MM/DD/YYYY HH12:MI:SS.US");

	}

	/*
	 * Create a session and override some config and session properties
	 */
	protected void createSession() throws Exception {
		session = new Session("ExprMappingWithSessPropOverriden",
				"ExprMappingWithSessPropOverriden",
				"Session with some session and config properties overriden");
		session.setMapping(mapping);

		// set some session and config properties
		setSessionAndConfigProperties();
	}

	/*
	 * Create workflow using teradata session
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow(
				"SessionPropertiesOverriddenWorkflow",
				"SessionPropertiesOverriddenWorkflow",
				"Workflow containing a session with properties overriden");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/*
	 * Main function to execute this sample program
	 */
	public static void main(String[] args) {
		try {
			SessionProperties sessProp = new SessionProperties();
			if (args.length > 0) {
				if (sessProp.validateRunMode(args[0])) {
					sessProp.execute();
				}
			} else {
				sessProp.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
