/*
 * SessionMappingTabProperties.java Created on January 02, 2009.
 *
 * Copyright 2008 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionProperties;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This sample program demonstrates how to use properties specified in Mapping Tab
 * of session (reader/writer properties and source/source qualifier/target properties) in JMF
 * 
 * It performs the following:
 *
 * 1. Create a mapping as follows
 *
 *    Oracle source --> DSQ --> Expr Transform --> Oracle target
 *
 *    Expr Transform finds average of two fields MAX_SALARY & MIN_SALARY
 *
 * 2. Create a session using this mapping
 * 
 * 3. Set following relational writer properties
 *
 *    a. set "Update as Update" to No (default is Yes)
 *    b. set the "Update else Insert" to Yes (default is No)
 *    c. set the "Reject file directory" session property to "C:\\JMF_Test")
 *    d. set the to "Reject filename" to "reject_file_name.bad"
 *
 * 4. Set following relational Source Qualifier, Source & Target properties
 * 
 * 	  // Source Qualifier properties
 * 	  a. set the "Tracing Level" to  "Verbose Data" (default is "Normal")
 *    b. set "Select Distinct" to "Yes" (default is No)
 * 	  
 * 	  // Source properties
 * 	  a. set the "Owner Name" to "JMFTestOwner"
 *    b. set "Source Table Name" to "JMFTesTTable"
 *
 * 	  // target properties
 * 	  a. set the "Table Name Prefix" to "JMFTestPrefix"
 *    b. set the "Target Table Name" to "JMFTestTgtTable"
 * 
 * 5. Create workflow using this session

 * @author nagarwal
 */
public class SessionMappingTabProperties extends Base {

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
				SourceTargetType.Oracle ,"Oracle_target");
	}

	/*
	 * Create a mapping as follows:
	 *
	 *  Oracle Source --> DSQ --> Expression --> Oracle Target
	 */
	protected void createMappings() throws Exception {
		// create a mapping object
		mapping = new Mapping(
				"SessionMappingTabProperties",
				"SessionMappingTabProperties",
				"Mapping containing oracle src, tgt and an expr." +
				"In the session, Writer and Source/Target properties in Mapping Tab are overriden.");
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

	/*
	 * Sets Relational Writer properties
	 */
	private void setWriterProperties() {
		ConnectionInfo tgtConInfo = this.oracleTgt.getConnInfo();
		
		ConnectionProperties tgtConnProps = tgtConInfo.getConnProps();
		
		// Set "Update as Update" to No (default is Yes)
		tgtConnProps.setProperty(ConnectionPropsConstants.RELATIONAL_UPDATE_AS_UPDATE , "No");
		// Set "Update else Insert" to Yes (default is No)
		tgtConnProps.setProperty(ConnectionPropsConstants.RELATIONAL_UPDATE_ELSE_INSERT , "Yes");
		// Set "Reject file directory"
		tgtConnProps.setProperty(ConnectionPropsConstants.REJECT_FILE_DIRECTORY , "C:\\JMF_Test\\");
		// Set "Reject filename"
		tgtConnProps.setProperty(ConnectionPropsConstants.REJECT_FILENAME , "jmf_reject_file_name.bad");
	}

	/*
	 * Sets Source Qualifier, Source and Target properties
	 */
	private void setSourceTargetProperties() {
		
		// get the DSQ Transformation (if Source name is "JOBS", then corresponding SQ name is
		// "SQ_JOBS")
		DSQTransformation dsq = (DSQTransformation)this.mapping.getTransformation("SQ_JOBS");
		
		// set the Source Qualifier properties
		dsq.setSessionTransformInstanceProperty("Tracing Level", "Verbose Data");
		dsq.setSessionTransformInstanceProperty("Select Distinct", "Yes");
		
		// set Source properties
		this.jobOracleSrc.setSessionTransformInstanceProperty("Owner Name", "JMFTestOwner");
		this.jobOracleSrc.setSessionTransformInstanceProperty("Source Table Name", "JMFTesTTable");

		// set Target properties
		this.oracleTgt.setSessionTransformInstanceProperty("Table Name Prefix", "JMFTestPrefix");
		this.oracleTgt.setSessionTransformInstanceProperty("Target Table Name", "JMFTestTgtTable");

	}
	/*
	 * Create a session and override some config and session properties
	 */
	protected void createSession() throws Exception {
		session = new Session("SessionMappingTabProperties",
				"SessionMappingTabProperties",
				"Session with Writer and Source/Target properties in Mapping Tab overriden.");
		session.setMapping(mapping);

		// set Writer properties (present in Mapping tab of Session)
		setWriterProperties();
		// set Source, Source Qualifier and Target Properties (present in Mapping tab of Session)
		setSourceTargetProperties();
	}

	/*
	 * Create workflow using teradata session
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow(
				"SessionMappingTabPropertiesWorkflow",
				"SessionMappingTabPropertiesWorkflow",
				"Workflow containing a session with Writer and Source/Target properties in Mapping Tab verriden");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/*
	 * Main function to execute this sample program
	 */
	public static void main(String[] args) {
		try {
			SessionMappingTabProperties sessProp = new SessionMappingTabProperties();
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
