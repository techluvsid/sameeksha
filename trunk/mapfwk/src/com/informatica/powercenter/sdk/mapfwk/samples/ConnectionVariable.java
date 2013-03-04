/*
 * ConnectionVariable.java Created on Nov 28, 2008.
 *
 * Copyright 2008 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This sample program demonstrates the use of connection variable
 * while create sources or targets and performs the following
 *
 * 1. Create a Oracle source where connection is set using
 *    connection variable
 * 2. Create a Oracle target 
 * 3. Create a mapping as follows
 *
 *    Oracle source --> DSQ --> Expr Transform --> Oracle target
 *
 *    Expr Transform finds average of two fields MAX_SALARY & MIN_SALARY
 *
 * 4. Create a session using this mapping
 * 5. Create workflow using this session

 * @author nagarwal
 */
public class ConnectionVariable extends Base{

	private Mapping mapping = null;
	private Source jobOracleSrc = null;
	private Target oracleTgt = null;

	
    /**
     * Create a Oracle source and sets the connection using a connection variable
     */
    protected void createSources() {
		List<Field> fields = new ArrayList<Field>();
		
		// create the fields and them to List
		
		Field jobIDField = new Field( "JOB_ID", "JOB_ID", "",
					NativeDataTypes.Oracle.VARCHAR2, "10", "0",
    				FieldKeyType.PRIMARY_KEY, FieldType.SOURCE, true );
    	fields.add( jobIDField );

		Field jobTitleField = new Field( "JOB_TITLE", "JOB_TITLE", "",
				NativeDataTypes.Oracle.VARCHAR2, "35", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false );
		fields.add( jobTitleField );

		Field minSalField = new Field( "MIN_SALARY", "MIN_SALARY", "",
				NativeDataTypes.Oracle.NUMBER_PS, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false );
		fields.add( minSalField );

		Field maxSalField = new Field( "MAX_SALARY", "MAX_SALARY", "",
				NativeDataTypes.Oracle.NUMBER_PS, "6", "0",
				FieldKeyType.NOT_A_KEY, FieldType.SOURCE, false );
		fields.add( maxSalField );

		// creation connection
		
		ConnectionInfo connInfo = null;
		connInfo = new ConnectionInfo( SourceTargetType.Oracle );
		connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, "JobInfo_Oracle_Source" );
		
		// set the connection variable "$DBConnectionOracle" for this connection
		connInfo.setConnectionVariable("$DBConnectionOracle");

		// create a source with this connection
		jobOracleSrc = new Source( "JOBS", "JOBS", "This is JOBS table", "JOBS", connInfo );
		
		// add the fields to the source
		jobOracleSrc.setFields(fields);
		
		// add the source to the folder
		folder.addSource(jobOracleSrc);
	}

	/*
	 * Create a Oracle target
	 */
	protected void createTargets() {
		
		// create a connection
		ConnectionInfo connInfo = new ConnectionInfo( SourceTargetType.Oracle );
		connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, "JobInfo_Oracle_Source" );
		// set the connection name
		connInfo.getConnProps().setProperty( ConnectionPropsConstants.CONNECTIONNAME, "Oracle_conn" );

		// create target
		oracleTgt = new Target("Oracle_target",	"Oracle_target","Oracle_target",
														"Oracle_target",connInfo);
	}

	/*
	 * Create a mapping as follows:
	 *
	 *  Oracle Source --> DSQ --> Expression --> Oracle Target
	 */
	protected void createMappings() throws Exception {
		// create a mapping object
		mapping = new Mapping(
				"OracleSrcUsingConnectionVariable",
				"OracleSrcUsingConnectionVariable",
				"Mapping containing oracle src with connection variable, tgt and an expr.");
				
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
		List<TransformField> tranFields = new ArrayList<TransformField>();
		tranFields.add(outField);
		RowSet expRS = (RowSet) helper.expression(dsqRowSet, tranFields,
				"exp_transform").getRowSets().get(0);

		// write to target
		mapping.writeTarget(expRS, this.oracleTgt);
		folder.addMapping(mapping);
	}

	/*
	 * Create a session
	 */
	protected void createSession() throws Exception {
		session = new Session("OracleSrcUsingConnectionVariable",
				"OracleSrcUsingConnectionVariable",
				"Session with oracle source using connection variable");
		session.setMapping(mapping);
	}

	/*
	 * Create workflow
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow(
				"OracleSrcUsingConnectionVariableWorkflow",
				"OracleSrcUsingConnectionVariableWorkflow",
				"Workflow containing a session with with oracle source using connection variable");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/*
	 * Main function to execute this sample program
	 */
	public static void main(String[] args) {
		try {
			ConnectionVariable connVariable = new ConnectionVariable();
			if (args.length > 0) {
				if (connVariable.validateRunMode(args[0])) {
					connVariable.execute();
				}
			} else {
				connVariable.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
