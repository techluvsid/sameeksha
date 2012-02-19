/*
 * PassiveCustom.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.GenericTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSet;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSetConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationProperties;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * @auther kbhat 
 */
public class GenericPassiveCustom extends Base {
    // ////////////////////////////////////////////////////////////////
    // instance variables
    // ////////////////////////////////////////////////////////////////
    protected Mapping mapping = null;
    protected Source jobSourceObj = null;
    protected Target targetObj = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        jobSourceObj = this.createJobSource();
        folder.addSource( jobSourceObj );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
        targetObj = this.createRelationalTarget( SourceTargetTypes.RELATIONAL_TYPE_ORACLE,
                "PMDP_COLUMN_7" );
    }

    protected Vector gengrpcreate() {
    	Vector vFields = new Vector();
		Field jobIDField = new Field( "JOB_ID", "JOB_ID", "",
					DataTypeConstants.VARCHAR2, "10", "0",
    				FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
    	vFields.add( jobIDField );

		Field jobTitleField = new Field( "JOB_TITLE", "JOB_TITLE", "",
				DataTypeConstants.VARCHAR2, "35", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( jobTitleField );

		Field minSalField = new Field( "MIN_SALARY", "MIN_SALARY", "",
				DataTypeConstants.DECIMAL, "6", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( minSalField );

		Field maxSalField = new Field( "MAX_SALARY", "MAX_SALARY", "",
				DataTypeConstants.DECIMAL, "6", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( maxSalField );
		
		return vFields;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping object
        mapping = new Mapping( "PassiveCustomTransformationGeneric", "PassiveCustomTransformationGeneric",
                "Mapping for FDA on JOBS table" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // create DSQ
        RowSet dsqRowSet1 = (RowSet) helper.sourceQualifier( this.jobSourceObj ).getRowSets().get(0 );
        // Two more fields are added
        GroupSet inputGroupSet1 = new GroupSet(dsqRowSet1 , "InputGroup1", GroupSetConstants.INPUTGROUP );
        
        GroupSet outputGroupSet1 = new GroupSet( this.getUpdDimCTFields(), "OutputGroup1",GroupSetConstants.OUTPUTGROUP );
        //inputGroupSet1.setInputSet((InputSet)dsqRowSet1);
        
        Vector vecin = new Vector();
        vecin.add( inputGroupSet1 );
        vecin.add( outputGroupSet1);
       
       
        
        TransformationProperties props = new TransformationProperties();		
		props.setProperty("Language", "C");
		props.setProperty("Module Identifier", "moduleTest");
		props.setProperty("Class Name", "");
		props.setProperty("Function Identifier", "functiontest");
		props.setProperty("Runtime Location", "$PMExtProcDir");
		props.setProperty("Tracing Level", "Normal");
		props.setProperty("Is Partitionable", "No");
		props.setProperty("Inputs Must Block", "YES");
		props.setProperty("Is Active", "NO");
		props.setProperty("Update Strategy Transformation", "NO");
		props.setProperty("Transformation Scope", "Row");
		props.setProperty("Generate Transaction", "NO");
		props.setProperty("Output Is Repeatable", "Based On Input Order");
		props.setProperty("Requires Single Thread Per Partition", "YES");
		props.setProperty("Output Is Deterministic", "YES");
		
        //OutputSet outf = (OutputSet) helper.custom( vec, TransformationConstants.PASSIVE_CUSTOM,"PASSIVE_CUSTOM_TRANSFORM" );		
		
		GenericTransformation trans = new GenericTransformation("custom","","","Custom","Custom Transformation",vecin,null,props);
		
		mapping.addTransformation(trans.getTransContext().getTransformObj());

		RowSet rs = (RowSet)trans.apply().getRowSets().get(0);
		
        mapping.writeTarget( rs, this.targetObj );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_CustomPASSIVE", "Session_For_CustomPASSIVE",
                "This is session for Custom Transformation" );
        session.setMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_CustomTransformation",
                "Workflow_for_CustomTransformation", "This workflow for Custom Transformation" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    private Vector getUpdDimCTFields() {
        Vector vec = new Vector();
        Field profileRunKeyField = new Field( "JOB_ID", "JOB_ID", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vec.add( profileRunKeyField );
        Field sfnField = new Field( "JOB_TITLE", "JOB_TITLE", "", DataTypeConstants.INTEGER, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vec.add( sfnField );
        return vec;
    }

    /**
     * @param args
     */
    public static void main( String[] args ) {
        try {
            GenericPassiveCustom customPassive = new GenericPassiveCustom();
            if (args.length > 0) {
                if (customPassive.validateRunMode( args[0] )) {
                    customPassive.execute();
                }
            } else {
                customPassive.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
