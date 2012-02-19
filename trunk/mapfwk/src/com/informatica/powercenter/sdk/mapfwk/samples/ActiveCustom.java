/*
 * ActiveCustom.java Created on Jul 5, 2005.
 * 
 * Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
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
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * 
 */
public class ActiveCustom extends Base {
    // ////////////////////////////////////////////////////////////////
    // instance variables
    // ////////////////////////////////////////////////////////////////
    protected Mapping mapping = null;
    protected Source jobSourceObj = null;
    protected Source employeeSourceObj = null;
    protected Target targetObj = null;
    protected Target targetObj1 = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        jobSourceObj = this.createJobSource();
        folder.addSource( jobSourceObj );
        employeeSourceObj = this.createEmployeeSource();
        folder.addSource( employeeSourceObj );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
        targetObj = this.createRelationalTarget( SourceTargetTypes.RELATIONAL_TYPE_ORACLE,
                "PMDP_COLUMN_7" );
        targetObj1 = this.createRelationalTarget( SourceTargetTypes.RELATIONAL_TYPE_ORACLE,
                "PMDP_COLUMN_8" );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping object
        mapping = new Mapping( "ActiveCustomTransformation", "ActiveCustomTransformation",
                "Mapping for FDA on JOBS table" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // create DSQ
        RowSet dsqRowSet1 = (RowSet) helper.sourceQualifier( this.jobSourceObj ).getRowSets().get(
                0 );
        RowSet dsqRowSet2 = (RowSet) helper.sourceQualifier( this.employeeSourceObj ).getRowSets()
                .get( 0 );
        // Two more fields are added
        GroupSet inputGroupSet1 = new GroupSet( dsqRowSet1, "Input1", GroupSetConstants.INPUTGROUP );
        GroupSet inputGroupSet2 = new GroupSet( dsqRowSet2, "Input2", GroupSetConstants.INPUTGROUP );
        GroupSet outputGroupSet1 = new GroupSet( this.getUpdDimCTFields(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        GroupSet outputGroupSet2 = new GroupSet( this.getCTOUTFields(), "Output2",
                GroupSetConstants.OUTPUTGROUP );
        Vector vec = new Vector();
        vec.add( inputGroupSet1 );
        vec.add( outputGroupSet1 );
        vec.add( inputGroupSet2 );
        vec.add( outputGroupSet2 );
        OutputSet outf = (OutputSet) helper.custom( vec, TransformationConstants.ACTIVE_CUSTOM,
                "ACTIVE_CUSTOM_TRANSFORM" );
        RowSet updDimOutput1 = (RowSet) outf.getRowSet( "Output1" );
        RowSet updDimOutput2 = (RowSet) outf.getRowSet( "Output2" );
        mapping.writeTarget( updDimOutput1, this.targetObj );
        mapping.writeTarget( updDimOutput2, this.targetObj1 );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_CustomACTIVE", "Session_For_CustomACTIVE",
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

    private Vector getCTOUTFields() {
        Vector vec = new Vector();
        Field profileRunKeyField = new Field( "EmployeeID", "EmployeeID", "",
                DataTypeConstants.INTEGER, "10", "0", FieldConstants.NOT_A_KEY,
                Field.FIELDTYPE_SOURCE, false );
        vec.add( profileRunKeyField );
        Field sfnField = new Field( "SFN", "SFN_O_@@@", "", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vec.add( sfnField );
        return vec;
    }

    /**
     * @param args
     */
    public static void main( String[] args ) {
        try {
            ActiveCustom customActive = new ActiveCustom();
            if (args.length > 0) {
                if (customActive.validateRunMode( args[0] )) {
                    customActive.execute();
                }
            } else {
                customActive.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
