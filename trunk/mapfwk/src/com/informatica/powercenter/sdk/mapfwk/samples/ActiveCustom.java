/*
 * ActiveCustom.java Created on Jul 5, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSet;
import com.informatica.powercenter.sdk.mapfwk.core.GroupType;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationDataTypes;
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
		jobSourceObj = this.createOracleJobSource("Oracle_Source");
		folder.addSource(jobSourceObj);
		employeeSourceObj = this.createEmployeeSource();
		folder.addSource(employeeSourceObj);
	}

    /*
	 * (non-Javadoc)
	 *
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
	 */
    protected void createTargets() {
        targetObj = this.createRelationalTarget( SourceTargetType.Oracle,
                "PMDP_COLUMN_7" );
        targetObj1 = this.createRelationalTarget( SourceTargetType.Oracle,
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
        GroupSet inputGroupSet1 = new GroupSet( dsqRowSet1, "Input1", GroupType.INPUT );
        GroupSet inputGroupSet2 = new GroupSet( dsqRowSet2, "Input2", GroupType.INPUT );
        GroupSet outputGroupSet1 = new GroupSet( this.getUpdDimCTFields(), "Output1",
                GroupType.OUTPUT);
        GroupSet outputGroupSet2 = new GroupSet( this.getCTOUTFields(), "Output2",
                GroupType.OUTPUT );
        List<GroupSet> list = new ArrayList<GroupSet>();
        list.add( inputGroupSet1 );
        list.add( outputGroupSet1 );
        list.add( inputGroupSet2 );
        list.add( outputGroupSet2 );
        OutputSet outf = (OutputSet) helper.custom( list, TransformationConstants.ACTIVE_CUSTOM,
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

    private List<Field> getUpdDimCTFields() {
        List<Field> list = new ArrayList<Field>();
        Field profileRunKeyField = new Field( "JOB_ID", "JOB_ID", "", TransformationDataTypes.INTEGER,
                "10", "0", FieldKeyType.NOT_A_KEY, FieldType.TRANSFORM, false );
        list.add( profileRunKeyField );
        Field sfnField = new Field( "JOB_TITLE", "JOB_TITLE", "", TransformationDataTypes.INTEGER, "10",
                "0", FieldKeyType.NOT_A_KEY, FieldType.TRANSFORM, false );
        list.add( sfnField );
        return list;
    }

    private List<Field> getCTOUTFields() {
        List<Field> list = new ArrayList<Field>();
        Field profileRunKeyField = new Field( "EmployeeID", "EmployeeID", "",
                TransformationDataTypes.INTEGER, "10", "0", FieldKeyType.NOT_A_KEY,
                FieldType.TRANSFORM, false );
        list.add( profileRunKeyField );
        Field sfnField = new Field( "SFN", "SFN_O_@@@", "", TransformationDataTypes.INTEGER, "10", "0",
                FieldKeyType.NOT_A_KEY, FieldType.TRANSFORM, false );
        list.add( sfnField );
        return list;
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
