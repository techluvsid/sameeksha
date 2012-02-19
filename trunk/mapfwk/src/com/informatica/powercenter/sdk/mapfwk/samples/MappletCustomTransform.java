/*
 * MappletCustomTransform.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
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
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

public class MappletCustomTransform extends Base {
    protected Target targetObj = null;
    protected Target targetObj1 = null;

    protected void createSources() {
    }

    protected void createTargets() {
        targetObj = this.createRelationalTarget( SourceTargetTypes.RELATIONAL_TYPE_ORACLE,
                "PMDP_COLUMN_7" );
        targetObj1 = this.createRelationalTarget( SourceTargetTypes.RELATIONAL_TYPE_ORACLE,
                "PMDP_COLUMN_8" );
    }

    protected void createMappings() throws Exception {
        // create a mapping object
        mapping = new Mapping( "MappletCustomTransformation", "MappletCustomTransformation",
                "MappletCustomTransformation" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // To create a mapplet, we need a vector of group sets. So, we create a
        // vector of group sets first.
        GroupSet outputGroupSet1 = new GroupSet( this.getMappletOutputFieldsForJobs(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        GroupSet outputGroupSet2 = new GroupSet( this.getMappletOutputFieldsForEmployee(),
                "Output2", GroupSetConstants.OUTPUTGROUP );
        Vector vec = new Vector();
        vec.add( outputGroupSet1 );
        vec.add( outputGroupSet2 );
        // Create a mapplet using the above vector of GroupSets
        // This mapplet should already be present in the Power Center
        // Repository. Please use the same name as that
        // of the mapplet. For e.g., as shown below, there should be a mapplet
        // with name "mappletCT" in the
        // Power Center Repository.
        OutputSet outSet = helper.mapplet( vec, TransformationConstants.MAPPLET, "mappletCT" );
        // Now, create a CT using the output row sets from the above Mapplet
        RowSet mapOutRowSet1 = (RowSet) outSet.getRowSet( "Output1" );
        RowSet mapOutRowSet2 = (RowSet) outSet.getRowSet( "Output2" );
        GroupSet inputGroupSet1 = new GroupSet( mapOutRowSet1, "Input1",
                GroupSetConstants.INPUTGROUP );
        GroupSet inputGroupSet2 = new GroupSet( mapOutRowSet2, "Input2",
                GroupSetConstants.INPUTGROUP );
        GroupSet outGrpSet1 = new GroupSet( this.getUpdDimCTFields(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        GroupSet outGrpSet2 = new GroupSet( this.getCTOUTFields(), "Output2",
                GroupSetConstants.OUTPUTGROUP );
        Vector vecCT = new Vector();
        vecCT.add( inputGroupSet1 );
        vecCT.add( outGrpSet1 );
        vecCT.add( inputGroupSet2 );
        vecCT.add( outGrpSet2 );
        // Use the above group sets and create a CT.
        OutputSet outf = (OutputSet) helper.custom( vecCT, TransformationConstants.ACTIVE_CUSTOM,
                "ACTIVE_CUSTOM_TRANSFORM" );
        RowSet updDimOutput1 = (RowSet) outf.getRowSet( "Output1" );
        RowSet updDimOutput2 = (RowSet) outf.getRowSet( "Output2" );
        mapping.writeTarget( updDimOutput1, this.targetObj );
        mapping.writeTarget( updDimOutput2, this.targetObj1 );
        folder.addMapping( mapping );
    }

    protected void createSession() throws Exception {
        session = new Session( "Session_For_" + mapping.getName(), "Session_For_"
                + mapping.getName(), "This is session for " + mapping.getName() );
        session.setMapping( this.mapping );
    }

    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_" + mapping.getName(), "Workflow_for_"
                + mapping.getName(), "This workflow for " + mapping.getName() );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            MappletCustomTransform mapCT = new MappletCustomTransform();
            if (args.length > 0) {
                if (mapCT.validateRunMode( args[0] )) {
                    mapCT.execute();
                }
            } else {
                mapCT.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
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

    private Vector getMappletOutputFieldsForJobs() {
        Vector vFields = new Vector();
        Field jobIDField = new Field( "JOB_ID", "JOB_ID", "", DataTypeConstants.VARCHAR2, "10",
                "0", FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
        vFields.add( jobIDField );
        Field jobTitleField = new Field( "JOB_TITLE", "JOB_TITLE", "", DataTypeConstants.VARCHAR2,
                "35", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vFields.add( jobTitleField );
        Field minSalField = new Field( "MIN_SALARY", "MIN_SALARY", "", DataTypeConstants.DECIMAL,
                "6", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vFields.add( minSalField );
        Field maxSalField = new Field( "MAX_SALARY", "MAX_SALARY", "", DataTypeConstants.DECIMAL,
                "6", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        vFields.add( maxSalField );
        return vFields;
    }

    private Vector getMappletOutputFieldsForEmployee() {
        Vector fields = new Vector();
        Field field1 = new Field( "EmployeeID", "EmployeeID", "", DataTypeConstants.INTEGER, "10",
                "0", FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
        fields.add( field1 );
        Field field2 = new Field( "LastName", "LastName", "", DataTypeConstants.STRING, "20", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field2 );
        Field field3 = new Field( "FirstName", "FirstName", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field3 );
        Field field4 = new Field( "Title", "Title", "", DataTypeConstants.STRING, "30", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field4 );
        Field field5 = new Field( "TitleOfCourtesy", "TitleOfCourtesy", "",
                DataTypeConstants.STRING, "25", "0", FieldConstants.NOT_A_KEY,
                Field.FIELDTYPE_SOURCE, false );
        fields.add( field5 );
        Field field6 = new Field( "BirthDate", "BirthDate", "", DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field6 );
        Field field7 = new Field( "HireDate", "HireDate", "", DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field7 );
        Field field8 = new Field( "Address", "Address", "", DataTypeConstants.STRING, "60", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field8 );
        Field field9 = new Field( "City", "City", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field9 );
        Field field10 = new Field( "Region", "Region", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field10 );
        Field field11 = new Field( "PostalCode", "PostalCode", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field11 );
        Field field12 = new Field( "Country", "Country", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field12 );
        Field field13 = new Field( "HomePhone", "HomePhone", "", DataTypeConstants.STRING, "24",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field13 );
        Field field14 = new Field( "Extension", "Extension", "", DataTypeConstants.STRING, "4",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field14 );
        Field field15 = new Field( "Notes", "Notes", "", DataTypeConstants.CLOB, "350", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field15 );
        Field field16 = new Field( "ReportsTo", "ReportsTo", "", DataTypeConstants.INTEGER, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field16 );
        return fields;
    }
}
