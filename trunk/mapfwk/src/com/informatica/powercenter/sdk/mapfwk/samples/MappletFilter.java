/*
 * MappletFilter.java Created on Nov 4, 2005.
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
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

public class MappletFilter extends Base {
    protected Target outputTarget;

    protected void createSources() {
    }

    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Filter_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "MappletFilter", "MappletFilter",
                "This is sample for Mapplet Filter" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // To create a mapplet, we need a vector of group sets. So, we create a
        // vector of group sets first.
        GroupSet outputGroupSet1 = new GroupSet( this.getMappletOutputFields(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        Vector vec = new Vector();
        vec.add( outputGroupSet1 );
        // This mapplet should already be present in the Power Center
        // Repository. Please use the same name as that
        // of the mapplet. For e.g., as shown below, there should be a mapplet
        // with name "testFilter" in the
        // Power Center Repository.
        RowSet dsqRS = (RowSet) helper.mapplet( vec, TransformationConstants.MAPPLET, "testFilter" )
                .getRowSets().get( 0 );
        // create a Filter Transformation
        // filter out rows that don't belong to USA
        RowSet filterRS = (RowSet) helper.filter( dsqRS, "Country = 'USA'", "filter_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( filterRS, outputTarget );
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

    // This method is used to define the vector of output fields coming out of
    // the Output transformation
    // of a mapplet.
    private Vector getMappletOutputFields() {
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

    public static void main( String args[] ) {
        try {
            MappletFilter mapFilter = new MappletFilter();
            if (args.length > 0) {
                if (mapFilter.validateRunMode( args[0] )) {
                    mapFilter.execute();
                }
            } else {
                mapFilter.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
