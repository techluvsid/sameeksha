/*
 * UncLkp.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputField;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationProperties;
import com.informatica.powercenter.sdk.mapfwk.core.UnconnectedLookup;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This example applies a simple expression transformation on the Employee table
 * and writes to a target
 * 
 */
public class UncLkp extends Base {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source employeeSrc;
    protected Target outputTarget;

    /**
     * Create sources
     */
    protected void createSources() {
        employeeSrc = this.createItemsSource();
        folder.addSource( employeeSrc );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createRelationalTarget( "Expression_Output", 3 );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "UncMapping", "mapping", "Testing UncMapping sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create an expression Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        String expr = "string(80, 0) firstName1= IIF(ISNULL(:LKP.lkp(ItemId, Item_Name)), "
                + "DD_UPDATE, DD_REJECT)";
        TransformField outField = new TransformField( expr );
        RowSet expRS = (RowSet) helper.expression( dsqRS, outField, "exp_transform" ).getRowSets()
                .get( 0 );
        // create a unconnected lookup transformation
        // set the return port
        Field retField = new Field( "Item_No1", "Item_No", "", DataTypeConstants.INTEGER, "10",
                "0", FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, false );
        // create input and output fields
        Vector input = new Vector();
        Vector output = new Vector();
        createUncLkpFields( input, output );
        // create an unconnected lookup
        String condition = "ItemId = EmployeeID";
        UnconnectedLookup uncLkp = helper.unconnectedLookup( "lkp", retField, input, condition,
                employeeSrc );
        uncLkp.setPortType( "EmployeeID", OutputField.TYPE_LOOKUP );
        // write to target
        mapping.writeTarget( expRS, outputTarget );
        folder.addMapping( mapping );
    }

    // create the fields for unconnected lookup transformation
    void createUncLkpFields( Vector input, Vector output ) {
        Field field1 = new Field( "EmployeeID", "EmployeeID", "", DataTypeConstants.DECIMAL, "10",
                "0", FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, false );
        input.add( field1 );
        Field field2 = new Field( "LastName", "LastName", "", DataTypeConstants.STRING, "20", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        input.add( field2 );
        Field field3 = new Field( "FirstName", "FirstName", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field3 );
        Field field4 = new Field( "Title", "Title", "", DataTypeConstants.STRING, "30", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field4 );
        Field field5 = new Field( "TitleOfCourtesy", "TitleOfCourtesy", "",
                DataTypeConstants.STRING, "25", "0", FieldConstants.NOT_A_KEY,
                Field.FIELDTYPE_SOURCE, false );
        output.add( field5 );
        Field field7 = new Field( "HireDate", "HireDate", "", DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field7 );
        Field field8 = new Field( "Address", "Address", "", DataTypeConstants.STRING, "60", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field8 );
        Field field9 = new Field( "City", "City", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field9 );
        Field field10 = new Field( "Region", "Region", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field10 );
        Field field11 = new Field( "PostalCode", "PostalCode", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field11 );
        Field field12 = new Field( "Country", "Country", "", DataTypeConstants.STRING, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field12 );
        Field field13 = new Field( "HomePhone", "HomePhone", "", DataTypeConstants.STRING, "24",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field13 );
        Field field14 = new Field( "Extension", "Extension", "", DataTypeConstants.STRING, "4",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field14 );
        Field field15 = new Field( "Notes", "Notes", "", DataTypeConstants.CLOB, "350", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field15 );
        Field field16 = new Field( "ReportsTo", "ReportsTo", "", DataTypeConstants.INTEGER, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        output.add( field16 );
    }

    public static void main( String args[] ) {
        try {
            UncLkp unclTrans = new UncLkp();
            if (args.length > 0) {
                if (unclTrans.validateRunMode( args[0] )) {
                    unclTrans.execute();
                }
            } else {
                unclTrans.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_UncMapping", "Session_For_UncMapping",
                "This is session for UncMapping" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_UncMapping", "Workflow_for_Expression",
                "This workflow for UncMapping" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
