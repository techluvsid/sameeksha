/*
 * StoredProcExample.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputField;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable;
import java.util.Vector;

public class StoredProcExample extends Base {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source itemsSource;
    protected Target outputTarget;

    /**
     * Create sources
     */
    protected void createSources() {
        itemsSource = this.createItemsSource();
        folder.addSource( itemsSource );
        ConnectionInfo connInfo = getRelationalConnectionInfo( SourceTargetTypes.RELATIONAL_TYPE_MSSQL );
        connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, "toolsdevelop" );
        itemsSource.setConnInfo( connInfo );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createRelationalTarget( "StoredProc_Output_SQLServer",
                SourceTargetTypes.RELATIONAL_TYPE_MSSQL );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "Stored Proc Mapping", "Stored Proc Mapping",
                "This is sample for Stored Procedure" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        RowSet dsqRS = (RowSet) helper.sourceQualifier( this.itemsSource ).getRowSets().get( 0 );
        // create a stored procedure transformation
        Vector vTransformFields = new Vector();
        Field field1 = new Field( "RetValue", "RetValue", "This is return value",
                DataTypeConstants.INTEGER, "10", "0", FieldConstants.NOT_A_KEY,
                OutputField.TYPE_RETURN_OUTPUT, false );
        TransformField tField1 = new TransformField( field1, OutputField.TYPE_RETURN_OUTPUT );
        vTransformFields.add( tField1 );
        Field field2 = new Field( "nID1", "nID1", "This is the ID field",
                DataTypeConstants.INTEGER, "10", "0", FieldConstants.NOT_A_KEY,
                OutputField.TYPE_INPUT, false );
        TransformField tField2 = new TransformField( field2, OutputField.TYPE_INPUT );
        // vTransformFields.add( tField2 );
        Field field3 = new Field( "outVar", "outVar", "This is the Output field",
                DataTypeConstants.STRING, "20", "0", FieldConstants.NOT_A_KEY,
                OutputField.TYPE_INPUT_OUTPUT, false );
        TransformField tField3 = new TransformField( field3, OutputField.TYPE_INPUT_OUTPUT );
        vTransformFields.add( tField3 );
        java.util.Hashtable link = new java.util.Hashtable();
        link.put( dsqRS.getField( "ItemId" ), field2 );
        PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap( link );
        RowSet storedProcRS = (RowSet) helper.storedProc( new InputSet( dsqRS, linkContext ),
                vTransformFields, "SampleStoredProc", "Sample Stored Procedure Transformation" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( storedProcRS, this.outputTarget );
        folder.addMapping( mapping );
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_StoredProc", "Session_For_StoredProc",
                "This is session for Stored Proc" );
        session.setMapping( mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_StoredProc", "Workflow_for_StoredProc",
                "This workflow for Stored Proc" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            StoredProcExample example = new StoredProcExample();
            if (args.length > 0) {
                if (example.validateRunMode( args[0] )) {
                    example.execute();
                }
            } else {
                example.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
