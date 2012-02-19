/*
 * LinkExpression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

public class LinkExpression extends Base {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source employeeSrc;
    protected Target outputTarget;

    /**
     * Create sources
     */
    protected void createSources() {
        employeeSrc = this.createEmployeeSource();
        folder.addSource( employeeSrc );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Expression_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "LinkExpressionMapping", "mapping", "Testing LinkExpression sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create link fields
        Vector linkFields = getLinkFields();
        // create the link
        PortLinkContext portLinkContext = PortLinkContextFactory
                .getPortLinkContextByPosition( linkFields );
        InputSet linkInputSet = new InputSet( dsqRS, portLinkContext );
        // create an expression Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        String expr = "string(80, 0) fullName= firstName1 || lastName1";
        TransformField outField = new TransformField( expr );
        RowSet expRS = (RowSet) helper.expression( linkInputSet, outField, "link_exp_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( expRS, outputTarget );
        folder.addMapping( mapping );
    }

    /**
     * This methos returns the link fields
     * 
     * @return Vector
     */
    public Vector getLinkFields() {
        Vector fields = new Vector();
        Field field1 = new Field( "EmployeeID1", "EmployeeID1", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
        fields.add( field1 );
        Field field2 = new Field( "LastName1", "LastName1", "", DataTypeConstants.STRING, "20",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field2 );
        Field field3 = new Field( "FirstName1", "FirstName1", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field3 );
        return fields;
    }

    public static void main( String args[] ) {
        try {
            LinkExpression linkExpressionTrans = new LinkExpression();
            if (args.length > 0) {
                if (linkExpressionTrans.validateRunMode( args[0] )) {
                    linkExpressionTrans.execute();
                }
            } else {
                linkExpressionTrans.printUsage();
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
        session = new Session( "Session_For_Expression", "Session_For_Expression",
                "This is session for expression" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Expression", "Workflow_for_Expression",
                "This workflow for expression" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
