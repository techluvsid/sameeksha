/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This example applies a simple expression transformation on the Employee table
 * and writes to a target
 * 
 */
public class Expression extends Base {
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
        mapping = new Mapping( "ExpressionMapping", "mapping", "Testing Expression sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create an expression Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        String expr = "string(80, 0) fullName= firstName || lastName";
        TransformField outField = new TransformField( expr );
        String expr2 = "integer (10,0) YEAR_out = GET_DATE_PART(BirthDate, 'YYYY')";
        TransformField outField2 = new TransformField( expr2 );
        Vector vFields = new Vector();
        vFields.add( outField );
        vFields.add( outField2 );
        RowSet expRS = (RowSet) helper.expression( dsqRS, vFields, "exp_transform" ).getRowSets()
                .get( 0 );
        // write to target
        mapping.writeTarget( expRS, outputTarget );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            Expression expressionTrans = new Expression();
            if (args.length > 0) {
                if (expressionTrans.validateRunMode( args[0] )) {
                    expressionTrans.execute();
                }
            } else {
                expressionTrans.printUsage();
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
