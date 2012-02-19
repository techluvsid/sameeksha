/*
 * MappingVarExpr.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariable;
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
public class MappingVarExpr extends Base {
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
        outputTarget = this.createFlatFileTarget( "MappingVarExpr_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "MappingVarExprMapping", "mapping", "Testing MappingVarExpr sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // set variable
        MappingVariable mappingVar = new MappingVariable( "MAX", DataTypeConstants.STRING, "0",
                "mapping variable example", false, "$$a", "10", "0", true );
        mapping.addMappingVariable( mappingVar );
        // set parameter
        mappingVar = new MappingVariable( DataTypeConstants.STRING, "0",
                "mapping variable example", true, "$$b", "10", "0", true );
        mapping.addMappingVariable( mappingVar );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create an MappingVarExpr Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        String expr = "string(80, 0) fullName=$$a";
        TransformField outField = new TransformField( expr );
        RowSet expRS = (RowSet) helper.expression( dsqRS, outField, "mappping_var_exp_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( expRS, outputTarget );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            MappingVarExpr mappingVarExprTrans = new MappingVarExpr();
            if (args.length > 0) {
                if (mappingVarExprTrans.validateRunMode( args[0] )) {
                    mappingVarExprTrans.execute();
                }
            } else {
                mappingVarExprTrans.printUsage();
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
        session = new Session( "Session_For_MappingVarExpr", "Session_For_MappingVarExpr",
                "This is session for MappingVarExpr" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_MappingVarExpr", "Workflow_for_MappingVarExpr",
                "This workflow for MappingVarExpr" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
