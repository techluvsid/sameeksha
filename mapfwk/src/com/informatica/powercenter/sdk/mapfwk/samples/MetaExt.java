/*
 * MetaExt.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MetaExtension;
import com.informatica.powercenter.sdk.mapfwk.core.MetaExtensionDataType;
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
public class MetaExt extends Base {
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
        addMetaExts( employeeSrc );
        folder.addSource( employeeSrc );
    }

    protected void addMetaExts( Object obj ) {
        if (obj instanceof Source) {
            ((Source) obj).addMetaExtension( new MetaExtension( "MetaExString",
                    MetaExtensionDataType.STRING, "10", "0", false ) );
            ((Source) obj).addMetaExtension( new MetaExtension( "MetaExBool",
                    MetaExtensionDataType.BOOLEAN, "1", "true", false ) );
            ((Source) obj).addMetaExtension( new MetaExtension( "MetaExNum",
                    MetaExtensionDataType.NUMBER, "15", "10", false ) );
            // ((Source)obj).addMetaExtension(new
            // MetaExtension("MetaExXML",MetaExtensionDataType.XML,"10","0",false));
        } else if (obj instanceof Target) {
            ((Target) obj).addMetaExtension( new MetaExtension( "MetaExString",
                    MetaExtensionDataType.STRING, "10", "0", false ) );
            ((Target) obj).addMetaExtension( new MetaExtension( "MetaExBool",
                    MetaExtensionDataType.BOOLEAN, "0", "true", false ) );
            ((Target) obj).addMetaExtension( new MetaExtension( "MetaExNum",
                    MetaExtensionDataType.NUMBER, "15", "15", false ) );
            // ((Target)obj).addMetaExtension(new
            // MetaExtension("MetaExXML",MetaExtensionDataType.XML,"10","0",false));
        }
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Expression_Output" );
        addMetaExts( outputTarget );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "MetaExtensionMapping", "mapping", "Testing Expression sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        InputSet dsqIS = new InputSet( dsqRS );
        Vector vinSets = new Vector();
        Vector vTrnsFields = new Vector();
        vinSets.add( dsqIS );
        // create an expression Transformation
        // the fields LastName and FirstName are concataneted to produce a new
        // field fullName
        String expr = "string(80, 0) fullName= firstName || lastName";
        TransformField outField = new TransformField( expr );
        vTrnsFields.add( outField );
        RowSet expRS = (RowSet) helper.expression( vinSets, vTrnsFields, "exp_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( expRS, outputTarget );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            MetaExt metaExt = new MetaExt();
            if (args.length > 0) {
                if (metaExt.validateRunMode( args[0] )) {
                    metaExt.execute();
                }
            } else {
                metaExt.printUsage();
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
        session = new Session( "Session_For_MetaExtension", "Session_For_MetaExtension",
                "This is session for MetaExtension" );
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
