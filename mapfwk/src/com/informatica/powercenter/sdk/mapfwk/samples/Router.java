/*
 * Router.java Created on Nov 4, 2005.
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
import com.informatica.powercenter.sdk.mapfwk.core.TransformGroup;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * Sample for Router transformation using Mapping framework API
 * 
 * 
 */
public class Router extends Base {
    protected Source employeeSrc;
    protected Target londonOutputTarget;
    protected Target seattleOutputTarget;
    protected Target defaultOutputTarget;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        employeeSrc = this.createEmployeeSource();
        folder.addSource( employeeSrc );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
        londonOutputTarget = this.createFlatFileTarget( "CityLondon_Router_Output" );
        seattleOutputTarget = this.createFlatFileTarget( "CitySeattle_Router_Output" );
        defaultOutputTarget = this.createFlatFileTarget( "Default_Router_Output" );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "RouterMapping", "RouterMapping",
                "This is Router Transformation sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // Create a TransformGroup
        Vector vTransformGrp = new Vector();
        TransformGroup transGrp = new TransformGroup( "LONDON_GROUP", "City = 'London'" );
        vTransformGrp.add( transGrp );
        transGrp = new TransformGroup( "SEATTLE_GROUP", "City = 'Seattle'" );
        vTransformGrp.add( transGrp );
        // creating DSQ Transformation
        OutputSet itemOSet = helper.sourceQualifier( employeeSrc );
        RowSet employeeRowSet = (RowSet) itemOSet.getRowSets().get( 0 );
        // create a Router Transformation
        OutputSet routerOutputSet = helper.router( employeeRowSet, vTransformGrp,
                "Router_transform" );
        // write to target
        RowSet outRS = routerOutputSet.getRowSet( "LONDON_GROUP" );
        if (outRS != null)
            mapping.writeTarget( outRS, londonOutputTarget );
        outRS = routerOutputSet.getRowSet( "SEATTLE_GROUP" );
        if (outRS != null)
            mapping.writeTarget( outRS, seattleOutputTarget );
        outRS = routerOutputSet.getRowSet( "DEFAULT" );
        if (outRS != null)
            mapping.writeTarget( outRS, defaultOutputTarget );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Router", "Session_For_Router",
                "This is session for Router" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Router", "Workflow_for_filter",
                "This workflow for Router" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String[] args ) {
        try {
            Router router = new Router();
            if (args.length > 0) {
                if (router.validateRunMode( args[0] )) {
                    router.execute();
                }
            } else {
                router.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
