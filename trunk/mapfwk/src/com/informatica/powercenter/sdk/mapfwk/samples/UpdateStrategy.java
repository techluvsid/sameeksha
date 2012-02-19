/*
 * UpdateStrategy.java Created on Jun 24, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SessionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * Sample for Update Strategy transformation using Mapping framework API
 * 
 */
public class UpdateStrategy extends Base {
    protected Source employeeSrc;
    protected Target outputTarget;

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
        outputTarget = this.createFlatFileTarget( "UpdateStrategy_Output" );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "UpdateStrategyMapping", "UpdateStrategyMapping",
                "This is Update Strategy sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create a Update Strategy Transformation
        // Insert only if the city is 'Seattle' else reject it
        RowSet filterRS = (RowSet) helper.updateStrategy( dsqRS,
                "IIF(City = 'Seattle', DD_INSERT, DD_REJECT )", "updateStrategy_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( filterRS, outputTarget );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_UpdateStrategy", "Session_For_UpdateStrategy",
                "This is session for Update Strategy" );
        session.getProperties().setProperty( SessionPropsConstants.TREAT_SOURCE_ROWS_AS,
                "Data driven" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_UpdateStrategy", "Workflow_for_UpdateStrategy",
                "This workflow for UpdateStrategy" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String[] args ) {
        try {
            UpdateStrategy updateStgy = new UpdateStrategy();
            if (args.length > 0) {
                if (updateStgy.validateRunMode( args[0] )) {
                    updateStgy.execute();
                }
            } else {
                updateStgy.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
