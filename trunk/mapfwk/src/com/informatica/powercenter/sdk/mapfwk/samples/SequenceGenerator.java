/*
 * SequenceGenerator.java Created on Jun 28, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * Sample for Update Strategy transformation using Mapping framework API
 * 
 */
public class SequenceGenerator extends Base {
    protected Source orderDetailSrc;
    protected Target outputTarget;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        orderDetailSrc = this.createOrderDetailSource();
        folder.addSource( orderDetailSrc );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "SequenceGenerator_Output" );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "SequenceGeneratorMapping", "SequenceGeneratorMapping",
                "This is Sequence Generator sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( orderDetailSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create a Sequence Generator Transformation
        RowSet seqGenRS = (RowSet) helper.sequenceGenerator( "sequencegenerator_transform" )
                .getRowSets().get( 0 );
        Vector vinSets = new Vector();
        vinSets.add( new InputSet( dsqRS ) );
        vinSets.add( new InputSet( seqGenRS ) );
        // write to target
        mapping.writeTarget( vinSets, outputTarget );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_SequenceGenerator", "Session_For_SequenceGenerator",
                "This is session for Sequence Generator" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_SequenceGenerator",
                "Workflow_for_SequenceGenerator", "This workflow for Sequence Generator" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String[] args ) {
        try {
            SequenceGenerator seqGen = new SequenceGenerator();
            if (args.length > 0) {
                if (seqGen.validateRunMode( args[0] )) {
                    seqGen.execute();
                }
            } else {
                seqGen.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
