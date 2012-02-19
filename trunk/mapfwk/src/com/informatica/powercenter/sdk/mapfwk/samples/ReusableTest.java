/**
 * Expression.java Created on Oct 30, 2007.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * rjain
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.FilterTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import java.util.Vector;

/**
 * This class is an sample for a reusable transformation
 * @author rjain
 *
 */
public class ReusableTest extends Base {
    protected Source employeeSrc;
    protected Target outputTarget;
    protected Target outputTarget1;

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
        outputTarget = this.createFlatFileTarget( "Filter_Output" );
        outputTarget1 = this.createFlatFileTarget( "Filter_Output1" );
        
    }

    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "ReusbaleFilterMapping", "ReusbaleFilterMapping", "This is Reusbale filter sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        RowSet dsqRS1=(RowSet)dsqRS.clone();
        
        // create a Filter Transformation
        Vector vInputSet=new Vector();
        vInputSet.add(new InputSet(dsqRS));
        
        Vector vInputSet1=new Vector();
        vInputSet1.add(new InputSet(dsqRS1));
        
        
        //create a filter transformation creating this way because we need to set it reusable
        FilterTransformation trans=new FilterTransformation("filter_transform","","","firstFilter",mapping,vInputSet,"TRUE",null);
        trans.setReusable(true);
        
        //creating another instance of reusable transformation not that in both filter transformation we are having 
        //same filtertransformation name we are passsing different instance name
        FilterTransformation trans1=new FilterTransformation("filter_transform","","","secondFilter",mapping,vInputSet1,"TRUE",null);
        trans1.setReusable(true);
        // write to target
        
        RowSet filterRS1= (RowSet)trans.apply().getRowSets().get(0);
        RowSet filterRS2= (RowSet)trans1.apply().getRowSets().get(0);
        /*filterRS1.setName("firstFilter");
        filterRS2.setName("secondFilter");*/
        mapping.writeTarget( filterRS1, outputTarget );
        mapping.writeTarget( filterRS2, outputTarget1 );
        
        folder.addMapping( mapping );
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Filter", "Session_For_Filter",
                "This is session for filter" );
        session.setMapping( this.mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_filter", "Workflow_for_filter",
                "This workflow for filter" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
        	ReusableTest filter = new ReusableTest();
            if (args.length > 0) {
                if (filter.validateRunMode( args[0] )) {
                    filter.execute();
                }
            } else {
                filter.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
