/*
* AggregatorForInformix.java Created on Jul 5, 2005.
* 
* Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
* PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
*/
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;

public class AggregatorForInformix extends Aggregator {
	   /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
    	orderDetailSource = this.createOrderDetailSource();
    	ConnectionInfo connInfo = getRelationalConnectionInfo(SourceTargetTypes.RELATIONAL_TYPE_INFORMIX);
    	connInfo.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,"toolsdevelop");
    	//set the new ConnectionInfo 
    	orderDetailSource.setConnInfo(connInfo);
    	folder.addSource( orderDetailSource );
    }
    
    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
    	outputTarget = this.createRelationalTarget( "Aggregate_Output_Informix", SourceTargetTypes.RELATIONAL_TYPE_INFORMIX);
    }
    
    public static void main(String[] args) {
		try {
			AggregatorForInformix aggregator = new AggregatorForInformix();
		    if (args.length > 0) {
		        if (aggregator.validateRunMode(args[0])){
		           aggregator.execute();
		        }
		    }else {
		        aggregator.printUsage();
		    }
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println( "Exception is: " + e.getMessage() );						
		}
	}       
}
