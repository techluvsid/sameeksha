/*
 * AggregatorForOracle.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;

/**
 * 
 * TODO 
 * 
 * @author rshashik
 * @version %I%
 *
 */
public class AggregatorForOracle extends Aggregator{

    
    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
    	orderDetailSource = this.createOrderDetailSource();
    	ConnectionInfo connInfo = getRelationalConnectionInfo(SourceTargetTypes.RELATIONAL_TYPE_ORACLE);
    	// The connection object "Oracle_src" should be present in the Power Center Repository. Please create 
    	// a connection object with a name and use the same to set the property of CONNECTIONNAME.
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.CONNECTIONNAME, "Oracle_src" );
    	connInfo.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,"toolsdevelop");
    	//set the new ConnectionInfo 
    	orderDetailSource.setConnInfo(connInfo);
    	folder.addSource( orderDetailSource );
    }
    
    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
    	outputTarget = this.createRelationalTarget( "Aggregate_Output_Oracle", SourceTargetTypes.RELATIONAL_TYPE_ORACLE);
    }
    
    /**
     * Create source for Employee Source
     */
    protected Source createOrderDetailSource() {
        Vector fields = new Vector();
        Field field1 = new Field("OrderID", "OrderID","", DataTypeConstants.DECIMAL, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field1);

        Field field2 = new Field("ProductID", "ProductID","", DataTypeConstants.DECIMAL, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field2);

        Field field3 = new Field("UnitPrice", "UnitPrice","", DataTypeConstants.DECIMAL, "28", "4",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field3);

        Field field4 = new Field("Quantity", "Quantity","", DataTypeConstants.DECIMAL, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field4);

        Field field5 = new Field("Discount", "Discount","", DataTypeConstants.DECIMAL, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field5);

        Field field6 = new Field("VarcharFld", "VarcharFld","", DataTypeConstants.VARCHAR, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field6);

        Field field7 = new Field("Varchar2Fld", "Varchar2Fld","", DataTypeConstants.VARCHAR2, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field7);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Order_Details.csv");
        Source ordDetailSource = new Source( "OrderDetail", "OrderDetail", "This is Order Detail Table", "OrderDetail", info );
        addFieldsToSource( ordDetailSource, fields );
        return ordDetailSource;
    }
    
    public static void main(String[] args) {
		try {
		    AggregatorForOracle aggregator = new AggregatorForOracle();
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
