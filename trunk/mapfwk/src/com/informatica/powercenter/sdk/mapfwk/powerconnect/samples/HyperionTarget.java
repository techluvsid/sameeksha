/*
 * HyperionTarget.java Created on May 12, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.powerconnect.samples;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectConInfo;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectTarget;
import com.informatica.powercenter.sdk.mapfwk.util.INIFile;

public class HyperionTarget extends PowerConnectTarget {

	/**
	 * Creates a Hyperion Source
	 *
	 * @param name
	 * @param busName
	 * @param desc
	 * @param instName
	 * @param connInfo
	 */
	public HyperionTarget( String name, String busName, String desc, String instName, PowerConnectConInfo connInfo ) {
		super( name, busName, desc, instName, connInfo );    	// The connection object "Essbase" should be present in the Power Center Repository. Please create     	// a connection object with a name and use the same to set the property of CONNECTIONNAME.		getConnInfo().getConnProps().setProperty( ConnectionPropsConstants.CONNECTIONNAME, "Essbase" );	
	}
	
	/**
	 *
	 * @param String sdkDataType
	 */
	public String getPowerConnectDataType(String sdkDataType) {
		INIFile dbTypesIni = INIFile.getInstance("HyperionMappings.ini");
		return dbTypesIni.getStringProperty(String.valueOf(1), sdkDataType);
	}
	/**
	 *
	 * @param String pcDataType
	 */
	public String getSDKDataType( String pcDataType ) {
		INIFile dbTypesIni = INIFile.getInstance("HyperionMappings.ini");
		return dbTypesIni.getStringProperty(String.valueOf(0), pcDataType );
	}

	public String getWriterName() {
		return "EssbaseConnector_WRITER";
	}
}
