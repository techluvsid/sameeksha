/*
 * GenerateTransformationProperties.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.File;
import java.io.FileOutputStream;
import com.informatica.powercenter.sdk.mapfwk.core.TransformPropsConstants;

/**
 * Class to generate transformation properties
 * 
 */
public class GenerateTransformationProperties {
    /**
     * main method
     */
    public static void main( String[] args ) {
        String fileName = System.getProperty( "user.dir" ) + File.separator + "transformProps.xml";
        try {
            System.out.println( "Writing to fileName :[" + fileName + "]...." );
            File file = new File( fileName );
            FileOutputStream fOut = new FileOutputStream( file );
            fOut.write( TransformPropsConstants.generateXML().getBytes() );
            System.out.println( "Completed writing to fileName :[" + fileName + "]...." );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
