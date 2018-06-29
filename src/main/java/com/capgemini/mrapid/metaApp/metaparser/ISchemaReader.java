package com.capgemini.mrapid.metaApp.metaparser;

import java.io.FileReader;
import java.util.ArrayList;

/**
 * Schema Reader Interface
 * @author Anurag Udasi
 *
 */
public interface ISchemaReader {

	public ArrayList<String> scanSchemaFile(FileReader reader);

}
