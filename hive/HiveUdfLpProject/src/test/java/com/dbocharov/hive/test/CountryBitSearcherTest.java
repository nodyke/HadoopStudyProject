package com.dbocharov.hive.test;

import com.dbocharov.hive.udf.CountryBinarySearcher;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.junit.Test;
import static org.junit.Assert.*;

public class CountryBitSearcherTest {
    private static final String GEODATA_PATH = "geodata.csv";
    @Test
    public void testSuccessCountryBinarySearcher() {
        String excepted = "Австралия";

        CountryBinarySearcher countryBinarySearcher = new CountryBinarySearcher();
        GenericUDF.DeferredObject deferredObject = new GenericUDF.DeferredJavaObject("1.0.0.1");
        countryBinarySearcher.setGeodata_path(GEODATA_PATH);
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{deferredObject};
        try {
            Object result = countryBinarySearcher.evaluate(deferredObjects).toString();
            assertEquals(excepted,result);
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testNotFoundCountry() {
        String excepted = "NONE";

        CountryBinarySearcher countryBinarySearcher = new CountryBinarySearcher();
        GenericUDF.DeferredObject deferredObject = new GenericUDF.DeferredJavaObject("0.0.0.0");
        countryBinarySearcher.setGeodata_path(GEODATA_PATH);
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{deferredObject};
        try {
            Object result = countryBinarySearcher.evaluate(deferredObjects).toString();
            assertEquals(excepted,result);
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }
}
