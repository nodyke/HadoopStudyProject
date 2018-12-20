package com.dbocharov.hive.udf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class CountryBinarySearcher extends GenericUDF {


    private final static String IP_REGEX = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";
    private final static String NONE = "NONE";

    private String geodata_path = "hdfs://localhost/user/hive/warehouse/events.db/geodata/000000_0";


    private final GenericUDFUtils.ReturnObjectInspectorResolver returnObjectInspectorResolver = new GenericUDFUtils
            .ReturnObjectInspectorResolver(true);

    private TreeMap<Range, String> country_bit_map = null;

    public void setGeodata_path(String geodata_path) {
        this.geodata_path = geodata_path;
    }

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length == 1 && checkFirstArgument(objectInspectors[0])) {
            returnObjectInspectorResolver.update(objectInspectors[0]);
            return returnObjectInspectorResolver.get();
        }
        throw new UDFArgumentException("Not valid arguments, should be 1 and should be primitive");
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Long ip = ipToLong(deferredObjects[0].get().toString());
        if (country_bit_map == null) {
            initCountryMap(geodata_path);
        }
        Map.Entry<Range, String> range_entry = country_bit_map.higherEntry(Range.createRangeFromNumber(ip));
        if (range_entry != null) return new Text(range_entry.getValue());
        return new Text(NONE);
    }

    public void initCountryMap(String path) throws HiveException {
        country_bit_map = new TreeMap<>(new Comparator<Range>() {
            @Override
            public int compare(Range o1, Range o2) {
                return o2.getStart().compareTo(o1.getEnd());
            }
        });
        initLookup(path);

    }

    //Csv string like network_start_integer,network_end_integer,geoname_id,country_name.
    //Example: 3758096128,3758096383,2077456,Австралия
    private void putCountryFromCsvString(String csv) {
        String[] temp = csv.split(",");
        Long start = Long.parseLong(temp[0]);
        Long end = Long.parseLong(temp[1]);
        String country_name = temp[3];
        country_bit_map.put(new Range(start, end), country_name);
    }


    public String getDisplayString(String[] strings) {
        return "Get country name from ip";
    }

    private void initLookup(String lookupFile) throws HiveException {
        Configuration conf = new Configuration();
        Path filePath = new Path(lookupFile);
        try (FileSystem fs = FileSystem.get(filePath.toUri(), conf);
             FSDataInputStream dis = fs.open(filePath);
             BufferedReader br = new BufferedReader(new InputStreamReader(dis))
        ) {
            String current_line;
            while ((current_line = br.readLine()) != null) {
                putCountryFromCsvString(current_line);
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }


    private boolean checkIp(String ip) {
        return ip.matches(IP_REGEX);
    }

    private boolean checkFirstArgument(ObjectInspector objectInspector) {
        return ObjectInspector.Category.PRIMITIVE.equals(objectInspector.getCategory());
    }

    private Long ipToLong(String ip) throws HiveException {
        if (!checkIp(ip)) throw new HiveException("Not valid ip address");
        String[] temp = ip.split("\\.");

        long result = 0;
        for (int i = 0; i < temp.length; i++) {
            int power = 3 - i;
            int _ip = Integer.parseInt(temp[i]);
            result += _ip * Math.pow(256, power);
        }
        return result;
    }
}
