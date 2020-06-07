//
//   Copyright 2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.script.ext.influxdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import com.geoxp.GeoXPLib;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class INFLUXDBUPDATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String KEY_BUCKET = "bucket";
  private static final String KEY_MEASUREMENT = "measurement";
  private static final String KEY_BATCHSIZE = "batchsize";
  private static final String KEY_V1 = "v1";
  private static final String KEY_ATTR = "attr";
  
  public static final String FIELD_ELEVATION = "elev";
  public static final String FIELD_LATITUDE = "lat";
  public static final String FIELD_LONGITUDE = "lon";
  
  private static final WritePrecision PRECISION;
  
  private static final int MAX_BATCH_SIZE = 10000;
  
  static {
    if (1000000000L == Constants.TIME_UNITS_PER_S) {
      PRECISION = WritePrecision.NS;      
    } else if (1000000L == Constants.TIME_UNITS_PER_S) {
      PRECISION = WritePrecision.US;
    } else if (1000L == Constants.TIME_UNITS_PER_S) {
      PRECISION = WritePrecision.MS;
    } else {
      throw new RuntimeException("Invalid time units found!");
    }
  }
  
  public INFLUXDBUPDATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map.");
    }
    
    Map<Object,Object> params = (Map<Object,Object>) top;

    if (Boolean.TRUE.equals(params.get(KEY_V1))) {
      //
      // InfluxDB 1.x
      //
      
      if (!(params.get(INFLUXDBFLUX.KEY_URL) instanceof String)) {
        throw new WarpScriptException(getName() + " missing key '" + INFLUXDBFLUX.KEY_URL + "'.");
      }
      String url = (String) params.get(INFLUXDBFLUX.KEY_URL);
      
      if (!(params.get(INFLUXDBFLUX.KEY_USERNAME) instanceof String)) {
        throw new WarpScriptException(getName() + " missing key '" + INFLUXDBFLUX.KEY_USERNAME + "'.");
      }
      String username = (String) params.get(INFLUXDBFLUX.KEY_USERNAME);
      
      if (!(params.get(INFLUXDBFLUX.KEY_PASSWORD) instanceof String)) {
        throw new WarpScriptException(getName() + " missing key '" + INFLUXDBFLUX.KEY_PASSWORD + "'.");
      }
      
      String password = (String) params.get(INFLUXDBFLUX.KEY_PASSWORD);
      
      if (!(params.get(KEY_MEASUREMENT) instanceof String)) {
        throw new WarpScriptException(getName() + " missing key '" + KEY_MEASUREMENT + "'.");
      }

      String measurement = (String) params.get(KEY_MEASUREMENT);

      String measurementAttr = null;
      
      if (params.get(KEY_ATTR) instanceof String) {
        measurementAttr = (String) params.get(KEY_ATTR);
      }
      
      List<Object> data;
      
      if (top instanceof List) {
        data = (List<Object>) top;
      } else if (top instanceof GeoTimeSerie || top instanceof GTSEncoder) {
        data = new ArrayList<Object>();
        data.add(top);
      } else {
        throw new WarpScriptException(getName() + " expects a Geo Time Series or an Encoder, or a list thereof.");
      }
      
      InfluxDB influxdb = null;
      
      try {
        influxdb = InfluxDBFactory.connect(url, username, password);
        influxdb.enableBatch(BatchOptions.DEFAULTS);
        
        long units = 1000000000L / Constants.TIME_UNITS_PER_S;
        
        for (Object elt: data) {
          // One batch per GTS/Encoder
          BatchPoints.Builder batch = BatchPoints.builder();
          if (elt instanceof GeoTimeSerie) {
            GeoTimeSerie gts = (GeoTimeSerie) elt;
            int n = GTSHelper.nvalues(gts);
            Map<String,String> labels = gts.getLabels();
            
            //
            // Extract the measurement from the attributes or use the default one
            //
            String name = gts.getName();
            String mes = measurement;
            if (gts.getMetadata().getAttributesSize() > 0 && gts.getMetadata().getAttributes().containsKey(measurementAttr)) {
              mes = gts.getMetadata().getAttributes().get(measurementAttr);
            }
            
            for (int i = 0; i < n; i++) {
              org.influxdb.dto.Point.Builder point = org.influxdb.dto.Point.measurement(mes);
              point.tag(labels);
              point.time(GTSHelper.tickAtIndex(gts, i) * units, TimeUnit.NANOSECONDS);
              Object value = GTSHelper.valueAtIndex(gts, i);
              if (value instanceof Number) {
                point.addField(name, (Number) value);
              } else if (value instanceof String) {
                point.addField(name, (String) value);
              } else {
                point.addField(name, (Boolean) value);
              }
              batch.point(point.build());
            }
          } else if (elt instanceof GTSEncoder) {
            GTSEncoder encoder = (GTSEncoder) elt;
            GTSDecoder decoder = encoder.getDecoder();

            Map<String,String> labels = encoder.getLabels();
            
            //
            // Extract the measurement from the attributes or use the default one
            //
            String name = encoder.getName();
            String mes = measurement;
            if (encoder.getMetadata().getAttributesSize() > 0 && encoder.getMetadata().getAttributes().containsKey(measurementAttr)) {
              mes = encoder.getMetadata().getAttributes().get(measurementAttr);
            }
            
            while(decoder.next()) {
              org.influxdb.dto.Point.Builder point = org.influxdb.dto.Point.measurement(mes);
              point.tag(labels);
              point.time(decoder.getTimestamp() * units, TimeUnit.NANOSECONDS);
              Object value = decoder.getValue();
              if (value instanceof Number) {
                point.addField(name, (Number) value);
              } else if (value instanceof String) {
                point.addField(name, (String) value);
              } else {
                point.addField(name, (Boolean) value);
              }
              batch.point(point.build());
            }
          } else {
            throw new WarpScriptException(getName() + " expects a Geo Time Series or an Encoder, or a list thereof.");
          }          
          influxdb.write(batch.build());
        }
      } catch (Exception e) {
        throw new WarpScriptException(getName() + " error while writing data points.", e);
      } finally {
        if (null != influxdb) {
          influxdb.close();
        }
      }      
    } else {
      //
      // InfluxDB 2.x
      //
      InfluxDBClientOptions.Builder builder = new InfluxDBClientOptions.Builder();

      if (!params.containsKey(INFLUXDBFLUX.KEY_URL) || !(params.get(INFLUXDBFLUX.KEY_URL) instanceof String)) {
        throw new WarpScriptException(getName() + " missing valid '" + INFLUXDBFLUX.KEY_URL + "' parameter.");
      }
      
      builder.url((String) params.get(INFLUXDBFLUX.KEY_URL));
      
      if (params.containsKey(INFLUXDBFLUX.KEY_TOKEN) && params.get(INFLUXDBFLUX.KEY_TOKEN) instanceof String) {
        builder.authenticateToken(((String) params.get(INFLUXDBFLUX.KEY_TOKEN)).toCharArray());
      } else if (params.containsKey(INFLUXDBFLUX.KEY_USERNAME)
          && params.containsKey(INFLUXDBFLUX.KEY_PASSWORD)
          && params.get(INFLUXDBFLUX.KEY_USERNAME) instanceof String
          && params.get(INFLUXDBFLUX.KEY_PASSWORD) instanceof String) {
        builder.authenticate((String) params.get(INFLUXDBFLUX.KEY_USERNAME), ((String) params.get(INFLUXDBFLUX.KEY_PASSWORD)).toCharArray()); 
      } else {
        throw new WarpScriptException(getName() + " missing key '" + INFLUXDBFLUX.KEY_TOKEN + "' or keys '" + INFLUXDBFLUX.KEY_USERNAME + "' and '" + INFLUXDBFLUX.KEY_PASSWORD + "'.");
      }
      
      if (!params.containsKey(INFLUXDBFLUX.KEY_ORG) || !(params.get(INFLUXDBFLUX.KEY_ORG) instanceof String)) {
        throw new WarpScriptException(getName() + " missing valid '" + INFLUXDBFLUX.KEY_ORG + "' parameter.");
      }
      
      builder.org((String) params.get(INFLUXDBFLUX.KEY_ORG));
      
      if (!params.containsKey(KEY_BUCKET) || !(params.get(KEY_BUCKET) instanceof String)) {
        throw new WarpScriptException(getName() + " missing valid '" + KEY_BUCKET + "' parameter.");
      }
      
      builder.bucket((String) params.get(KEY_BUCKET));

      if (!params.containsKey(KEY_MEASUREMENT) || !(params.get(KEY_MEASUREMENT) instanceof String)) {
        throw new WarpScriptException(getName() + " missing valid '" + KEY_MEASUREMENT + "' parameter.");      
      }
      
      String measurement = (String) params.get(KEY_MEASUREMENT);
      
      InfluxDBClientOptions options = builder.build();
      
      InfluxDBClient client = null;
      WriteApi write = null;
      
      try {
        top = stack.pop();

        List<Object> data;
        
        if (top instanceof List) {
          data = (List<Object>) top;
        } else if (top instanceof GeoTimeSerie || top instanceof GTSEncoder) {
          data = new ArrayList<Object>();
          data.add(top);
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series or an Encoder, or a list thereof.");
        }
        
        client = InfluxDBClientFactory.create(options);
        WriteOptions.Builder woptions = WriteOptions.builder();
        
        if (params.containsKey(KEY_BATCHSIZE) && params.get(KEY_BATCHSIZE) instanceof Long) {
          woptions.batchSize(Math.min(((Long) params.get(KEY_BATCHSIZE)).intValue(), MAX_BATCH_SIZE));        
        } else {
          woptions.batchSize(MAX_BATCH_SIZE >> 2);
        }
        
        write =  client.getWriteApi(woptions.build());
        
        
        for (Object elt: data) {
          if (elt instanceof GTSEncoder) {
            GTSDecoder decoder = ((GTSEncoder) elt).getDecoder(true);
            String field = decoder.getName();
            Map<String,String> tags = new HashMap<String,String>(decoder.getLabels());
            
            while(decoder.next()) {
              long ts = decoder.getTimestamp();
              long location = decoder.getLocation();
              long elevation = decoder.getElevation();
              Object value = decoder.getValue();
              
              Point point = Point.measurement(measurement);
              point.addTags(tags);
              point.time(ts, PRECISION);
              
              if (GeoTimeSerie.NO_LOCATION != location) {
                double[] latlon = GeoXPLib.fromGeoXPPoint(location);
                point.addField(FIELD_LATITUDE, (double) latlon[0]);
                point.addField(FIELD_LONGITUDE, (double) latlon[1]);
              }
              
              if (GeoTimeSerie.NO_ELEVATION != elevation) {
                point.addField(FIELD_ELEVATION, (long) elevation);
              }
              
              if (value instanceof Long) {
                point.addField(field, ((Long) value).longValue());
              } else if (value instanceof Double) {
                point.addField(field, ((Double) value).doubleValue());
              } else if (value instanceof Boolean) {
                point.addField(field, Boolean.TRUE.equals(value));
              } else if (value instanceof String) {
                point.addField(field, (String) value);
              }
              
              write.writePoint(point);
            }
          } else if (elt instanceof GeoTimeSerie) {
            GeoTimeSerie gts = (GeoTimeSerie) elt;
            String field = gts.getName();
            Map<String,String> tags = new HashMap<String,String>(gts.getLabels());
            for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
              long ts = GTSHelper.tickAtIndex(gts, i);
              long location = GTSHelper.locationAtIndex(gts, i);
              long elevation = GTSHelper.elevationAtIndex(gts, i);
              Object value = GTSHelper.valueAtIndex(gts, i);
              
              Point point = Point.measurement(measurement);
              point.addTags(tags);
              point.time(ts, PRECISION);
              
              if (GeoTimeSerie.NO_LOCATION != location) {
                double[] latlon = GeoXPLib.fromGeoXPPoint(location);
                point.addField(FIELD_LATITUDE, (double) latlon[0]);
                point.addField(FIELD_LONGITUDE, (double) latlon[1]);
              }
              
              if (GeoTimeSerie.NO_ELEVATION != elevation) {
                point.addField(FIELD_ELEVATION, (long) elevation);
              }
              
              if (value instanceof Long) {
                point.addField(field, ((Long) value).longValue());
              } else if (value instanceof Double) {
                point.addField(field, ((Double) value).doubleValue());
              } else if (value instanceof Boolean) {
                point.addField(field, Boolean.TRUE.equals(value));
              } else if (value instanceof String) {
                point.addField(field, (String) value);
              }
              
              write.writePoint(point);            
            }
          }
        }
      } catch (Exception e) {
        throw new WarpScriptException(getName() + " error writing data points.", e);
      } finally {
        if (null != write) {
          write.flush();
          write.close();
        }
        if (null != client) {
          client.close();
        }
      }      
    }

    return stack;
  }
}
