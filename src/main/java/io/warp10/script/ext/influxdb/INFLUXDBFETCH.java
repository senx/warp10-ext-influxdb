//
//   Copyright 2018-2021  SenX S.A.S.
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

import okhttp3.OkHttpClient.Builder;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class INFLUXDBFETCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
  public static final String KEY_DB = "db";
  private static final String KEY_INFLUXQL = "influxql";

  public INFLUXDBFETCH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    String influxql;
    String dbName;
    String password;
    String username;
    String url;
    Builder okHttpClientBuilder = null;
    if (top instanceof Map) {
      Map<Object,Object> params = (Map<Object,Object>) top;
      influxql = String.valueOf(params.get(KEY_INFLUXQL));
      dbName = String.valueOf(params.get(KEY_DB));
      password = String.valueOf(params.get(INFLUXDBFLUX.KEY_PASSWORD));
      username = String.valueOf(params.get(INFLUXDBFLUX.KEY_USER));
      url = String.valueOf(params.get(INFLUXDBFLUX.KEY_URL));
      okHttpClientBuilder = HttpClientUtils.getOkHttpClientBuilder(getName(), params);
    } else {
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects an InfluxQL query.");
      }
      
      influxql = top.toString();
      
      top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a database name before the query.");
      }
      
      dbName = top.toString();
      
      top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a password before the dbname.");
      }
      
      password = top.toString();
      
      top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a user name before the password.");
      }
      
      username = top.toString();
      
      top = stack.pop();
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a database URL before the username.");
      }
      
      url = top.toString();
    }

    InfluxDB influxdb;
    if (null != okHttpClientBuilder) {
      influxdb = InfluxDBFactory.connect(url, username, password, okHttpClientBuilder);
    } else {
      influxdb = InfluxDBFactory.connect(url, username, password);
    }

    Query query = new Query(influxql, dbName, true);
    
    // Request timestamps as nanoseconds so we do not waste time parsing timestamps
    QueryResult results = influxdb.query(query, TimeUnit.NANOSECONDS);
           
    List<List<GeoTimeSerie>> allgts = new ArrayList<List<GeoTimeSerie>>();
    
    for (Result result: results.getResults()) {
      
      List<GeoTimeSerie> resultgts = new ArrayList<GeoTimeSerie>();

      allgts.add(resultgts);
      
      List<Series> series = result.getSeries();

      Metadata meta = new Metadata();
      
      for (Series serie: series) {
        
        // Set labels
        if (meta.getLabelsSize() > 0) {
          meta.getLabels().clear();
        }
        
        if (null != serie.getTags()) {
          meta.setLabels(serie.getTags());
        } else {
          meta.setLabels(new HashMap<String,String>());
        }
        
        String measurement = serie.getName();

        List<List<Object>> allvalues = serie.getValues();
        
        // Loop over the columns, column 0 is the timestamp
        for (int i = 1; i < serie.getColumns().size(); i++) {
          meta.setName(measurement + " " + serie.getColumns().get(i));
          
          int hint = serie.getValues().size();
          GeoTimeSerie gts = new GeoTimeSerie(hint);          
          gts.setMetadata(meta);
          
          resultgts.add(gts);
          
          for (List<Object> values: allvalues) {            
            long timestamp = 0L;
            if (1 == i) {
              long dts = (long) ((double) values.get(0));

              timestamp += dts / (1000000000L / Constants.TIME_UNITS_PER_S);
              
              // Set timestamp
              values.set(0, timestamp);
            } else {
              timestamp = (long) values.get(0);
            }
                                   
            Object value = values.get(i);

            if (null == value) {
              continue;
            }

            GTSHelper.setValue(gts, timestamp, value);
          }                    
        }         
      }
    }
        
    influxdb.close();
    
    stack.push(allgts);
    
    return stack;
  }
}
