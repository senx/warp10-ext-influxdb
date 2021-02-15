//
//   Copyright 2020-2021  SenX S.A.S.
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import okhttp3.OkHttpClient.Builder;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class INFLUXDBFLUX extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String TABLE_LABEL = "_table";
  
  public static final String KEY_URL = "url";
  public static final String KEY_TOKEN = "token";
  public static final String KEY_ORG = "org";
  public static final String KEY_USER = "user";
  public static final String KEY_PASSWORD = "password";
  private static final String KEY_FLUX = "flux";

  public INFLUXDBFLUX(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map.");
    }
    
    Map<Object,Object> params = (Map<Object,Object>) top;
    
    if (!params.containsKey(KEY_FLUX) || !(params.get(KEY_FLUX) instanceof String)) {
      throw new WarpScriptException(getName() + " missing '" + KEY_FLUX + "' parameter.");
    }
    
    String query = (String) params.get(KEY_FLUX);
    
    InfluxDBClientOptions.Builder builder = new InfluxDBClientOptions.Builder();

    if (!params.containsKey(KEY_URL) || !(params.get(KEY_URL) instanceof String)) {
      throw new WarpScriptException(getName() + " missing valid '" + KEY_URL + "' parameter.");
    }
    
    builder.url((String) params.get(KEY_URL));
    
    if (params.containsKey(KEY_TOKEN) && params.get(KEY_TOKEN) instanceof String) {
      builder.authenticateToken(((String) params.get(KEY_TOKEN)).toCharArray());
    } else if (params.get(KEY_USER) instanceof String && params.get(KEY_PASSWORD) instanceof String) {
      builder.authenticate((String) params.get(KEY_USER), ((String) params.get(KEY_PASSWORD)).toCharArray()); 
    } else {
      throw new WarpScriptException(getName() + " missing key '" + KEY_TOKEN + "' or keys '" + KEY_USER + "' and '" + KEY_PASSWORD + "'.");
    }
    
    if (!(params.get(KEY_ORG) instanceof String)) {
      throw new WarpScriptException(getName() + " missing valid '" + KEY_ORG + "' parameter.");
    }
    
    builder.org((String) params.get(KEY_ORG));

    final Builder okHttpClientBuilder = HttpClientUtils.getOkHttpClientBuilder(getName(), params);
    builder.okHttpClient(okHttpClientBuilder);

    InfluxDBClientOptions options = builder.build();

    InfluxDBClient client = null;

    try {
      client = InfluxDBClientFactory.create(options);

      //
      // We can use GeoTimeSerie instances because a table only contains elements of the same type
      //
      final Map<Map<String,String>,GeoTimeSerie> gts = new HashMap<Map<String,String>, GeoTimeSerie>();

      QueryApi api = client.getQueryApi();
      List<FluxTable> tables = api.query(query);

      for (FluxTable table: tables) {
        List<FluxColumn> group = table.getGroupKey();

        Map<String,String> labels = null;
        GeoTimeSerie series = null;

        List<FluxRecord> records = table.getRecords();

        for (FluxRecord record: records) {
          if (null == labels) {
            labels = new HashMap<String,String>(group.size());
            for (FluxColumn col: group) {
              // Ignore _start, _stop
              if ("_start".equals(col.getLabel()) || "_stop".equals(col.getLabel())) {
                continue;
              }
              labels.put(col.getLabel(), String.valueOf(record.getValueByIndex(col.getIndex())));
            }
            String table_label = TABLE_LABEL;
            while(labels.containsKey(table_label)) {
              table_label = "_" + table_label;
            }
            labels.put(table_label, record.getTable().toString());

            series = gts.get(labels);

            if (null == series) {
              series = new GeoTimeSerie(records.size());
              gts.put(labels, series);
              series.setLabels(labels);
              series.setName(record.getTable().toString() + " " + record.getMeasurement() + " " + record.getField());
            }
          }

          Instant instant = record.getTime();
          long ts = instant.getEpochSecond() * 1000000000L + instant.getNano();
          // Convert to platform time units
          ts = ts / (1000000L / Constants.TIME_UNITS_PER_MS);
          GTSHelper.setValue(series, ts, record.getValue());
        }
      }

      List<GeoTimeSerie> fetched = new ArrayList<GeoTimeSerie>(gts.values());
      stack.push(fetched);
    } catch (Throwable t) {
      throw new WarpScriptException(getName() + " error reading data.", t);
    } finally {
      if (null != client) {
        client.close();
      }
    }
    
    return stack;
  }



}
