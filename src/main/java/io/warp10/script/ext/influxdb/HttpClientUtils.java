//
//   Copyright 2021  SenX S.A.S.
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.warp10.script.WarpScriptException;
import okhttp3.OkHttpClient.Builder;

public class HttpClientUtils {

    private static final String KEY_READ_TIMEOUT = "readTimeout";
    private static final String KEY_WRITE_TIMEOUT = "writeTimeout";

    public static Builder getOkHttpClientBuilder(final String functionName, final Map<Object, Object> params) throws WarpScriptException {

        Builder result = new Builder();
        result.readTimeout(getLongValueFromKey(functionName, params, KEY_READ_TIMEOUT, result.getReadTimeout$okhttp()), TimeUnit.MILLISECONDS);
        result.writeTimeout(getLongValueFromKey(functionName, params, KEY_WRITE_TIMEOUT, result.getWriteTimeout$okhttp()), TimeUnit.MILLISECONDS);

        return result;
    }

    private static long getLongValueFromKey(final String functionName, final Map<Object, Object> params, final String key, final long defaultValue) throws WarpScriptException {

        Object value = params.getOrDefault(key, defaultValue);
        if (!(value instanceof Long)) {
            throw new WarpScriptException(functionName + " expects a Long value for the " + key + " parameter.");
        }

        return (long) value;
    }

}
