# warp10-ext-influxdb
WarpScript™ extension to fetch data from InfluxDB

## Building

```
./gradlew shadowJar
```

## Installing

Once the extension is built, copy the `warp10-ext-influxdb.jar` file from `build/libs` into the `lib` directory of your Warp 10™ installation.

Alternatively, the extension is available on [WarpFleet™(https://warpfleet.senx.io/).

You need to add the following line in your Warp 10™ configuration file and restart your Warp 10™ instance.

```
warpscript.extension.influxdb = io.warp10.script.ext.influxdb.InfluxDBWarpScriptExtension
```

## Usage

This WarpScript™ extension adds a function called `INFLUXDBFETCH` which will fetch data from InfluxDB and convert the fetched data into Geo Time Series™ which you can then manipulate using WarpScript™.

The function is invoked as shown below

```
'URL'      // URL of the InfluxDB endpoint, i.e. http://127.0.0.1:8086/
'username' // Username to use for authentication, put a dummy value if not using authentication
'password' // Password associated with 'username', use a dummy value if not using authentication
'DB'       // Name of the InfluxDB to connect to
'QUERIES'  // InfluxQL queries (separated by ';').
           // Queries should contain a GROUP BY clause otherwise tags will produce new Geo Time Series.
           // The result is a list of lists of Geo Time Series, one inner list per query.
INFLUXDBFETCH
```

## Security

There is no control of the provided URL, so a rogue user could issue calls to internal services this way. Consider opening an issue or submitting a PR if you would like to have configuration options to further restrict the list of allowed URLs.
