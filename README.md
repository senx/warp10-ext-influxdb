# Overview

The `warp10-ext-influxdb` extension allows WarpScript to interact with InfluxDB instances.

This extension is compatible with InfluxDB version `1.x` and `2.x`.

# Installation

## Using WarpFleet

The easiest way to install the extension is via [WarpFleet](https://warpfleet.senx.io/), simply follow the procedure on the extension's WarpFleet [page](https://warpfleet.senx.io/browse/io.warp10/warp10-ext-influxdb).

## From source

Build the extension jar by issueing the following command:

```
./gradlew -Duberjar shadowJar
```

the resulting `.jar` file will be created in the `build/libs` directory. Copy this file into the `lib` directory of your Warp 10 installation and proceed with the configuration.

# Configuration

Add the following line to your Warp 10 configuration to enable the extension:

```
warpscript.extension.influxdb = io.warp10.script.ext.influxdb.InfluxDBWarpScriptExtension
```

then restart your Warp 10 instance, the extension will be automagically added.

# Functions

Three functions are provided by this extension, `INFLUXDB.UPDATE` to store data in InfluxDB, `INFLUXDB.FETCH` to retrieve data from a `1.x` InfluxDB instance and `INFLUXDB.FLUX` to execute a flux query on a flux enabled InfluxDB instance.

# Security

There is no control of the provided endpoint URLs, so a rogue user could issue calls to internal services this way. Consider opening an issue or submitting a PR if you would like to have configuration options to further restrict the list of allowed URLs.
