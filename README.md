Available scripts:
- `com.pepperdata.BulkWrite` (Bulk write with multi-threading - divide host list among thread)
- `com.pepperdata.BulkWrite2` (Bulk write with multi-threading - divide host list among thread)
- `com.pepperdata.Write` (Simple write script need to define file to be processed with in script)
- `com.pepperdata.Read` (read using java - incomplete due to lack of resources)

Variables in script:

`writerUrl = "http://127.0.0.1:7201/api/v1/prom/remote/write";` (m3db write url)

`dataDir = "static/data";` (place all file with in this directory to process)
note: file will be removed once it gets processed.

SET WRITE SCRIPT:

in `build.gradle` update `javaMainClass` with script class.

RUN SCRIPT:

`./gradlew runWithJavaExec`

