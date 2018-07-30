# mongodb-apm
MongoDB APM integrations

* Experimenting with APM integrations for MongoDB

## Assumptions
* You have a MongoDB instance running on `localhost:27017`
with a DB called `media-searches` and some content with a key called `key`
inside a collection `youtube-searches`

### Installing it
```shell
mvn install
```

### Running it
* With your GCP account that has Stackdriver Tracing and Monitoring enabled, get the credentials into a .json file and then run the CLI like this
```shell
GOOGLE_APPLICATION_CREDENTIALS=credentials.json mvn exec:java -Dexec.mainClass=io.opencensus.apm.Inspector
```

and after that, go to Stackdriver Monitoring to see some metrics but also
they'll be posted on the CLI as you type
