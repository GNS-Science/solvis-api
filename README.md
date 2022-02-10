# solvis-api

a serverless web API for analysis of opensha modular Inversion Solutions


The API openAPI/Swagger documentation is served by default from the service root.


## Getting started

```
virtualenv solvis-api
npm install --save serverless
npm install --save serverless-dynamodb-local
npm install --save serverless-python-requirements
npm install --save serverless-wsgi
sls dynamodb install
```

### WSGI

```
sls wsgi serve
```

### Run full stack locally
```
sls dynamodb start --stage dev &\
SLS_OFFLINE=1 sls offline-sns start &\
SLS_OFFLINE=1 sls wsgi serve
```

### Unit tests

`TESTING=1 nosetests -v --nologcapture`

**TESTING** overrides **SLS_OFFLINE** to keep moto mockling happy
