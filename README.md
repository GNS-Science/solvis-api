# solvis-api

a serverless web API for analysis of opensha modular Inversion Solutions


## Getting started

```
virtualenv solvis-api
npm install --save serverless-dynamodb-local
npm install --save serverless-python-requirements
npm install --save serverless-wsgi
sls dynamodb install
```

### WSGI

```
sls wsgi serve
```

### Run
sls dynamodb start --stage dev &
sls offline-sns start &
sls wsgi serve