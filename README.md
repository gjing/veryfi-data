# VeryFi Data project

Run
```docker compose up```
Then go to
```
http://localhost:3001/locations/pipeline/jobs/load_items
```
Click Launchpad tap, then click Launch Run
Verify the data has been transferred with mongodb compass or the flask api
default is
```mongodb://root:pass@localhost:27017```

## Tech used

### Dagster
* simple data orchestration pipeline
* works well with poetry and docker
* lightweight, scalabe
* see pipeline/pipeline/assets.py for more details

### Mongodb
* simple, horizontally-scalable, noSQL document db
* schemaless

### Docker
* Docker and Docker compose for local dev
* prod would use Docker Swarm or Kubernetes for Dagster and a separate db instance
* 

### Flask
* Not part of project reqs, just needed a simple api