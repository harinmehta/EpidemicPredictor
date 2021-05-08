# EpidemicPredictor
Predicts the next epidemic by analyzing twitter streams

## Start the service
GET localhost:8080/start<br>
RequestBody : 
```
{
    "southWestLongitude" : 78.5122,
    "southWestLatitude" : 11.5330,
    "northEastLongitude" : 88.6569,
    "northEastLatitude" : 27.1271,
    "terms" : ["vegan", "veganism"]
}
```
Response : No response (the service will keep on running in the background)

## Get Stats
GET localhost:8080/analyse <br>
Request: no request required <br>
Response:
```
{
    "Very Negative" : 2,
    "Negative" : 5,
    "Neutral" : 14,
    "Positive" : 20,
    "Very Positive" : 12
}
```
## Stop the service
GET localhost:8080/stop <br>
Request: no request <br>
Response: no response

