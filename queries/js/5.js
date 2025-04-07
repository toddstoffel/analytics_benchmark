db.flight_data.aggregate([
    {
        "$match": {
            "destination.state": "CA",
            "year": 2020
        }
    },
    {
        "$group": {
            "_id": {
                "airline": "$carrier.name",
                "airport": "$destination.airport_name"
            },
            "volume": { "$sum": 1 },
            "avg_arrival_delay": { "$avg": "$arr_delay" }
        }
    },
    {
        "$addFields": {
            "avg_arrival_delay": { "$round": ["$avg_arrival_delay", 2] }
        }
    },
    {
        "$sort": {
            "_id.airline": 1,
            "_id.airport": 1
        }
    },
    {
        "$project": {
            "_id": 0,
            "airline": "$_id.airline",
            "airport": "$_id.airport",
            "volume": "$volume",
            "avg_arrival_delay": "$avg_arrival_delay"
        }
    }
])
