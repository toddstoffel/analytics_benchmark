db.flight_data.aggregate([
    {
        "$match": { "year": 2020 }
    },
    {
        "$group": {
            "_id": "$carrier.name",
            "flight_count": { "$sum": 1 },
            "cancelled": { "$sum": { "$toInt": "$cancelled" } },
            "diverted": { "$sum": { "$toInt": "$diverted" } }
        }
    },
    {
        "$group": {
            "_id": null,
            "total_volume": { "$sum": "$flight_count" },
            "stats": { "$push": "$$ROOT" }
        }
    },
    {
        "$unwind": "$stats"
    },
    {
        "$addFields": {
            "stats.airline": "$stats._id",
            "stats.market_share_pct": {
                "$round": [
                    { "$multiply": [100, { "$divide": ["$stats.flight_count", "$total_volume"] }] }, 2
                ]
            },
            "stats.cancelled_pct": {
                "$round": [
                    { "$multiply": [100, { "$divide": ["$stats.cancelled", "$stats.flight_count"] }] }, 2
                ]
            },
            "stats.diverted_pct": {
                "$round": [
                    { "$multiply": [100, { "$divide": ["$stats.diverted", "$stats.flight_count"] }] }, 2
                ]
            }
        }
    },
    {
        "$sort": { "stats.flight_count": -1 }
    },
    {
        "$replaceRoot": { "newRoot": "$stats" }
    },
    {
        "$project": {
            "_id": 0,
            "airline": "$airline",
            "flight_count": "$flight_count",
            "cancelled": "$cancelled",
            "diverted": "$diverted",
            "market_share_pct": "$market_share_pct",
            "cancelled_pct": "$cancelled_pct",
            "diverted_pct": "$diverted_pct"
        }
    }
])
