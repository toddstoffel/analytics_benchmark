db.flight_data.aggregate([
    {
        "$group": {
            "_id": { 
                "airline": "$carrier.name", 
                "year": "$year" 
            },
            "Total_Flights": { "$sum": 1 },
            "Avg_Departure_Delay": { "$avg": "$dep_delay" },
            "Avg_Arrival_Delay": { "$avg": "$arr_delay" }
        }
    },
    {
        "$addFields": {
            "Airline": "$_id.airline",
            "Year": "$_id.year"
        }
    },
    {
        "$project": {
            "_id": 0,
            "Airline": 1,
            "Year": 1,
            "Total_Flights": 1,
            "Avg_Departure_Delay": 1,
            "Avg_Arrival_Delay": 1
        }
    },
    {
        "$replaceRoot": {
            "newRoot": {
                "Airline": "$Airline",
                "Year": "$Year",
                "Total_Flights": "$Total_Flights",
                "Avg_Departure_Delay": "$Avg_Departure_Delay",
                "Avg_Arrival_Delay": "$Avg_Arrival_Delay"
            }
        }
    },
    {
        "$sort": {
            "Year": 1, 
            "Total_Flights": 1
        }
    }
])
