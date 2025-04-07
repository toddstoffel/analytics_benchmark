db.flight_data.aggregate([
    {
        "$match": {
            "destination.airport_code": { "$in": ["SFO", "OAK", "SJC"] },
            "arr_delay": { "$gt": 0 },
            "year": 2020
        }
    },
    {
        "$project": {
            "_id": 0,
            "dest": "$destination.airport_code",
            "month_name": {
                "$dateToString": { "format": "%B", "date": { "$toDate": { "$concat": ["2020-", { "$toString": "$month" }, "-01"] } } }
            },
            "scheduled_arrival_hr": { "$toInt": { "$substr": ["$crs_arr_time", 0, 2] } },
            "arr_delay": 1
        }
    },
    {
        "$group": {
            "_id": {
                "dest": "$dest",
                "month_name": "$month_name",
                "scheduled_arrival_hr": "$scheduled_arrival_hr"
            },
            "avg_arr_delay": { "$avg": "$arr_delay" },
            "max_arr_delay": { "$max": "$arr_delay" }
        }
    },
    {
        "$addFields": {
            "avg_arr_delay": { "$round": ["$avg_arr_delay", 2] }
        }
    },
    {
        "$sort": {
            "_id.dest": 1,
            "_id.month_name": 1,
            "_id.scheduled_arrival_hr": 1
        }
    },
    {
        "$project": {
            "_id": 0,
            "dest": "$_id.dest",
            "month_name": "$_id.month_name",
            "scheduled_arrival_hr": "$_id.scheduled_arrival_hr",
            "avg_arr_delay": "$avg_arr_delay",
            "max_arr_delay": "$max_arr_delay"
        }
    }
])
