db.flight_data.aggregate([
  {
      "$match": {
          "destination.airport_code": { "$in": ["SFO", "OAK", "SJC"] },
          "arr_delay": { "$gt": 0 },
          "month": 11,
          "year": 2020
      }
  },
  {
      "$set": {
          "dest": { "$getField": { "field": "airport_code", "input": "$destination" } },
          "month_name": {
              "$dateToString": { 
                  "format": "%B", 
                  "date": { "$toDate": { "$concat": ["2020-", { "$toString": "$month" }, "-01"] } } 
              }
          }
      }
  },
  {
      "$group": {
          "_id": {
              "dest": "$dest",
              "month": "$month",
              "month_name": "$month_name",
              "day": "$day"
          },
          "avg_arr_delay": { "$avg": "$arr_delay" },
          "max_arr_delay": { "$max": "$arr_delay" }
      }
  },
  {
      "$project": {
          "_id": 0,
          "dest": "$_id.dest",
          "month": "$_id.month",
          "month_name": "$_id.month_name",
          "day": "$_id.day",
          "avg_arr_delay": { "$round": ["$avg_arr_delay", 2] },
          "max_arr_delay": "$max_arr_delay"
      }
  },
  { "$sort": { "dest": 1, "month": 1, "day": 1 } }
])
