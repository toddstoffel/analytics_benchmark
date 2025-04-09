db.flight_data.aggregate([
    {
      $match: {
        year: 2020
      }
    },
    {
      $group: {
        _id: "$destination.airport_code",
        total_flights: { $sum: 1 },
        delayed_flights: {
          $sum: { $cond: [{ $gt: ["$arr_delay", 0] }, 1, 0] }
        }
      }
    },
    {
      $set: {
        delay_percentage: {
          $round: [
            { $multiply: [{ $divide: ["$delayed_flights", "$total_flights"] }, 100] },
            2
          ]
        }
      }
    },
    {
      $replaceRoot: {
        newRoot: {
          airport_name: "$_id",
          total_flights: "$total_flights",
          delayed_flights: "$delayed_flights",
          delay_percentage: "$delay_percentage"
        }
      }
    },
    {
      $sort: {
        delayed_flights: -1
      }
    },
    {
      $limit: 5
    }
  ]);
  