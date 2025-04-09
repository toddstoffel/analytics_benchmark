db.flight_data.aggregate([
    {
      $match: { year: 2020 }
    },
    {
      $group: {
        _id: "$carrier.name",
        total_flights: { $sum: 1 },
        cancelled_count: {
          $sum: { $cond: [{ $gt: ["$cancelled", 0] }, 1, 0] }
        }
      }
    },
    {
      $set: {
        airline_name: "$_id",
        total_flights: "$total_flights",
        market_share_percentage: {
          $round: [
            {
              $multiply: [
                { $divide: ["$total_flights", db.flight_data.countDocuments({ year: 2020 })] },
                100
              ]
            },
            2
          ]
        },
        cancellation_percentage: {
          $round: [
            { $multiply: [{ $divide: ["$cancelled_count", "$total_flights"] }, 100] },
            2
          ]
        }
      }
    },
    {
      $project: {
        airline_name: 1,
        total_flights: 1,
        market_share_percentage: 1,
        cancellation_percentage: 1,
        _id: 0
      }
    },
    {
      $sort: { market_share_percentage: -1 }
    }
  ]);
  