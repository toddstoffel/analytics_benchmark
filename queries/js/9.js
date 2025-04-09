db.flight_data.aggregate([
    {
      $group: {
        _id: {
          year: "$year",
          airline_name: "$carrier.name"
        },
        avg_arrival_delay: { $avg: "$arr_delay" },
        avg_departure_delay: { $avg: "$dep_delay" }
      }
    },
    {
      $project: {
        year: "$_id.year",
        airline_name: "$_id.airline_name",
        avg_arrival_delay: { $round: ["$avg_arrival_delay", 2] },
        avg_departure_delay: { $round: ["$avg_departure_delay", 2] },
        _id: 0
      }
    },
    {
      $sort: {
        year: 1,
        avg_arrival_delay: -1
      }
    }
  ]);
  