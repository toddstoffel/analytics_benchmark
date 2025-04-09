db.flight_data.aggregate([
    {
      $group: {
        _id: { airline: "$carrier.name", year: "$year" },
        Total_Distance_Traveled: { $sum: "$distance" },
        Avg_Air_Time: { $avg: "$air_time" }
      }
    },
    {
      $project: {
        Airline: "$_id.airline",
        Year: "$_id.year",
        Total_Distance_Traveled: 1,
        Avg_Air_Time: { $round: ["$Avg_Air_Time", 2] },
        _id: 0
      }
    },
    {
      $replaceRoot: {
        newRoot: {
          Airline: "$Airline",
          Year: "$Year",
          Total_Distance_Traveled: "$Total_Distance_Traveled",
          Avg_Air_Time: "$Avg_Air_Time"
        }
      }
    },
    {
      $sort: {
        Year: 1,
        Total_Distance_Traveled: -1
      }
    }
  ]);
  