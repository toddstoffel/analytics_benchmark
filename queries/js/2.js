db = db.getSiblingDB("bts");
db.getCollection("flight_data").aggregate(
    [
        {
            "$match" : {
                "year" : 2020.0
            }
        }, 
        {
            "$facet" : {
                "airline_delay" : [
                    {
                        "$match" : {
                            "carrier_delay" : {
                                "$gt" : 0.0
                            }
                        }
                    },
                    {
                        "$group" : {
                            "_id" : {
                                "airline" : "$carrier.name",
                                "year" : "$year"
                            },
                            "delay" : {
                                "$sum" : 1.0
                            }
                        }
                    },
                    {
                        "$addFields" : {
                            "delay_type" : "Airline Delay"
                        }
                    }
                ],
                "late_aircraft_delay" : [
                    {
                        "$match" : {
                            "late_aircraft_delay" : {
                                "$gt" : 0.0
                            }
                        }
                    },
                    {
                        "$group" : {
                            "_id" : {
                                "airline" : "$carrier.name",
                                "year" : "$year"
                            },
                            "delay" : {
                                "$sum" : 1.0
                            }
                        }
                    },
                    {
                        "$addFields" : {
                            "delay_type" : "Late Aircraft Delay"
                        }
                    }
                ],
                "air_system_delay" : [
                    {
                        "$match" : {
                            "nas_delay" : {
                                "$gt" : 0.0
                            }
                        }
                    },
                    {
                        "$group" : {
                            "_id" : {
                                "airline" : "$carrier.name",
                                "year" : "$year"
                            },
                            "delay" : {
                                "$sum" : 1.0
                            }
                        }
                    },
                    {
                        "$addFields" : {
                            "delay_type" : "Air System Delay"
                        }
                    }
                ],
                "weather_delay" : [
                    {
                        "$match" : {
                            "weather_delay" : {
                                "$gt" : 0.0
                            }
                        }
                    },
                    {
                        "$group" : {
                            "_id" : {
                                "airline" : "$carrier.name",
                                "year" : "$year"
                            },
                            "delay" : {
                                "$sum" : 1.0
                            }
                        }
                    },
                    {
                        "$addFields" : {
                            "delay_type" : "Weather Delay"
                        }
                    }
                ]
            }
        }, 
        {
            "$project" : {
                "data" : {
                    "$concatArrays" : [
                        "$airline_delay",
                        "$late_aircraft_delay",
                        "$air_system_delay",
                        "$weather_delay"
                    ]
                }
            }
        }, 
        {
            "$unwind" : "$data"
        }, 
        {
            "$replaceRoot" : {
                "newRoot" : "$data"
            }
        }, 
        {
            "$sort" : {
                "_id.airline" : 1.0,
                "_id.year" : 1.0,
                "delay_type" : 1.0
            }
        }, 
        {
            "$project" : {
                "_id" : 0.0,
                "airline" : "$_id.airline",
                "year" : "$_id.year",
                "delay_type" : "$delay_type",
                "delay" : "$delay"
            }
        }
    ]
);
