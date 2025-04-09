db.flight_data.createIndexes([
    { key: { "destination.airport_code": 1, "month": 1, "year": 1, "arr_delay": 1 } },
    { key: { "year": 1, "carrier.name": 1 } },
    { key: { "arr_delay": 1 } },
    { key: { "destination.state": 1, "year": 1 } },
    { key: { "fl_date": 1, "carrier.name": 1, "year": 1 } }
])
