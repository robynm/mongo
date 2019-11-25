// For all films that won at least 1 Oscar, calculate the standard deviation,
// highest, lowest, and average imdb.rating.
// Use the sample standard deviation expression.

db.movies.aggregate([
  {
    $match: {
      awards: { $regex: /Won \d+ Oscar(s)?/ }
    }
  },
  {
    $group: {
      _id: null,
      highest_rating: { $max: "$imdb.rating" },
      lowest_rating: { $min: "$imdb.rating" },
      avg_rating: { $avg: "$imdb.rating" },
      std_dev: { $stdDevSamp: "$imdb.rating" }
    }
  }
]);

// What is the name, number of movies, and average rating (truncated to one decimal)
// for the cast member that has been in the most number of movies with English as an available language?

db.movies.aggregate([
  {
    $match: { languages: "English" }
  },
  {
    $project: { _id: 0, cast: 1, "imdb.rating": 1 }
  },
  {
    $unwind: "$cast"
  },
  {
    $group: {
      _id: "$cast",
      numMovies: { $sum: 1 },
      rating: { $avg: "$imdb.rating" }
    }
  },
  {
    $sort: { numMovies: -1 }
  }
]);

// { "_id" : "John Wayne", "numFilms" : 107, "average" : 6.4 }

// Which alliance from air_alliances flies the most routes with either a Boeing 747 or an Airbus A380
// (abbreviated 747 and 380 in air_routes)?

db.air_routes.aggregate([
  {
    $match: { airplane: { $in: ["747", "380"] } }
  },
  {
    $lookup: {
      from: "air_alliances",
      localField: "airline.name",
      foreignField: "airlines",
      as: "alliance"
    }
  },
  {
    $unwind: "$alliance"
  },
  {
    $group: {
      _id: "$alliance.name",
      routes: { $sum: 1 }
    }
  },
  {
    $sort: { routes: -1 }
  }
]);

// which alliance has the most unique airlines operating between jfk and lhr
db.air_routes.aggregate([
  {
    $match: { $or: [{ src_airport: "JFK", dst_airport: "LHR" }, { src_airport: "LHR", dst_airport: "JFK" }] }
  },
  {
    $lookup: {
      from: "air_alliances",
      localField: "airline.name",
      foreignField: "airlines",
      as: "alliance"
    }
  },
  {
    $unwind: "$alliance"
  },
  {
    $group: {
      _id: "$alliance.name",
      jfkToLhr: { $addToSet: "$airline.name" }
    }
  },
  {
    $sort: { jfkToLhr: -1}
  },
  {
    $limit: 1
  }
])

db.air_routes.aggregate([
  {
    $match: {
      src_airport: { $in: ["LHR", "JFK"] },
      dst_airport: { $in: ["LHR", "JFK"] }
    }
  },
  {
    $lookup: {
      from: "air_alliances",
      foreignField: "airlines",
      localField: "airline.name",
      as: "alliance"
    }
  },
  {
    $match: { alliance: { $ne: [] } }
  },
  {
    $addFields: {
      alliance: { $arrayElemAt: ["$alliance.name", 0] }
    }
  },
  {
    $group: {
      _id: "$airline.id",
      alliance: { $first: "$alliance" }
    }
  },
  {
    $sortByCount: "$alliance"
  }
])

// Find the list of all possible distinct destinations, with at most one layover,
// departing from the base airports of airlines that make part of the "OneWorld" alliance.
// The airlines should be national carriers from Germany, Spain or Canada only.
// Include both the destination and which airline services that location.

db.air_alliances.aggregate([
  {
    $match: { name: "OneWorld" }
  },
  {
    $graphLookup: {
      startWith: "$airlines",
      from: "air_airlines",
      connectFromField: "name",
      connectToField: "name",
      as: "airlines",
      maxDepth: 0,
      restrictSearchWithMatch: {
        country: { $in: ["Germany", "Spain", "Canada"] }
      }
    }
  },
  {
    $graphLookup: {
      startWith: "$airlines.base",
      from: "air_routes",
      connectFromField: "dst_airport",
      connectToField: "src_airport",
      as: "connections",
      maxDepth: 1
    }
  },
  {
    $project: {
      validAirlines: "$airlines.name",
      "connections.dst_airport": 1,
      "connections.airline.name": 1
    }
  },
  { $unwind: "$connections" },
  {
    $project: {
      isValid: { $in: ["$connections.airline.name", "$validAirlines"] },
      "connections.dst_airport": 1
    }
  },
  { $match: { isValid: true } },
  {
    $group: {
      _id: "$connections.dst_airport"
    }
  }
]);
