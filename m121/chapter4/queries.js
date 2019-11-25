// How many movies are in both the top ten highest rated movies according to the imdb.rating and the metacritic fields?
db.movies.aggregate([
  {
    $match: { "imdb.rating": { $gte: 1 }, metacritic: { $gte: 1 } }
  },
  {
    $project: { _id: 0, title: 1, "imdb.rating": 1, metacritic: 1 }
  },
  {
    $facet: {
      imdbRating: [{ $sort: { "imdb.rating": -1 } }, { $limit: 10 }],
      metacriticRating: [{ $sort: { metacritic: -1 } }, { $limit: 10 }]
    }
  },
  {
    $project: {
      common: {
        $setIntersection: ["$imdbRating", "$metacriticRating"]
      }
    }
  }
]);
