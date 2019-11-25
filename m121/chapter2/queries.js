// $sort can take advantage of db indexes if used early in the pipeline, before $project or $unwind

const favorites = [
			"Sandra Bullock",
		  "Tom Hanks",
		  "Julia Roberts",
		  "Kevin Spacey",
		  "George Clooney"
		  ];


// find the 25th movie with most number of favorites in cast
db.movies.aggregate([
{
	$match: {
		cast: { 
			$in: favorites 
		},
		"tomatoes.viewer.rating": { $gte: 3 },
		"countries": "USA"
	}
},
{
	$addFields: {
		"num_faves": {
			$size: { $setIntersection: ["$cast", favorites]}
		}
	}
},
{
	$sort: { num_faves: -1, "tomatoes.viewer.rating": -1, title: -1}
},
{
	$project: { title: 1, num_faves: 1, tomatoScore: "$tomatoes.viewer.rating" }
},
{
	$skip: 24
},
{
	$limit: 1
}
]);

// average movies by imdb rating and find lowest
db.movies.aggregate([
{
	$match: {
		"languages": "English",
		"imdb.rating": { $gte: 1 },
		"imdb.votes": { $gte: 1 },
		"year": { $gte: 1990 }
	}
},
{
	$addFields: {
		"scaled_votes": {
	    $add: [
	      1,
	      {
	        $multiply: [
	          9,
	          {
	            $divide: [
	              { $subtract: ["$imdb.votes", 5] },
	              { $subtract: [1521105, 5] }
	            ]
	          }
	        ]
	      }
	    ]
	  },
	  "normalized_rating": {
	  	$avg: ["$scaled_votes", "$imdb.rating"]
	  }
	}
},
{
	$sort: { normalized_rating: 1 }
},
{
	$project: { title: 1, normalized_rating: 1, _id: 0 }
}
])