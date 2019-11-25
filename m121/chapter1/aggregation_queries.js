// filter movies by rating, genre, languages
var pipeline = [
	{ 
		$match: { 
			"imdb.rating": { $gte: 7 }, 
			"genres": { $nin: ["Crime", "Horror"]}, 
			"rated": {$in: ["PG", "G"]}, 
			"languages": { $all: ["English", "Japanese"]} 
		}
	},
	{

		$project: {
			"_id": 0,
			"title": 1,
			"rated": 1
		}
	}
];


// all movies with a single word title
db.movies.aggregate([
	{
		$match: {
			"title": { $type: "string" }
		}
	},
	{ 
		$project: { 
			"title": { $split: ["$title", " "] },
			"_id": 0
		}
	},
	{
		$match: {
			"title": { $size: 1 }
		}
	}
]).itcount();


// find all the movies with the same person in writers, directors, and cast
// filtering writers for any () characters
db.movies.aggregate([
	{ 
		$match: { 
			writers: { 
				$elemMatch: { $exists: true } 
			},
			cast: { 
				$elemMatch: { $exists: true } 
			},
			directors: { 
				$elemMatch: { $exists: true } 
			}
		}
	},
	{ 
		$project: {
			_id: 0,
			cast: 1,
			directors: 1,
			writers: {
			  $map: {
			    input: "$writers",
			    as: "writer",
			    in: {
			      $arrayElemAt: [
			        {
			          $split: [ "$$writer", " (" ]
			        },
			        0
			      ]
			    }
			  }
			}
		}
	},
	{
		$project: {
			common: {
				$gt: [
					{ $size: { $setIntersection: ["$writers", "$cast", "$directors"]} },
					0
				]
			}
		}
	},
	{
		$match: {
			common: true
		}
	},
	{
		$count: "labors of love"
	}
])