
// mongo_queries_examples.js
// Usage (inside mongosh, after using the DB):
//   use minor_proj
//   load('mongo_queries_examples.js')

// 1) Count users, edges by type
print("Counts:");
printjson({
  users: db.users.countDocuments(),
  friend_edges: db.edges.countDocuments({type:'friend'}),
  follow_edges: db.edges.countDocuments({type:'follow'}),
  interactions: db.interactions.countDocuments()
});

// 2) Top 10 by followers
print("\nTop 10 by followers:");
db.edges.aggregate([
  { $match: { type: 'follow' } },
  { $group: { _id: '$dst', followers: { $sum: 1 } } },
  { $sort: { followers: -1 } },
  { $limit: 10 },
  { $lookup: { from: 'users', localField: '_id', foreignField: '_id', as: 'u' } },
  { $unwind: '$u' },
  { $project: { _id: 0, user: '$u.name', followers: 1 } }
]).forEach(doc => printjson(doc));

// 3) Top 10 by undirected friend degree
print("\nTop 10 by friend degree:");
db.edges.aggregate([
  { $match: { type: 'friend' } },
  { $project: { node: '$src' } },
  { $unionWith: { coll: 'edges', pipeline: [
      { $match: { type: 'friend' } },
      { $project: { node: '$dst' } }
  ]}},
  { $group: { _id: '$node', deg: { $sum: 1 } } },
  { $sort: { deg: -1 } },
  { $limit: 10 },
  { $lookup: { from: 'users', localField: '_id', foreignField: '_id', as: 'u' } },
  { $unwind: '$u' },
  { $project: { _id: 0, user: '$u.name', deg: 1 } }
]).forEach(doc => printjson(doc));

// 4) Followers/following for one user by _id
// Replace the id string below with any _id from db.users.findOne()._id.valueOf()
function followerSummary(userIdStr){
  const uid = ObjectId(userIdStr);
  const followers = db.edges.countDocuments({ type:'follow', dst: uid });
  const following = db.edges.countDocuments({ type:'follow', src: uid });
  const friends = db.edges.countDocuments({ type:'friend', $or: [{src:uid},{dst:uid}] });
  const u = db.users.findOne({_id: uid}, {projection:{name:1, location:1, primaryLang:1}});
  printjson({ user: u?.name, city: u?.location?.city, primaryLang: u?.primaryLang, followers, following, friends });
}

// 5) Mutual friends between two users
function mutualFriends(id1Str, id2Str){
  const id1 = ObjectId(id1Str);
  const id2 = ObjectId(id2Str);
  const friendsOf = (id) => {
    const res = [];
    db.edges.find({ type:'friend', $or: [{src:id},{dst:id}] }).forEach(e => {
      res.push(e.src.valueOf().equals(id) ? e.dst : e.src);
    });
    return res;
  };
  const f1 = friendsOf(id1).map(x => x.valueOf().toString());
  const f2 = new Set(friendsOf(id2).map(x => x.valueOf().toString()));
  const inter = f1.filter(x => f2.has(x)).map(s => ObjectId(s));
  const users = db.users.find({_id: { $in: inter }}, {projection:{name:1}}).toArray();
  print("Mutual friend count:", inter.length);
  users.forEach(u => print(u._id.valueOf(), "-", u.name));
}

// 6) Simple influence score (followers + weighted interactions pointing to user)
print("\nTop 10 by simple influence (followers + weighted interactions):");
db.users.aggregate([
  { $project: { _id: 1, name: 1 } },
  { $lookup: {
      from: 'edges',
      let: { uid: '$_id' },
      pipeline: [
        { $match: { $expr: { $and: [{$eq:['$type','follow']}, {$eq:['$dst','$$uid']}] } } },
        { $count: 'followers' }
      ],
      as: 'fstats'
  }},
  { $lookup: {
      from: 'interactions',
      let: { uid: '$_id' },
      pipeline: [
        { $match: { $expr: { $eq: ['$target','$$uid'] } } },
        { $group: { _id: null, score: { $sum: '$weight' } } }
      ],
      as: 'istats'
  }},
  { $addFields: {
      followers: { $ifNull: [ { $arrayElemAt: ['$fstats.followers', 0] }, 0 ] },
      interScore:{ $ifNull: [ { $arrayElemAt: ['$istats.score', 0] }, 0 ] }
  }},
  { $addFields: { influence: { $add: ['$followers', '$interScore'] } } },
  { $sort: { influence: -1 } },
  { $limit: 10 },
  { $project: { name: 1, followers:1, interScore:1, influence:1, _id:0 } }
]).forEach(doc => printjson(doc));

print("\nLoaded helper functions: followerSummary(userIdStr), mutualFriends(id1Str, id2Str)");
