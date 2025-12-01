// mongo_social_setup.js
// Generate ~15000 realistic users + attributes and a friend/follow graph ~112k-187k edges.
// Usage: mongosh --file mongo_social_setup.js
// This version streams user inserts in batches to reduce memory footprint.

const DB_NAME   = "minor_proj";
const N_USERS   = 15000;                // number of users
const TARGET_EDGE_MIN = 112500;         // scaled target friend edges (min)
const TARGET_EDGE_MAX = 187500;         // scaled target friend edges (max)
const AVG_DEGREE = 10;                  // nominal average degree (used to derive degree distribution)
const FRIEND_DEGREE_VARIANCE = 6;
const FOLLOW_MIN = 2;
const FOLLOW_MAX = 6;
const N_INTERACTIONS = 60000;           // total interaction events
const BATCH_SIZE = 5000;                // batch size for insertMany
let GLOBAL_USER_ID = 1; // Global / unique user id generator

// ---------------- RNG (deterministic) ----------------
let _seed = 1234567;
function rand(){ _seed = ((_seed*1664525) + 1013904223) % 4294967296; return _seed/4294967296; }
function randint(a,b){ return a + Math.floor(rand()*(b-a+1)); }
function choice(arr){ return arr[Math.floor(rand()*arr.length)]; }
function sample(arr,k){
  const res=[]; const s=new Set();
  if(k>=arr.length) return arr.slice();
  while(res.length<k){
    const i = Math.floor(rand()*arr.length);
    if(!s.has(i)){ s.add(i); res.push(arr[i]); }
  }
  return res;
}
function randBool(p=0.5){ return rand() < p; }

// ---------------- reference lists ----------------
const languages = ['hi','bn','ta','te','mr','gu','kn','ml','pa','or','as','en'];
const cities = [
  {city:'Delhi',lang:'hi'},{city:'Mumbai',lang:'mr'},{city:'Pune',lang:'mr'},{city:'Kolkata',lang:'bn'},
  {city:'Chennai',lang:'ta'},{city:'Hyderabad',lang:'te'},{city:'Bengaluru',lang:'kn'},{city:'Ahmedabad',lang:'gu'},
  {city:'Jaipur',lang:'hi'},{city:'Lucknow',lang:'hi'},{city:'Bhopal',lang:'hi'},{city:'Patna',lang:'hi'},
  {city:'Bhubaneswar',lang:'or'},{city:'Guwahati',lang:'as'},{city:'Kochi',lang:'ml'},{city:'Thiruvananthapuram',lang:'ml'}
];

const firstNamesF = ['Ananya','Ishita','Meera','Zoya','Priya','Aisha','Diya','Sanya','Kavya','Neha','Riya','Sneha','Pooja','Nisha','Tanvi','Aarohi','Ira','Jahnvi','Mira','Aditi'];
const firstNamesM = ['Rohan','Aarav','Kabir','Dev','Nikhil','Arjun','Rahul','Siddharth','Rajat','Yash','Vikram','Rohit','Aman','Ankit','Kunal','Varun','Aakash','Naveen','Sagar','Harsh'];
const lastNames   = ['Sharma','Gupta','Iyer','Nair','Khan','Patel','Banerjee','Deshmukh','Verma','Singh','Chatterjee','Mukherjee','Agarwal','Bose','Kapoor','Joshi','Mehta','Bhat','Gowda','Reddy'];

const educations = ['highschool','undergrad','postgrad','phd','other'];
const professions = ['student','engineer','doctor','teacher','researcher','artist','lawyer','manager','sales','developer','data_scientist','other'];
const interestsPool = ['music','travel','sports','reading','movies','gaming','cooking','photography','fitness','tech','art','politics','fashion','science','nature'];
const purposes = ['socializing','networking','gaming','news','learning','business'];

// ---------------- helper functions ----------------
function randomName(g){
  const first = g==='female' ? choice(firstNamesF) : choice(firstNamesM);
  const last  = choice(lastNames);
  return first + " " + last;
}
function pickPrimaryLang(cityLang){
  if(rand() < 0.7) return cityLang;
  return choice(languages.filter(x => x !== 'en'));
}
function buildLanguages(primary){
  const arr = [{code:primary,is_primary:true}];
  if(rand() < 0.8) arr.push({code:'en',is_primary:false});
  if(rand() < 0.3){
    const extra = choice(languages.filter(l => l !== primary && l !== 'en'));
    arr.push({code:extra,is_primary:false});
  }
  return arr;
}
function dateRandom(startYear=2015){
  const year = randint(startYear, 2025);
  const month = String(randint(1,12)).padStart(2,'0');
  const day = String(randint(1,28)).padStart(2,'0');
  return year + "-" + month + "-" + day;
}

// ---------------- connect ----------------
const conn = connect("127.0.0.1:27017/" + DB_NAME);
const db = conn.getSiblingDB(DB_NAME);

print("Dropping existing collections (if any)...");
try { db.users.drop(); } catch(e){ print("users drop error:", e); }
try { db.edges.drop(); } catch(e){ print("edges drop error:", e); }
try { db.interactions.drop(); } catch(e){ print("interactions drop error:", e); }

// ---------------- generate users (streamed) ----------------
print("Generating users and attributes (streamed in batches)...");
const nCommunities = randint(12,22);

// Determine community sizes first (so we know community distribution)
const communities = [];
let rem = N_USERS;
for(let i=0;i<nCommunities;i++){
  const min = Math.floor(N_USERS / (nCommunities*2));
  if(i === nCommunities-1){ communities.push(rem); break; }
  const take = randint(min, Math.max(min, Math.floor(rem/(nCommunities - i))));
  communities.push(take);
  rem -= take;
}

// We'll keep only small user-metadata structures in memory:
// - allUserIds: array of all user ids
// - idsByCommunity: map community -> [ids]
// - idsByCity: map city -> [ids]
// - degree: map id -> 0 (updated as edges are added)
const allUserIds = [];
const idsByCommunity = {};
const idsByCity = {};
const degree = {}; // initialize later while we add users

// Insert users in batches to reduce memory usage:
let userBatch = [];
let currentCommunity = 0;
let assignedToCurrent = 0;
let communityIndex = 0;
for(let created=0; created<N_USERS; ){
  // Determine community for this user using the communities array
  if(assignedToCurrent >= communities[communityIndex]){ communityIndex++; assignedToCurrent = 0; }
  const community = communityIndex;
  assignedToCurrent++;

  const _id = GLOBAL_USER_ID++;
  const gender = rand() < 0.5 ? 'female' : 'male';
  const place = choice(cities);
  const primary = pickPrimaryLang(place.lang);
  const languagesList = buildLanguages(primary);
  const education = choice(educations);
  const profession = choice(professions);
  const nInterests = randint(1,3);
  const interests = sample(interestsPool, nInterests);
  const dateJoined = dateRandom(2016);
  const purpose = choice(purposes);
  const thirdParty = randBool(0.25);
  const name = randomName(gender);

  const userDoc = {
    _id, name, age: randint(16, 60), gender,
    location: { city: place.city, state: (place.state || ""), country: 'India' },
    languages: languagesList, primaryLang: primary, joinedAt: dateJoined,
    education, profession, interests, purpose, thirdParty, community
  };

  userBatch.push(userDoc);

  // Maintain minimal metadata
  allUserIds.push(_id);
  if(!idsByCommunity[community]) idsByCommunity[community] = [];
  idsByCommunity[community].push(_id);
  if(!idsByCity[place.city]) idsByCity[place.city] = [];
  idsByCity[place.city].push(_id);
  degree[_id] = 0;

  created++;

  if(userBatch.length >= BATCH_SIZE || created === N_USERS){
    // insert batch
    try {
      db.users.insertMany(userBatch, { ordered: false });
    } catch(e){
      print("Warning: insertMany users batch error (continuing).", e);
    }
    print("Inserted users so far: " + db.users.countDocuments());
    userBatch = [];
  }
}

print("Finished creating users. Total users inserted (count): " + db.users.countDocuments());

// ---------------- helper for pair key ----------------
function pairKey(a,b){
  const s = a.toString(), t = b.toString();
  return (s < t) ? s+"|"+t : t+"|"+s;
}

// ---------------- generate biased friend edges (batched) ----------------
print("Preparing to generate biased friend edges (batched).");

const friendSet = {}; // keep track of undirected pairs inserted
let edgeBatch = [];
function flushEdges(){
  if(edgeBatch.length){
    try {
      db.edges.insertMany(edgeBatch, { ordered: false });
    } catch(e){
      // duplicates or other errors shouldn't stop the run; print and continue
      print("Edge batch insert warning:", e);
    }
    edgeBatch = [];
  }
}

// Choose random target edges in specified range
let targetEdges = randint(TARGET_EDGE_MIN, TARGET_EDGE_MAX);
const totalStubs = targetEdges * 2;

// 1) generate a base degree for each node (random around AVG_DEGREE)
// 2) scale base degrees so sum(baseDegrees) ≈ totalStubs
const baseDegrees = {};
let sumBase = 0;
for(const id of allUserIds){
  const d = Math.max(1, Math.floor(randint(AVG_DEGREE - FRIEND_DEGREE_VARIANCE, AVG_DEGREE + FRIEND_DEGREE_VARIANCE)));
  baseDegrees[id] = d;
  sumBase += d;
}

// avoid divide by zero
const scaleFactor = sumBase > 0 ? (totalStubs / sumBase) : 1.0;
const targetDegrees = {};
let sumAssigned = 0;
for(const id of allUserIds){
  let assigned = Math.max(1, Math.floor(baseDegrees[id] * scaleFactor));
  targetDegrees[id] = assigned;
  sumAssigned += assigned;
}

// fix rounding differences by distributing any remaining stubs
let remaining = totalStubs - sumAssigned;
let idx = 0;
const idsLen = allUserIds.length;
while(remaining > 0 && idsLen>0){
  const id = allUserIds[idx % idsLen];
  targetDegrees[id] += 1;
  remaining--;
  idx++;
}
if(remaining < 0){
  // if we overshot (shouldn't normally happen), trim some
  idx = 0;
  while(remaining < 0 && idsLen>0){
    const id = allUserIds[idx % idsLen];
    if(targetDegrees[id] > 1){ targetDegrees[id] -= 1; remaining++; }
    idx++;
  }
}

let expectedEdges = Math.floor(Object.values(targetDegrees).reduce((a,b)=>a+b,0)/2);
print("Target friend edges:" + targetEdges + "  expected ~" + expectedEdges);

// --- create fast lookup maps: user id -> community index, user id -> city name
const userCommunityMap = {};
const userCityMap = {};
for (const c in idsByCommunity) {
  for (const id of idsByCommunity[c]) userCommunityMap[id] = parseInt(c);
}
for (const city in idsByCity) {
  for (const id of idsByCity[city]) userCityMap[id] = city;
}

// neighbor selection function (biased to community, then city, else global)
function pickNeighbor(uId){
  const cidx = userCommunityMap[uId];
  const city = userCityMap[uId];
  const r = rand();
  let pool = [];
  if(r < 0.75 && idsByCommunity[cidx] && idsByCommunity[cidx].length > 1){
    pool = idsByCommunity[cidx];
  } else if(r < 0.9 && idsByCity[city] && idsByCity[city].length > 1){
    pool = idsByCity[city];
  } else {
    pool = allUserIds;
  }
  // sample a few and pick the one with highest degree (prefer higher-degree)
  const candidates = sample(pool.filter(id => id !== uId), Math.min(6, Math.max(0, pool.length-1)));
  let best = null;
  let bestDeg = -1;
  for(const cand of candidates){
    const dv = degree[cand] || 0;
    if(dv > bestDeg && cand !== uId) { best = cand; bestDeg = dv; }
  }
  if(!best && candidates.length) best = candidates[0];
  return best;
}

// Now create friend edges trying to respect targetDegrees
let totalEdges = 0;
let progressCounter = 0;
for(const uId of allUserIds){
  const desired = targetDegrees[uId] || 1;
  let tries = 0;
  while (degree[uId] < desired && totalEdges < targetEdges) {
    if (tries++ > desired * 15) break; // avoid infinite loops for hard-to-find neighbors
    const vId = pickNeighbor(uId);
    if (!vId) continue;
    if (uId === vId) continue;
    const k = pairKey(uId, vId);
    if (friendSet[k]) continue;
    friendSet[k] = true;
    // push undirected friend edge once (src/dst arbitrary)
    edgeBatch.push({ type: 'friend', src: uId, dst: vId, pair: k, weight: 1.0 });
    degree[uId]++; degree[vId] = (degree[vId] || 0) + 1;
    totalEdges++;
    if(edgeBatch.length >= BATCH_SIZE) flushEdges();
    // occasionally print progress
    if(++progressCounter % 50000 === 0) print("Friend edges produced so far: " + totalEdges);
  }
  if (totalEdges >= targetEdges) break;
}
flushEdges();
print("Friend edges inserted (capped): " + db.edges.countDocuments({type:'friend'}));

// ---------------- generate follow edges ----------------
print("Generating follow (directed) edges...");
let followBatch = [];
function flushFollow(){
  if(followBatch.length){
    try {
      db.edges.insertMany(followBatch, { ordered: false });
    } catch(e){
      print("Follow batch insert warning:", e);
    }
    followBatch = [];
  }
}
const followSet = {};
for(const uId of allUserIds){
  const nF = randint(FOLLOW_MIN, FOLLOW_MAX);
  let made = 0;
  let tries = 0;
  while(made < nF && tries < nF*8){
    tries++;
    let candidate;
    const r = rand();
    if(r < 0.4){
      candidate = choice(allUserIds);
    } else if(r < 0.8){
      const commList = idsByCommunity[userCommunityMap[uId]];
      candidate = choice(commList);
    } else {
      candidate = choice(allUserIds);
    }
    if(candidate === uId) continue;
    const key = uId + "|" + candidate;
    if(followSet[key]) continue;
    followSet[key] = true;
    followBatch.push({ type: 'follow', src: uId, dst: candidate, weight: 1.0 });
    made++;
    if(followBatch.length >= BATCH_SIZE) flushFollow();
  }
}
flushFollow();
print("Total edges after follows: " + db.edges.countDocuments());

// ---------------- interactions (batched) ----------------
print("Generating interactions (batched)...");
let interactionsBatch = [];
function flushInteractions(){
  if(interactionsBatch.length){
    try {
      db.interactions.insertMany(interactionsBatch, { ordered: false });
    } catch(e){
      print("Interactions batch insert warning:", e);
    }
    interactionsBatch = [];
  }
}
const interactionTypes = ['message','like','comment','view'];
for(let i=0;i<N_INTERACTIONS;i++){
  const actor = choice(allUserIds);
  const target = choice(allUserIds);
  if(actor === target) continue;
  const t = choice(interactionTypes);
  const w = t === 'message' ? 2.0 : t === 'comment' ? 1.5 : t === 'like' ? 1.0 : 0.5;
  interactionsBatch.push({ actor, target, type: t, weight: w, createdAt: new Date() });
  if(interactionsBatch.length >= BATCH_SIZE) flushInteractions();
}
flushInteractions();
print("Interactions inserted: " + db.interactions.countDocuments());

// ---------------- indexes ----------------
print("Creating indexes...");
try { db.users.createIndex({"location.city":1}); } catch(e){ print("idx users.city:", e); }
try { db.users.createIndex({"primaryLang":1}); } catch(e){ print("idx users.primaryLang:", e); }
try { db.users.createIndex({"community":1}); } catch(e){ print("idx users.community:", e); }
try { db.edges.createIndex({type:1, src:1}); } catch(e){ print("idx edges.src:", e); }
try { db.edges.createIndex({type:1, dst:1}); } catch(e){ print("idx edges.dst:", e); }
try { db.edges.createIndex({pair:1},{unique:true,partialFilterExpression:{type:'friend'}}); } catch(e){ print("idx edges.pair:", e); }
try { db.edges.createIndex({type:1,src:1,dst:1},{unique:true,partialFilterExpression:{type:'follow'}}); } catch(e){ print("idx edges.srcdst:", e); }
try { db.interactions.createIndex({actor:1}); } catch(e){ print("idx interactions.actor:", e); }
try { db.interactions.createIndex({target:1}); } catch(e){ print("idx interactions.target:", e); }

print("Summary:");
printjson({
  users: db.users.countDocuments(),
  friend_edges: db.edges.countDocuments({type:'friend'}),
  follow_edges: db.edges.countDocuments({type:'follow'}),
  interactions: db.interactions.countDocuments()
});
print("Done. Next: load('mongo_queries_examples.js') to explore.");

// ---------- end ----------
