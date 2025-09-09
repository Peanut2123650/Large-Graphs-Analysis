// mongo_social_setup.js
// Generate ~4000 realistic users + attributes and a friend/follow graph ~30k-50k edges.
// Usage: mongosh < mongo_social_setup.js
// IMPORTANT: this script uses batch inserts to avoid memory issues.

const DB_NAME   = "minor_proj";
const N_USERS   = 4000;
const TARGET_EDGE_MIN = 30000; // desired min friend edges
const TARGET_EDGE_MAX = 50000; // desired max friend edges
const AVG_DEGREE = 20;         // target average friendship degree (~15-25 is reasonable)
const FRIEND_DEGREE_VARIANCE = 6; // how much each node's degree can vary
const FOLLOW_MIN = 2;
const FOLLOW_MAX = 6;
const N_INTERACTIONS = 15000;
const BATCH_SIZE = 1000;
let GLOBAL_USER_ID = 1; // Global variable for user ID

// ---------------- RNG ----------------
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
  return `${first} ${last}`;
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
  return `${year}-${month}-${day}`;
}
function randBool(p=0.5){ return rand() < p; }

// ---------------- connect ----------------
const conn = connect("127.0.0.1:27017/" + DB_NAME);
const db = conn.getSiblingDB(DB_NAME);

print("Dropping existing collections...");
db.users.drop(); db.edges.drop(); db.interactions.drop();

// ---------------- generate users ----------------
print("Generating users and attributes...");
const users = [];
const communities = [];
const nCommunities = randint(12,22);
let rem = N_USERS;
for(let i=0;i<nCommunities;i++){
  const min = Math.floor(N_USERS / (nCommunities*2));
  if(i === nCommunities-1){ communities.push(rem); break; }
  const take = randint(min, Math.max(min, Math.floor(rem/(nCommunities - i))));
  communities.push(take);
  rem -= take;
}

const communityOf = {};
for(let c=0;c<nCommunities;c++){
  const size = communities[c];
  for(let j=0;j<size;j++){
    const _id = GLOBAL_USER_ID++; // Assign the global ID and increment it
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
    users.push({
      _id, name, age: randint(16, 60), gender,
      location: { city: place.city, state: place.state, country: 'India' },
      languages: languagesList, primaryLang: primary, joinedAt: dateJoined,
      education, profession, interests, purpose, thirdParty, community: c
    });
    communityOf[_id] = c;
  }
}
db.users.insertMany(users);
print(`Inserted users: ${db.users.countDocuments()}`);

// ---------------- prepare helper indexes ----------------
const idsByCommunity = {};
for(const u of users){
  const c = u.community;
  if(!idsByCommunity[c]) idsByCommunity[c] = [];
  idsByCommunity[c].push(u._id);
}
const idsByCity = {};
for(const u of users){
  const c = u.location.city;
  if(!idsByCity[c]) idsByCity[c] = [];
  idsByCity[c].push(u._id);
}
const degree = {};
for(const u of users) degree[u._id] = 0;

function pairKey(a,b){
  const s = a.toString(), t = b.toString(); // Use .toString() on numbers
  return (s < t) ? `${s}|${t}` : `${t}|${s}`;
}

// ---------------- generate biased friend edges (target total) ----------------
print("Generating biased friend edges (batched).");
const friendSet = {};
let edgeBatch = [];
function flushEdges(){ if(edgeBatch.length){ db.edges.insertMany(edgeBatch); edgeBatch = []; } }

let targetEdges = randint(TARGET_EDGE_MIN, TARGET_EDGE_MAX);
const targetDegrees = {};
let remainingDegree = targetEdges * 2;

// assign degrees
for (const u of users) {
  let d = Math.max(1, Math.floor(randint(AVG_DEGREE - FRIEND_DEGREE_VARIANCE, AVG_DEGREE + FRIEND_DEGREE_VARIANCE)));
  if (remainingDegree - d < 0) d = remainingDegree;
  targetDegrees[u._id] = d;
  remainingDegree -= d;
  if (remainingDegree <= 0) break;
}
if (remainingDegree > 0) {
  for (const id in targetDegrees) {
    targetDegrees[id]++;
    remainingDegree--;
    if (remainingDegree <= 0) break;
  }
}

let expectedEdges = Math.floor(Object.values(targetDegrees).reduce((a,b)=>a+b,0)/2);
print(`Target friend edges: ${targetEdges}, expected ~${expectedEdges}`);

function pickNeighbor(uObj){
  const uId = uObj._id;
  const community = uObj.community;
  const r = rand();
  let pool = [];
  if(r < 0.75 && idsByCommunity[community].length > 1){
    pool = idsByCommunity[community];
  } else if(r < 0.9 && idsByCity[uObj.location.city] && idsByCity[uObj.location.city].length > 1){
    pool = idsByCity[uObj.location.city];
  } else {
    pool = users.map(x => x._id);
  }
  // Use !== for number comparison instead of .equals()
  const candidates = sample(pool.filter(id => id !== uId), Math.min(6, pool.length-1));
  let best = null;
  let bestDeg = -1;
  for(const cand of candidates){
    const dv = degree[cand] || 0; // Access degree directly with number
    if(dv > bestDeg && cand !== uId) { best = cand; bestDeg = dv; }
  }
  if(!best && candidates.length) best = candidates[0];
  return best;
}

let totalEdges = 0;
for (const u of users) {
  const uId = u._id;
  const desired = targetDegrees[uId];
  let tries = 0;
  while (degree[uId] < desired && totalEdges < targetEdges) {
    if (tries++ > desired * 10) break;
    const vId = pickNeighbor(u);
    if (!vId) continue;
    if (uId === vId) continue; // Use === for number comparison
    const k = pairKey(uId, vId);
    if (friendSet[k]) continue;
    friendSet[k] = true;
    edgeBatch.push({ type: 'friend', src: uId, dst: vId, pair: k, weight: 1.0 });
    degree[uId]++;
    degree[vId]++;
    totalEdges++;
    if (edgeBatch.length >= BATCH_SIZE) flushEdges();
  }
  if (totalEdges >= targetEdges) break;
}
flushEdges();
print(`Friend edges inserted (capped): ${db.edges.countDocuments({type:'friend'})}`);

// ---------------- generate follow edges ----------------
print("Generating follow (directed) edges...");
let followBatch = [];
function flushFollow(){ if(followBatch.length){ db.edges.insertMany(followBatch); followBatch = []; } }
const followSet = {};
for(const u of users){
  const nF = randint(FOLLOW_MIN, FOLLOW_MAX);
  let made = 0;
  let tries = 0;
  while(made < nF && tries < nF*8){
    tries++;
    let candidate;
    if(rand() < 0.4){
      candidate = choice(users)._id;
    } else if(rand() < 0.8){
      candidate = choice(idsByCommunity[u.community]);
    } else {
      candidate = choice(users)._id;
    }
    if(candidate === u._id) continue; // Use === for number comparison
    const key = `${u._id}|${candidate}`;
    if(followSet[key]) continue;
    followSet[key] = true;
    followBatch.push({ type: 'follow', src: u._id, dst: candidate, weight: 1.0 });
    made++;
    if(followBatch.length >= BATCH_SIZE) flushFollow();
  }
}
flushFollow();
print(`Total edges after follows: ${db.edges.countDocuments()}`);

// ---------------- interactions ----------------
print("Generating interactions...");
let interactionsBatch = [];
function flushInteractions(){ if(interactionsBatch.length){ db.interactions.insertMany(interactionsBatch); interactionsBatch = []; } }
const interactionTypes = ['message','like','comment','view'];
for(let i=0;i<N_INTERACTIONS;i++){
  const actor = choice(users)._id;
  const target = choice(users)._id;
  if(actor === target) continue; // Use === for number comparison
  const t = choice(interactionTypes);
  const w = t === 'message' ? 2.0 : t === 'comment' ? 1.5 : t === 'like' ? 1.0 : 0.5;
  interactionsBatch.push({ actor, target, type: t, weight: w, createdAt: new Date() });
  if(interactionsBatch.length >= BATCH_SIZE) flushInteractions();
}
flushInteractions();
print(`Interactions: ${db.interactions.countDocuments()}`);

// ---------------- indexes ----------------
print("Creating indexes...");
db.users.createIndex({"location.city":1});
db.users.createIndex({"primaryLang":1});
db.users.createIndex({"community":1});
db.edges.createIndex({type:1, src:1});
db.edges.createIndex({type:1, dst:1});
db.edges.createIndex({pair:1},{unique:true,partialFilterExpression:{type:'friend'}});
db.edges.createIndex({type:1,src:1,dst:1},{unique:true,partialFilterExpression:{type:'follow'}});
db.interactions.createIndex({actor:1});
db.interactions.createIndex({target:1});

print("Summary:");
printjson({
  users: db.users.countDocuments(),
  friend_edges: db.edges.countDocuments({type:'friend'}),
  follow_edges: db.edges.countDocuments({type:'follow'}),
  interactions: db.interactions.countDocuments()
});
print("Done. Next: load('mongo_queries_examples.js') to explore.");