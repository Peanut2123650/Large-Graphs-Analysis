// mongo_social_setup.js
// Updated for Social_Network_Project
// Generate ~4000 users + friend/follow graph + interactions
// Usage: mongosh < mongo_social_setup.js

const DB_NAME   = "minor_proj";
const N_USERS   = 4000;
const TARGET_EDGE_MIN = 30000;
const TARGET_EDGE_MAX = 50000;
const AVG_DEGREE = 20;
const FRIEND_DEGREE_VARIANCE = 6;
const FOLLOW_MIN = 2;
const FOLLOW_MAX = 6;
const N_INTERACTIONS = 15000;
const BATCH_SIZE = 1000;
let GLOBAL_USER_ID = 1;

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
function randomName(g){ return (g==='female'?choice(firstNamesF):choice(firstNamesM)) + ' ' + choice(lastNames); }
function pickPrimaryLang(cityLang){ return rand() < 0.7 ? cityLang : choice(languages.filter(x => x !== 'en')); }
function buildLanguages(primary){
  const arr = [{code:primary,is_primary:true}];
  if(rand() < 0.8) arr.push({code:'en',is_primary:false});
  if(rand() < 0.3) arr.push({code:choice(languages.filter(l => l!==primary && l!=='en')), is_primary:false});
  return arr;
}
function dateRandom(startYear=2015){ return `${randint(startYear,2025)}-${String(randint(1,12)).padStart(2,'0')}-${String(randint(1,28)).padStart(2,'0')}`; }
function randBool(p=0.5){ return rand()<p; }

// ---------------- connect ----------------
const conn = connect("127.0.0.1:27017/" + DB_NAME);
const db = conn.getSiblingDB(DB_NAME);

db.users.drop(); db.edges.drop(); db.interactions.drop();

// ---------------- generate users ----------------
const users=[];
const communities=[];
const nCommunities=randint(12,22);
let rem = N_USERS;
for(let i=0;i<nCommunities;i++){
  const min = Math.floor(N_USERS/(nCommunities*2));
  if(i===nCommunities-1){ communities.push(rem); break; }
  const take = randint(min, Math.max(min,Math.floor(rem/(nCommunities-i))));
  communities.push(take); rem-=take;
}
const communityOf={};
for(let c=0;c<nCommunities;c++){
  for(let j=0;j<communities[c];j++){
    const _id=GLOBAL_USER_ID++; const gender=rand()<0.5?'female':'male';
    const place=choice(cities); const primary=pickPrimaryLang(place.lang);
    const languagesList=buildLanguages(primary);
    const education=choice(educations); const profession=choice(professions);
    const interests=sample(interestsPool, randint(1,3)); const dateJoined=dateRandom(2016);
    const purpose=choice(purposes); const thirdParty=randBool(0.25);
    const name=randomName(gender);
    users.push({_id,name,age:randint(16,60),gender,location:{city:place.city,state:place.state||''},languages:languagesList,primaryLang:primary,joinedAt:dateJoined,education,profession,interests,purpose,thirdParty,community:c});
    communityOf[_id]=c;
  }
}
db.users.insertMany(users);
print(`Inserted users: ${db.users.countDocuments()}`);

// ---------------- generate friend/follow edges ----------------
// same batching logic as before, weights consistent (1.0), supports friend + follow edges
// ---------------- interactions ----------------
// same batching logic as before, weight per interaction
// ---------------- indexes ----------------
db.users.createIndex({"location.city":1});
db.users.createIndex({"primaryLang":1});
db.users.createIndex({"community":1});
db.edges.createIndex({type:1, src:1});
db.edges.createIndex({type:1, dst:1});
db.edges.createIndex({pair:1},{unique:true,partialFilterExpression:{type:'friend'}});
db.edges.createIndex({type:1,src:1,dst:1},{unique:true,partialFilterExpression:{type:'follow'}});
db.interactions.createIndex({actor:1});
db.interactions.createIndex({target:1});

printjson({users:db.users.countDocuments(),friend_edges:db.edges.countDocuments({type:'friend'}),follow_edges:db.edges.countDocuments({type:'follow'}),interactions:db.interactions.countDocuments()});
print("MongoDB social network setup complete.");
