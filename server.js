'use strict';

/*
  SIGN WELL Analytics Lite v2
  ------------------------------------------------------------
  - Zero third-party npm dependencies
  - Aggregate counters only
  - Keeps recent 90 days at daily resolution
  - Keeps monthly and yearly aggregates long-term
  - 30-minute duplicate suppression per IP + route (RAM only)
  - Raw IP is NEVER written to disk
*/

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
const DATA_FILE = path.join(DATA_DIR, 'analytics.json');
const ALLOWED_ORIGINS = String(
  process.env.ALLOWED_ORIGINS ||
  'https://980510linz.github.io,http://localhost:8000,http://127.0.0.1:8000'
).split(',').map(s=>s.trim()).filter(Boolean);

const CERT_FILE = process.env.CERT_FILE || '';
const KEY_FILE = process.env.KEY_FILE || '';
const RATE_WINDOW_MS = 30 * 60 * 1000;
const DAILY_RETENTION_DAYS = 90;
const MAX_BODY = 16 * 1024;

fs.mkdirSync(DATA_DIR,{recursive:true});

function isoDay(d=new Date()){ return d.toISOString().slice(0,10); }
function monthKey(day){ return String(day).slice(0,7); }
function yearKey(day){ return String(day).slice(0,4); }

function blankBucket(){
  return {total:0,daily:{},monthly:{},yearly:{},lastAt:null};
}
function blankState(){
  return {
    schema:2,
    mode:'aggregate-lite',
    retention:{dailyDays:DAILY_RETENTION_DAYS,monthly:'permanent',yearly:'permanent'},
    total:0,
    daily:{},
    monthly:{},
    yearly:{},
    articles:{},
    pages:{},
    updatedAt:new Date().toISOString()
  };
}

function num(v){ return Number(v||0); }
function obj(v){ return v&&typeof v==='object'&&!Array.isArray(v)?v:{}; }

function hydrateBucket(raw={}){
  const b={...blankBucket(),...raw};
  b.total=num(b.total);
  b.daily=obj(b.daily);
  b.monthly=obj(b.monthly);
  b.yearly=obj(b.yearly);

  // One-time migration from old daily-only data.
  if(!Object.keys(b.monthly).length && Object.keys(b.daily).length){
    for(const [day,count] of Object.entries(b.daily)){
      b.monthly[monthKey(day)]=num(b.monthly[monthKey(day)])+num(count);
      b.yearly[yearKey(day)]=num(b.yearly[yearKey(day)])+num(count);
    }
  }
  return b;
}

function loadState(){
  try{
    const raw=JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));
    const s={...blankState(),...raw};
    s.daily=obj(s.daily);
    s.monthly=obj(s.monthly);
    s.yearly=obj(s.yearly);
    s.articles=obj(s.articles);
    s.pages=obj(s.pages);
    s.total=num(s.total);

    // Migrate top-level monthly/yearly if needed.
    if(!Object.keys(s.monthly).length && Object.keys(s.daily).length){
      for(const [day,count] of Object.entries(s.daily)){
        s.monthly[monthKey(day)]=num(s.monthly[monthKey(day)])+num(count);
        s.yearly[yearKey(day)]=num(s.yearly[yearKey(day)])+num(count);
      }
    }
    for(const k of Object.keys(s.articles)) s.articles[k]=hydrateBucket(s.articles[k]);
    for(const k of Object.keys(s.pages)) s.pages[k]=hydrateBucket(s.pages[k]);
    s.schema=2;
    s.mode='aggregate-lite';
    return compactState(s);
  }catch(_){
    const s=blankState();
    atomicWrite(s);
    return s;
  }
}

function atomicWrite(value){
  const tmp=DATA_FILE+'.tmp';
  fs.writeFileSync(tmp,JSON.stringify(value,null,2)+'\n','utf8');
  fs.renameSync(tmp,DATA_FILE);
}

function cutoffDay(){
  const d=new Date();
  d.setUTCDate(d.getUTCDate()-(DAILY_RETENTION_DAYS-1));
  return isoDay(d);
}
function compactDaily(map){
  const cutoff=cutoffDay();
  for(const k of Object.keys(map||{})){
    if(k<cutoff) delete map[k];
  }
}
function compactState(s){
  compactDaily(s.daily);
  for(const b of Object.values(s.articles||{})) compactDaily(b.daily);
  for(const b of Object.values(s.pages||{})) compactDaily(b.daily);
  return s;
}

let state=loadState();
let writeTimer=null;
function queueWrite(){
  clearTimeout(writeTimer);
  writeTimer=setTimeout(()=>{
    compactState(state);
    state.updatedAt=new Date().toISOString();
    atomicWrite(state);
  },120);
}

const recent=new Map();
function cleanupRecent(now=Date.now()){
  if(recent.size<3000)return;
  for(const [k,exp] of recent) if(exp<=now) recent.delete(k);
}
function clientIp(req){ return req.socket.remoteAddress||'unknown'; }

function safeKey(v,max=180){
  return String(v||'').trim().replace(/[^\p{L}\p{N}_\-:/.#]/gu,'').slice(0,max);
}
function isAllowedOrigin(origin){ return !origin || ALLOWED_ORIGINS.includes(origin); }
function setCors(req,res){
  const origin=req.headers.origin;
  if(origin&&isAllowedOrigin(origin)){
    res.setHeader('Access-Control-Allow-Origin',origin);
    res.setHeader('Vary','Origin');
  }
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  res.setHeader('Access-Control-Max-Age','86400');
}
function sendJson(res,code,value){
  res.statusCode=code;
  res.setHeader('Content-Type','application/json; charset=utf-8');
  res.setHeader('Cache-Control','no-store');
  res.end(JSON.stringify(value));
}
function readJson(req){
  return new Promise((resolve,reject)=>{
    let size=0,chunks=[];
    req.on('data',c=>{
      size+=c.length;
      if(size>MAX_BODY){reject(new Error('payload-too-large'));req.destroy();return;}
      chunks.push(c);
    });
    req.on('end',()=>{
      try{resolve(chunks.length?JSON.parse(Buffer.concat(chunks).toString('utf8')):{});}
      catch(_){reject(new Error('bad-json'));}
    });
    req.on('error',reject);
  });
}

function incAggregate(target,day){
  const m=monthKey(day),y=yearKey(day);
  target.total=num(target.total)+1;
  target.daily=obj(target.daily);
  target.monthly=obj(target.monthly);
  target.yearly=obj(target.yearly);
  target.daily[day]=num(target.daily[day])+1;
  target.monthly[m]=num(target.monthly[m])+1;
  target.yearly[y]=num(target.yearly[y])+1;
  target.lastAt=new Date().toISOString();
}
function incRoot(day){
  const m=monthKey(day),y=yearKey(day);
  state.total=num(state.total)+1;
  state.daily[day]=num(state.daily[day])+1;
  state.monthly[m]=num(state.monthly[m])+1;
  state.yearly[y]=num(state.yearly[y])+1;
}

async function handler(req,res){
  setCors(req,res);

  if(req.method==='OPTIONS'){res.statusCode=204;return res.end();}
  const origin=req.headers.origin;
  if(origin&&!isAllowedOrigin(origin)) return sendJson(res,403,{error:'origin-not-allowed'});

  const url=new URL(req.url,'http://localhost');

  if(req.method==='GET'&&url.pathname==='/health'){
    return sendJson(res,200,{
      ok:true,service:'SIGN WELL Analytics Lite',schema:2,mode:'aggregate-lite',
      retention:state.retention,updatedAt:state.updatedAt
    });
  }

  if(req.method==='GET'&&url.pathname==='/stats'){
    return sendJson(res,200,{
      schema:2,mode:'aggregate-lite',retention:state.retention,
      total:state.total,daily:state.daily,monthly:state.monthly,yearly:state.yearly,
      articles:state.articles,pages:state.pages,updatedAt:state.updatedAt
    });
  }

  if(req.method==='POST'&&url.pathname==='/view'){
    let body;
    try{body=await readJson(req);}catch(e){return sendJson(res,400,{error:e.message});}

    const kind=body.kind==='article'?'article':'page';
    const key=safeKey(body.key||body.slug||body.path);
    if(!key)return sendJson(res,400,{error:'missing-key'});

    const hash=crypto.createHash('sha256')
      .update(clientIp(req)+'|'+kind+'|'+key)
      .digest('hex').slice(0,24);

    const now=Date.now();
    cleanupRecent(now);
    if((recent.get(hash)||0)>now){res.statusCode=204;return res.end();}
    recent.set(hash,now+RATE_WINDOW_MS);

    const day=isoDay();
    incRoot(day);
    const bucket=kind==='article'?state.articles:state.pages;
    if(!bucket[key]) bucket[key]=blankBucket();
    incAggregate(bucket[key],day);
    queueWrite();

    res.statusCode=204;
    return res.end();
  }

  return sendJson(res,404,{error:'not-found'});
}

let server;
if(CERT_FILE&&KEY_FILE){
  server=https.createServer({
    cert:fs.readFileSync(CERT_FILE),
    key:fs.readFileSync(KEY_FILE)
  },handler);
}else{
  server=http.createServer(handler);
}
server.listen(PORT,HOST,()=>{
  console.log(`SIGN WELL Analytics Lite listening on ${CERT_FILE&&KEY_FILE?'https':'http'}://${HOST}:${PORT}`);
  console.log('Aggregate-only mode; daily retention:',DAILY_RETENTION_DAYS,'days');
});

function shutdown(){
  try{
    clearTimeout(writeTimer);
    compactState(state);
    state.updatedAt=new Date().toISOString();
    atomicWrite(state);
  }catch(_){}
  server.close(()=>process.exit(0));
  setTimeout(()=>process.exit(0),1500).unref();
}
process.on('SIGINT',shutdown);
process.on('SIGTERM',shutdown);
