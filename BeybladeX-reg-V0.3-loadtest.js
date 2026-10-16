'use strict';
/*
 * Beyblade X Registration V0.3 — 100 simultaneous SSE / idle / resume probe.
 * Emulator only. Does not contact or modify production Firebase.
 *
 * Place in ~/BeybladeX-reg-V0.3/functions/tests/100_users.emulator.js
 * Run (from repo root):
 * firebase emulators:exec --only database --project demo-ljtreg 'node functions/tests/100_users.emulator.js'
 *
 * Requires firebase-admin already installed under functions/node_modules.
 */
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const assert=require('node:assert/strict');
const { initializeApp, deleteApp }=require('firebase-admin/app');
const { getDatabase }=require('firebase-admin/database');

const host=process.env.FIREBASE_DATABASE_EMULATOR_HOST||'';
if(!/^(?:localhost|127\.0\.0\.1):\d+$/.test(host)){
 console.error('REFUSING: FIREBASE_DATABASE_EMULATOR_HOST must point to a local RTDB Emulator.');
 process.exit(2);
}
const CLIENTS=100, SLEEPING=80, AWAKE=CLIENTS-SLEEPING;
const NS='demo-ljtreg-default-rtdb';
const EID='P'+crypto.randomBytes(2).toString('hex').toUpperCase();
const URL=`http://${host}/syncV03/${EID}.json?ns=${NS}`;
const app=initializeApp({projectId:'demo-ljtreg',databaseURL:`https://${NS}.firebaseio.com`});
const db=getDatabase(app);
const log=(...a)=>console.log(new Date().toISOString(),...a);
const pause=ms=>new Promise(r=>setTimeout(r,ms));
const waitUntil=async(predicate,timeoutMs,message)=>{
 const end=Date.now()+timeoutMs;
 while(Date.now()<end){if(predicate())return;await pause(75)}
 throw new Error(message);
};
let requestCount=0,streamBytes=0, active=0, peak=0;
const connections=[];

async function connect(i){
 const c={i,controller:new AbortController(),seen:new Map(),bytes:0,closed:false,openedAt:Date.now(),errors:[]};
 const res=await fetch(URL,{signal:c.controller.signal,headers:{Accept:'text/event-stream'},cache:'no-store'});
 requestCount++;
 if(!res.ok||!String(res.headers.get('content-type')).includes('text/event-stream')){
  const text=await res.text();throw new Error(`SSE ${i} failed: HTTP ${res.status} ${text.slice(0,200)}`);
 }
 active++;peak=Math.max(peak,active);
 c.close=()=>{if(c.closed)return;c.closed=true;active--;c.controller.abort()};
 c.readerTask=(async()=>{
  const reader=res.body.getReader();const decoder=new TextDecoder();let pending='';
  try{
   for(;;){
    const {done,value}=await reader.read();if(done)break;
    c.bytes+=value.length;streamBytes+=value.length;
    pending+=decoder.decode(value,{stream:true}).replace(/\r\n/g,'\n');
    let boundary;
    while((boundary=pending.indexOf('\n\n'))>=0){
     const frame=pending.slice(0,boundary);pending=pending.slice(boundary+2);
     const payload=frame.split('\n').filter(x=>x.startsWith('data:')).map(x=>x.slice(5)).join('\n');
     if(!payload)continue;
     let data;try{data=JSON.parse(payload)}catch{continue}
     const blob=JSON.stringify(data);
     for(const marker of ['IDLE_INIT','ACTIVE_1','ACTIVE_2','SLEEP_CHANGE']){
      if(blob.includes(marker)&&!c.seen.has(marker)) c.seen.set(marker,Date.now());
     }
    }
    // Defensively cap decoder buffer in case emulator uses a different delimiter.
    if(pending.length>1e6)throw new Error('Unexpected SSE message size > 1 MB');
   }
  }catch(e){if(!c.controller.signal.aborted)c.errors.push(String(e))}
  finally{try{reader.releaseLock()}catch{};if(!c.closed){c.closed=true;active--}}
 })();
 connections.push(c);return c;
}

async function openGroup(count,offset=0){
 const group=[];
 // Batches avoid a spiky artificial connection storm on the user's small Cloud Shell VM.
 for(let start=0;start<count;start+=10){
  const c=await Promise.all(Array.from({length:Math.min(10,count-start)},(_,j)=>connect(offset+start+j)));
  group.push(...c);await pause(80);
 }
 return group;
}
const latency=c=>(c.seen.get('ACTIVE_1')??Date.now())-sentAt;
let sentAt=0;
(async()=>{
 log('START',`clients=${CLIENTS}`,`sleeping=${SLEEPING}`,`target=LOCAL EMULATOR ONLY`,`namespace=${NS}`);
 await db.ref(`eventsV03/${EID}/meta`).set({title:'SSE壓測',open:true,capacity:1,deleted:false});
 await db.ref(`syncV03/${EID}`).set({token:'IDLE_INIT',lastChange:'IDLE_INIT'});
 const all=await openGroup(CLIENTS);
 await waitUntil(()=>all.every(c=>c.seen.has('IDLE_INIT')),15000,'Some clients missed initial SSE state');
 log('CONNECT PASS',`active=${active}`,`SSE GET=${requestCount}`);
 const baselineRequests=requestCount;
 const baselineBytes=streamBytes;
 await pause(10000);
 assert.equal(requestCount,baselineRequests,'unexpected new HTTP requests at idle');
 log('IDLE PASS','10 seconds, no new HTTP GET',`SSE bytes +${streamBytes-baselineBytes}`);
 sentAt=Date.now();
 await db.ref(`syncV03/${EID}`).update({token:'ACTIVE_1',lastChange:'ACTIVE_1'});
 await waitUntil(()=>all.every(c=>c.seen.has('ACTIVE_1')),15000,'SSE change not delivered to every active client');
 const delays=all.map(c=>c.seen.get('ACTIVE_1')-sentAt).sort((a,b)=>a-b);
 const percentile=p=>delays[Math.min(delays.length-1,Math.ceil(p/100*delays.length)-1)];
 log('100 LIVE PASS',`p50=${percentile(50)}ms`,`p95=${percentile(95)}ms`,`max=${delays.at(-1)}ms`);
 for(const c of all.slice(0,SLEEPING))c.close();
 await waitUntil(()=>active===AWAKE,5000,'80 background streams not closed');
 const beforeRemaining=all.slice(SLEEPING).map(c=>c.bytes);
 await db.ref(`syncV03/${EID}`).update({token:'ACTIVE_2',lastChange:'ACTIVE_2'});
 await waitUntil(()=>all.slice(SLEEPING).every(c=>c.seen.has('ACTIVE_2')),15000,'20 foreground streams did not get change');
 await pause(200);
 assert(all.slice(0,SLEEPING).every(c=>!c.seen.has('ACTIVE_2')),'a sleeping client still received a change');
 log('BACKGROUND PASS','80 SSE closed; 20 stay live; no change sent to sleeping SSE');
 const reopened=await openGroup(SLEEPING,CLIENTS);
 await waitUntil(()=>reopened.every(c=>c.seen.has('ACTIVE_2')),15000,'resume clients did not fetch current state');
 log('RESUME PASS','80 reconnect to latest marker; stale state caught up');
 const totalErrors=[...all,...reopened].flatMap(c=>c.errors);
 if(totalErrors.length)throw new Error(`SSE reader errors: ${totalErrors.slice(0,5).join(' | ')}`);
 const report={test:'Beyblade X V0.3 100 SSE emulator clients',productionTouched:false,project:'demo-ljtreg',clients:CLIENTS,background:SLEEPING,awake:AWAKE,initialConnections:CLIENTS,reconnected:SLEEPING,totalSseHttpRequests:requestCount,peakOpenSse:peak,streamBytesReceived:streamBytes,firstUpdateLatencyMs:{p50:percentile(50),p95:percentile(95),max:delays.at(-1)},note:'This is a real local Firebase RTDB Emulator 100-connection test. It does not measure 100 physical phones, browser memory, or production billing.'};
 const out=path.join(process.cwd(),'V0.3_100_SSE_EMULATOR_REPORT.json');fs.writeFileSync(out,JSON.stringify(report,null,2)+'\n');
 log('PASS 100 SSE EMULATOR',`report=${out}`);console.log(JSON.stringify(report,null,2));
})().catch(e=>{console.error('FAIL 100 SSE EMULATOR',e?.stack||String(e));process.exitCode=1})
 .finally(async()=>{
  for(const c of connections)if(c.close)c.close();
  await Promise.allSettled(connections.map(c=>c.readerTask));
  try{await db.ref(`syncV03/${EID}`).remove();await db.ref(`eventsV03/${EID}`).remove()}catch(e){console.error('cleanup:',String(e))}
  await deleteApp(app);
 });
