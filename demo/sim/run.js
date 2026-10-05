const {simDown,simUp,simUpMany,gzPerRow}=require('./sim.js');const fs=require('fs');
const out={};const strip=r=>{const {samples,...x}=r;return x};
const t0=Date.now();
// 1. Онлайн, стабільний режим
out.online=[];
for(const coalesce of [0,0.5])for(const W of [65535,1<<20])for(const L of [1.0,1.5,2.5])for(const lambda of [10,200,500,1000,3000,5000,8000])
  out.online.push({coalesce,W,L,lambda,...strip(simDown({lambda,L,W,T:300,coalesce}))});
// 2. Онлайн, 10 інстансів ділять канал
out.share=[];
for(const lambda of [10,100,300,600])out.share.push({lambda,share:10,L:1.25,...strip(simDown({lambda,L:1.25,share:10,T:300}))});
// 3. Досинхронізація після офлайну
out.offline=[];
for(const coalesce of [0,0.5])for(const W of [65535,1<<20])for(const [off,lambda] of [[3600,100],[3600,1000],[8*3600,100],[600,3000]])for(const L of [1.25,2.5]){
  const r=simDown({lambda,L,W,offline:off,T:5000,coalesce});out.offline.push({coalesce,W,L,lambda,offline:off,backlog:off*lambda,...strip(r),curve:r.samples.filter((s,i)=>i%20==0).map(s=>[s.t,s.completeAge,s.topAge,s.queue])})}
// 4. Ліміт діапазонів при темпі понад пропускну здатність
out.cap=[];
for(const cap of [4,16])for(const lambda of [6000,8000]){const r=simDown({lambda,L:1.25,offline:600,T:900,cap});out.cap.push({cap,lambda,...strip(r)})}
// 5. Знімок
out.snapshot=[];
for(const rows of [1e5,1e6,5e6])for(const W of [65535,1<<20])for(const share of [1,10]){
  const bytes=rows*gzPerRow(5000);const bps=Math.min(2e6/8/share,W/(2*1.25));out.snapshot.push({rows,W,share,MB:+(bytes/1e6).toFixed(1),sec:Math.round(bytes/bps+3*1.25)})}
// 6. Відправка дій (Apply)
out.up=[];
const A=(name,o)=>out.up.push({name,...o,...simUpMany(o)});
A('одна правка',{actions:1,bytesPerAction:120,L:1.25});
A('одна правка за імпортом 10k',{actions:10001,bytesPerAction:130,L:1.25});
A('одна правка за імпортом 10k, вниз зайнятий',{actions:10001,bytesPerAction:130,L:1.25,overRatio:0.5});
A('масове видалення 100k',{actions:1e5,bytesPerAction:22,L:1.25});
A('архів 100k (ключ+expected)',{actions:1e5,bytesPerAction:30,L:1.25});
for(const mtbf of [120,300,900])for(const bb of [1<<20,256<<10,64<<10,16<<10])
  A(`імпорт 10k, пачка ${bb>>10} КБ, обрив ~${mtbf/60} хв`,{actions:1e4,bytesPerAction:130,L:1.25,batchBytes:bb,batchMax:1e9,mtbf,seed:7});
fs.writeFileSync(__dirname+'/results.json',JSON.stringify(out,null,1));
console.log('done',(Date.now()-t0)/1000+'s');
const show=(k,cols)=>{console.log('\n== '+k);for(const r of out[k])console.log(cols.map(c=>c+'='+r[c]).join(' '))};
show('online',['coalesce','W','L','lambda','p50','p95','p99','avgBatch','linkUtil','topAgeEnd','behindEnd','queueMax']);
show('share',['lambda','p50','p95','linkUtil','topAgeEnd','behindEnd']);
show('offline',['coalesce','W','L','lambda','offline','backlog','firstVisible','fullSync','topAgeP95','queueP95','behindEnd','maxRanges','avgBatch']);
show('cap',['cap','lambda','topAgeEnd','behindEnd','maxRanges','avgBatch','rowsPerSec']);
show('snapshot',['rows','W','share','MB','sec']);
show('up',['name','t','tP95','wastedKB','attempts']);
