// Кількісна симуляція протоколу синхронізації на слабкому каналі.
// Модель: одна таблиця, власник створює рядки з темпом lambda/с (найгірший випадок: тільки вставки).
// Канал власник -> клієнт: пропускна здатність DOWN (ділиться на share інстансів), затримка в один бік L, вікно потоку HTTP/2 W байт.
// Власник щоразу шле найвищий невідправлений проміжок (6.4), пачками до N рядків; ліміт діапазонів cap.
// Розмір рядка після gzip залежить від розміру пачки (gzip-bench.json).
const fs=require('fs');
const BENCH=JSON.parse(fs.readFileSync(__dirname+'/gzip-bench.json'));
function gzPerRow(n,kind='wide'){
  const G=BENCH.filter(x=>x.kind===kind);
  if(n<=G[0].n)return G[0].gzPerRow;
  for(let i=1;i<G.length;i++)if(n<=G[i].n){const a=G[i-1],b=G[i],k=(Math.log(n)-Math.log(a.n))/(Math.log(b.n)-Math.log(a.n));return a.gzPerRow+k*(b.gzPerRow-a.gzPerRow)}
  return G[G.length-1].gzPerRow;
}
const MSG_OVERHEAD=40; // заголовки gRPC/HTTP2 + службові поля пачки

class Ranges{ // курсор + відрізки (lo,hi] вище курсора
  constructor(c){this.c=c;this.r=[]}
  add(lo,hi){this.r.push([lo,hi]);this.r.sort((a,b)=>a[0]-b[0]);const m=[];
    for(const x of this.r){const l=m[m.length-1];if(l&&x[0]<=l[1])l[1]=Math.max(l[1],x[1]);else m.push([...x])}
    while(m.length&&m[0][0]<=this.c){this.c=Math.max(this.c,m[0][1]);m.shift()}this.r=m}
  gaps(H){const g=[];let top=H;for(let i=this.r.length-1;i>=0;i--){const [lo,hi]=this.r[i];if(top>hi)g.push([hi,top]);top=lo}if(top>this.c)g.push([this.c,top]);return g} // зверху вниз
}

function simDown({lambda,L,DOWN=2e6,W=1<<20,N=500,offline=0,T=600,cap=16,share=1,kind='wide',dt=0.02,coalesce=0}){
  // coalesce: верхній (свіжий) проміжок шлемо не частіше, ніж раз на coalesce с, поки він менший за пачку
  let lastTop=-1e9;
  const bps=DOWN/8/share;
  const created=v=>v/lambda-offline;           // час створення версії v
  const head=t=>Math.floor(lambda*(t+offline));
  const start=offline?3*L:0;                    // Handshake + відповідь + Start
  const owner=new Ranges(offline?0:head(0)), client=new Ranges(owner.c);
  let linkFree=start,inflight=0;const returns=[],recv=[];
  const hist=[],samples=[],qd=[];let firstVisible=null,fullSync=null,maxRanges=0,topVisible=client.c,bytesSent=0,msgs=0,rowsSent=0;
  for(let t=start;t<T;t+=dt){
    while(returns.length&&returns[0].at<=t)inflight-=returns.shift().bytes;
    while(recv.length&&recv[0].at<=t){const m=recv.shift();client.add(m.lo,m.hi);topVisible=Math.max(topVisible,m.hi);
      if(firstVisible==null&&offline)firstVisible=m.at;
      const step=Math.max(1,Math.floor((m.hi-m.lo)/20));for(let v=m.hi;v>m.lo;v-=step)hist.push({d:m.at-created(v),w:Math.min(step,v-m.lo)});
      maxRanges=Math.max(maxRanges,client.r.length);
      if(fullSync==null&&offline&&!client.r.length&&head(m.at)-client.c<=Math.max(N,lambda*2*L+lambda))fullSync=m.at}
    // власник шле, поки канал вільний у межах кроку і вікно дозволяє
    const H=head(t);
    while(linkFree<=t+dt){
      const g=owner.gaps(H);if(!g.length)break;
      let gi=0;
      if(coalesce&&g[0][1]===H&&g[0][1]-g[0][0]<N&&t-lastTop<coalesce)gi=1; // свіжий хвіст чекає, шлемо старіший проміжок
      if(gi===0&&owner.r.length>=cap&&g.length>1)gi=1; // на ліміті: заповнює проміжок під верхнім діапазоном
      if(gi>=g.length)break;let gap=g[gi];if(gap[1]===H)lastTop=t;
      const n=Math.min(N,gap[1]-gap[0]);const bytes=Math.ceil(n*gzPerRow(n,kind))+MSG_OVERHEAD;
      if(inflight+bytes>W&&inflight>0)break;
      const st=Math.max(t,linkFree),fin=st+bytes/bps;linkFree=fin;inflight+=bytes;bytesSent+=bytes;msgs++;rowsSent+=n;
      // на ліміті пачка йде знизу проміжку (зростити з нижнім), інакше згори
      const lo=(owner.r.length>=cap&&gi===1&&!coalesce)?gap[0]:gap[1]-n;
      owner.add(lo,lo+n);recv.push({at:fin+L,lo,hi:lo+n});returns.push({at:fin+2*L,bytes});
    }
    if(samples.length===0||t-samples[samples.length-1].t>=1)
      samples.push({t:+t.toFixed(1),topAge:+(t-created(Math.max(topVisible,1))).toFixed(2),completeAge:+(t-created(Math.max(client.c,1))).toFixed(1),behind:head(t)-client.c,ranges:client.r.length,queue:+Math.max(0,linkFree-t).toFixed(2)});
  }
  hist.sort((a,b)=>a.d-b.d);const tot=hist.reduce((s,x)=>s+x.w,0);
  const pct=p=>{let acc=0;for(const x of hist){acc+=x.w;if(acc>=p*tot)return +x.d.toFixed(2)}return null};
  const last=samples.slice(-60);
  const during=samples.filter(x=>fullSync==null||x.t<fullSync);const p95of=(a)=>{const b=a.slice().sort((x,y)=>x-y);return b.length?b[Math.floor(.95*(b.length-1))]:null};
  return {p50:pct(.5),p95:pct(.95),p99:pct(.99),firstVisible:firstVisible&&+firstVisible.toFixed(1),fullSync:fullSync&&+fullSync.toFixed(0),
    topAgeEnd:last.length?+Math.max(...last.map(s=>s.topAge)).toFixed(1):null,
    behindEnd:samples[samples.length-1].behind,topAgeP95:p95of(during.map(x=>x.topAge)),queueP95:p95of(during.map(x=>x.queue)),queueMax:Math.max(...samples.map(x=>x.queue)),maxRanges,avgBatch:+(rowsSent/Math.max(1,msgs)).toFixed(1),
    linkUtil:+(bytesSent/((T-start)*bps)).toFixed(2),rowsPerSec:+(rowsSent/(T-start)).toFixed(0),samples};
}

// Відправка дій клієнта (Apply): unary, одна пачка в дорозі, FIFO за seq, обриви з MTBF.
function simUp({actions,bytesPerAction,UP=70e3,L,batchBytes=1<<20,batchMax=1000,mtbf=Infinity,seed=1,reconnect=null,overRatio=1.0}){
  let r=seed;const rnd=()=>{r=(r*16807)%2147483647;return r/2147483647};
  const bps=UP/8*overRatio;let t=0,done=0,wasted=0,attempts=0;
  const nextDrop=()=>mtbf===Infinity?Infinity:-Math.log(1-rnd())*mtbf;
  let drop=nextDrop();const rc=reconnect??(4*L);
  while(done<actions){
    const n=Math.max(1,Math.min(batchMax,Math.floor(batchBytes/bytesPerAction),actions-done));const bytes=n*bytesPerAction+MSG_OVERHEAD;
    const dur=bytes/bps+2*L;attempts++;
    if(attempts>1e5)return {t:Infinity,wastedKB:+(wasted/1024).toFixed(0),attempts};
    if(t+dur>drop){wasted+=Math.min(bytes,Math.max(0,drop-t)*bps);t=drop+rc;drop=t+nextDrop();continue}
    t+=dur;done+=n;
  }
  return {t:+t.toFixed(1),wastedKB:+(wasted/1024).toFixed(0),attempts};
}
function simUpMany(o,seeds=300){const ts=[],ws=[];let att=0;for(let i=1;i<=seeds;i++){const r=simUp({...o,seed:i*7919});ts.push(r.t);ws.push(r.wastedKB);att+=r.attempts}
  ts.sort((a,b)=>a-b);const m=a=>a.reduce((x,y)=>x+y,0)/a.length;return {t:+m(ts).toFixed(0),tP95:ts[Math.floor(.95*(ts.length-1))],wastedKB:+m(ws).toFixed(0),attempts:+(att/seeds).toFixed(1)}}
module.exports={simDown,simUp,simUpMany,gzPerRow};
