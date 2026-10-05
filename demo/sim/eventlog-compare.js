// Порівняння: лог подій (event sourcing, клієнт читає всі події по порядку)
// проти поточного дизайну (стан рядка + SyncBase/SyncMask, новіші першими, схлопування).
// Канал власник -> клієнт 2 Мбіт/с, RTT ~2.5 с. Вікно HTTP/2 ~ BDP, тож упор у пропускну здатність.
// Байти рахуються справжнім gzip над закодованими записами (кодування як у gzip-bench.js).
const zlib=require('zlib');const fs=require('fs');
let seed=11;const rnd=()=>{seed=(Math.imul(seed,1103515245)+12345)&0x7fffffff;return seed/0x80000000};
const BPS=2e6/8, L=1.25, OVH=40;
const words=['Степлер','Папір','Маркери','Палета','Плівка','Скотч','Принтер','Кабель','Лампа','Монітор','Ящик','Фарба','Стілець','Коробка','Етикетки','Батарейки'];
const statuses=['новий','активний','в роботі','відвантажено','архів'];
const cats=Array.from({length:20},(_,i)=>{const b=Buffer.alloc(16);for(let j=0;j<16;j++)b[j]=(i*37+j*101)&255;return b});
function varint(n){const b=[];do{let x=n%128;n=Math.floor(n/128);if(n)x|=128;b.push(x)}while(n);return Buffer.from(b)}
function str(s){const b=Buffer.from(s,'utf8');return Buffer.concat([varint(b.length),b])}
function pk(id){const b=Buffer.alloc(16);b.writeUInt32BE(1790000000+Math.floor(id/50),0);b.writeUInt32BE(id>>>0,4);b.writeUInt32BE((id*2654435761)>>>0,8);b.writeUInt32BE((id*40503)>>>0,12);return b}
// 10 колонок: 0 Id, 1 CategoryId, 2 Qty, 3 Kind, 4 Price, 5 UpdatedAt, 6 Name, 7 Status, 8 Description, 9 Note
const COLS=10, FULL=(1<<COLS)-1;
let ver=1_000_000,clock=1_790_000_000_000;
function col(c,id){switch(c){
  case 0:return pk(id);case 1:return cats[id%20];case 2:return varint(Math.floor(rnd()*100000));case 3:return varint(Math.floor(rnd()*5));
  case 4:{const b=Buffer.alloc(8);b.writeDoubleLE(Math.round(rnd()*1e5)/100);return b}
  case 5:return varint(clock+=Math.floor(rnd()*200));case 6:return str(words[id%16]+' '+(id%1000));
  case 7:return str(statuses[Math.floor(rnd()*5)]);case 8:return str(Array.from({length:12},()=>words[Math.floor(rnd()*16)]).join(' '));
  case 9:return str('');}}
const cols=(mask,id)=>{const p=[];for(let c=1;c<COLS;c++)if(mask&(1<<c))p.push(col(c,id));return p};
// Поточний дизайн: повний рядок або PK + версія + маска + колонки з маски
const recState=(id,mask)=>mask===FULL?Buffer.concat([pk(id),varint(++ver),...cols(FULL,id)]):Buffer.concat([pk(id),varint(++ver),varint(mask),...cols(mask,id)]);
// Лог подій: версія + таблиця + тип + PK + маска + колонки
const recEvent=(id,mask)=>Buffer.concat([varint(++ver),Buffer.from([3,mask===FULL?1:2]),pk(id),varint(mask),...cols(mask,id)]);
const gz=recs=>recs.length?zlib.gzipSync(Buffer.concat(recs)).length+5+OVH:0;
// Оновлення: Status+UpdatedAt 60%, Qty+UpdatedAt 30%, Description+UpdatedAt 10%
function updMask(){const r=rnd();return (1<<5)|(r<0.6?1<<7:r<0.9?1<<2:1<<8)}
function pickRow(w){return w.dist==='hot'?Math.floor(rnd()*w.hot):Math.floor(rnd()*w.rows)}
// стиснутий розмір на запис, виміряний на вибірці пачок по 1000
function perRec(make,count){const S=Math.min(count,30000);if(!S)return 0;let bytes=0;
  for(let i=0;i<S;i+=1000){const recs=[];for(let j=i;j<Math.min(S,i+1000);j++)recs.push(make(j));bytes+=gz(recs)}return bytes/S}

// Досинхронізація після офлайну тривалістю off секунд
function offline(w){
  const nIns=Math.round(w.ins*w.off), nUpd=Math.round(w.upd*w.off);
  const masks=new Map();const evMasks=[];
  for(let i=0;i<nUpd;i++){const id=pickRow(w),m=updMask();masks.set(id,(masks.get(id)||0)|m);if(evMasks.length<30000)evMasks.push([id,m])}
  const distinct=masks.size,uMasks=[...masks.entries()];
  // поточний дизайн: вставки повністю, кожен змінений рядок один раз з об'єднаною маскою
  const stIns=perRec(j=>recState(10_000_000+j,FULL),nIns),stUpd=perRec(j=>recState(...uMasks[j%uMasks.length]),distinct);
  const evIns=perRec(j=>recEvent(10_000_000+j,FULL),nIns),evUpd=perRec(j=>recEvent(...evMasks[j%evMasks.length]),nUpd);
  const stBytes=nIns*stIns+distinct*stUpd, evBytes=nIns*evIns+nUpd*evUpd;
  // поки клієнт досинхронізується, нові зміни тривають: час T, за який канал наздогнав голову
  const catchup=(bytes,rate)=>rate>=BPS?Infinity:bytes/(BPS-rate)+2*L;
  // песимістично для дизайну: нові зміни під час досинхронізації рахуються без схлопування
  const stRate=w.ins*stIns+w.upd*stUpd, evRate=w.ins*evIns+w.upd*evUpd;
  return {name:w.name,events:nIns+nUpd,inserts:nIns,updates:nUpd,distinctUpdated:distinct,
    state:{MB:+(stBytes/1e6).toFixed(1),catchupSec:Math.round(catchup(stBytes,stRate)),freshVisibleSec:+(2*L+Math.min(stBytes,64e3)/BPS).toFixed(1)},
    log:{MB:+(evBytes/1e6).toFixed(1),catchupSec:Math.round(catchup(evBytes,evRate)),freshVisibleSec:Math.round(catchup(evBytes,evRate))},
    ratio:+(evBytes/stBytes).toFixed(2)};
}

// Онлайн: раунди по 0.5 с; дизайн схлопує зміни рядка в межах раунду, лог везе кожну подію
function online(w,T=60,round=0.5){
  let st=0,ev=0;
  for(let t=0;t<T;t+=round){
    const nIns=Math.round(w.ins*round),nUpd=Math.round(w.upd*round);
    const masks=new Map(),evs=[];
    for(let i=0;i<nUpd;i++){const id=pickRow(w),m=updMask();masks.set(id,(masks.get(id)||0)|m);evs.push(recEvent(id,m))}
    const ins=[];for(let i=0;i<nIns;i++)ins.push(10_000_000+Math.floor(t*1e4)+i);
    st+=gz([...ins.map(id=>recState(id,FULL)),...[...masks].map(([id,m])=>recState(id,m))]);
    ev+=gz([...ins.map(id=>recEvent(id,FULL)),...evs]);
  }
  return {name:w.name,state:{KBps:+(st/T/1e3).toFixed(1),linkPct:Math.round(st/T/BPS*100)},log:{KBps:+(ev/T/1e3).toFixed(1),linkPct:Math.round(ev/T/BPS*100)},ratio:+(ev/st).toFixed(2)};
}

// Місце на власнику: лог тримається не менше вікна активності (24 год), інакше клієнт іде на знімок
function storage(w){const evRaw=(36+20)*w.upd+(250+20)*w.ins; // запис + індекс/службове SQLite
  return {name:w.name,logGBperDay:+(evRaw*86400/1e9).toFixed(2),logInsertsPerSec:w.ins+w.upd,stateExtraBytesPerRow:10}}

const ON=[
  {name:'Онлайн, легко: 10 вставок/с, 50 оновлень/с по 1000 гарячих рядків',ins:10,upd:50,dist:'hot',hot:1000},
  {name:'Онлайн, важко: 300 вставок/с, 3000 оновлень/с по 10 000 гарячих',ins:300,upd:3000,dist:'hot',hot:10000},
  {name:'Онлайн, оновлення розкидані: 100 вставок/с, 1000 оновлень/с по 1 млн рядків',ins:100,upd:1000,dist:'spread',rows:1e6},
  {name:'Онлайн, майже лише вставки: 1500 вставок/с, 20 оновлень/с',ins:1500,upd:20,dist:'hot',hot:10000},
];
const OFF=[
  {name:'Офлайн 1 год: 100 вставок/с, 500 оновлень/с по 10 000 гарячих',off:3600,ins:100,upd:500,dist:'hot',hot:10000},
  {name:'Офлайн 8 год: 100 вставок/с, 500 оновлень/с по 10 000 гарячих',off:8*3600,ins:100,upd:500,dist:'hot',hot:10000},
  {name:'Офлайн 8 год: 100 вставок/с, 500 оновлень/с розкидано по 1 млн',off:8*3600,ins:100,upd:500,dist:'spread',rows:1e6},
  {name:'Офлайн 1 год: 1000 вставок/с, 10 оновлень/с',off:3600,ins:1000,upd:10,dist:'hot',hot:10000},
  {name:'Офлайн 15 хв: 300 вставок/с, 3000 оновлень/с по 10 000 гарячих',off:900,ins:300,upd:3000,dist:'hot',hot:10000},
];
const out={params:{downMbit:2,rttSec:2*L,onlineRoundSec:0.5,cols:COLS,update:'Status|Qty|Description + UpdatedAt'},
  online:ON.map(w=>online(w)),offline:OFF.map(offline),storage:[...ON,...OFF].map(storage)};
fs.writeFileSync(__dirname+'/eventlog-compare.json',JSON.stringify(out,null,1));
for(const r of out.online)console.log('ON ',r.name,'| стан',r.state,'| лог',r.log,'| x',r.ratio);
for(const r of out.offline)console.log('OFF',r.name,'| подій',r.events,'рядків змін.',r.distinctUpdated,'| стан',r.state,'| лог',r.log,'| x',r.ratio);
for(const r of out.storage)console.log('DISK',r.name,r.logGBperDay,'ГБ/добу',r.logInsertsPerSec,'вставок у лог/с');
